# contracts/generator_fixed.py
#!/usr/bin/env python3
"""
Fixed ContractGenerator that handles nested structures
"""

import argparse
import json
import yaml
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np


class FixedContractGenerator:
    """Generates data contracts from JSONL files, handling nested structures"""
    
    def __init__(self, source_path: str, output_dir: str):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def safe_nunique(self, series: pd.Series) -> int:
        """Safely calculate unique count for series that may contain unhashable types"""
        try:
            return series.nunique()
        except TypeError:
            # Convert unhashable types to strings for uniqueness check
            unique_set = set()
            for val in series:
                if val is None:
                    unique_set.add(None)
                elif isinstance(val, (list, dict)):
                    # Convert to JSON string for hashing
                    try:
                        unique_set.add(json.dumps(val, sort_keys=True))
                    except:
                        unique_set.add(str(type(val)))
                else:
                    unique_set.add(val)
            return len(unique_set)
    
    def load_data(self) -> tuple[pd.DataFrame, List[Dict]]:
        """Load JSONL data into DataFrame and keep original records"""
        records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Warning: Skipping invalid JSON at line {line_num}: {e}")
                        continue
        
        if not records:
            raise ValueError(f"No valid records found in {self.source_path}")
        
        print(f"📊 Loaded {len(records)} records")
        
        # Flatten nested structures for analysis, but handle lists carefully
        df = pd.json_normalize(records, max_level=1)
        print(f"   Created DataFrame with {len(df.columns)} columns")
        
        return df, records
    
    def detect_schema_type(self, records: List[Dict]) -> str:
        """Detect which week/system this data belongs to"""
        if not records:
            return "unknown"
        
        sample = records[0]
        sample_str = str(sample).lower()
        
        # Check for extraction data
        if 'extracted_facts' in sample or 'doc_id' in sample or 'confidence' in sample_str:
            return "week3"
        
        # Check for event sourcing
        if 'event_id' in sample or 'aggregate_id' in sample or 'sequence_number' in sample:
            return "week5"
        
        # Check for intent records
        if 'intent_id' in sample or 'code_refs' in sample:
            return "week1"
        
        # Check for verdict records
        if 'verdict_id' in sample or 'rubric_id' in sample:
            return "week2"
        
        # Check for lineage snapshots
        if 'snapshot_id' in sample or 'nodes' in sample:
            return "week4"
        
        return "generic"
    
    def extract_nested_confidence(self, records: List[Dict]) -> List[float]:
        """Extract confidence values from nested structures"""
        confidence_values = []
        
        for record in records:
            # Direct confidence field
            if 'confidence' in record:
                val = record['confidence']
                if isinstance(val, (int, float)):
                    confidence_values.append(float(val))
            
            # Nested in extracted_facts
            if 'extracted_facts' in record and isinstance(record['extracted_facts'], list):
                for fact in record['extracted_facts']:
                    if isinstance(fact, dict) and 'confidence' in fact:
                        val = fact['confidence']
                        if isinstance(val, (int, float)):
                            confidence_values.append(float(val))
            
            # Nested in payload
            if 'payload' in record and isinstance(record['payload'], dict):
                if 'confidence' in record['payload']:
                    val = record['payload']['confidence']
                    if isinstance(val, (int, float)):
                        confidence_values.append(float(val))
        
        return confidence_values
    
    def structural_profiling(self, df: pd.DataFrame, records: List[Dict]) -> Dict[str, Any]:
        """Structural profiling of columns, handling nested structures"""
        profile = {
            'columns': {},
            'record_count': len(df),
            'file': str(self.source_path)
        }
        
        # First, extract confidence values from nested structures
        confidence_values = self.extract_nested_confidence(records)
        if confidence_values:
            profile['confidence_values'] = confidence_values
        
        for col in df.columns:
            col_data = df[col]
            null_count = col_data.isna().sum()
            null_fraction = null_count / len(df) if len(df) > 0 else 0
            
            # Safely calculate unique count
            cardinality = self.safe_nunique(col_data)
            
            # Sample values (handle unhashable types)
            sample_values = []
            for val in col_data.dropna().head(3):
                if isinstance(val, (list, dict)):
                    sample_values.append(str(type(val)))
                else:
                    sample_values.append(val)
            
            profile['columns'][col] = {
                'dtype': str(col_data.dtype),
                'null_count': int(null_count),
                'null_fraction': float(null_fraction),
                'unique_count': cardinality,
                'sample_values': sample_values,
                'is_confidence': 'confidence' in col.lower()
            }
            
            # Infer patterns for string columns
            if col_data.dtype == 'object' and len(col_data.dropna()) > 0:
                try:
                    sample = str(col_data.dropna().iloc[0])
                    profile['columns'][col]['pattern'] = self._infer_pattern(sample)
                except:
                    pass
        
        return profile
    
    def _infer_pattern(self, sample: str) -> str:
        """Infer pattern from string sample"""
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if re.match(uuid_pattern, sample, re.IGNORECASE):
            return 'uuid'
        
        iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
        if re.match(iso_pattern, sample):
            return 'iso8601'
        
        sha_pattern = r'^[0-9a-f]{64}$'
        if re.match(sha_pattern, sample):
            return 'sha256'
        
        return 'string'
    
    def statistical_profiling(self, records: List[Dict], confidence_values: List[float] = None) -> Dict[str, Any]:
        """Statistical profiling focusing on confidence values"""
        stats = {}
        
        # Analyze confidence values if available
        if confidence_values:
            confidence_array = np.array(confidence_values)
            stats['confidence'] = {
                'min': float(confidence_array.min()),
                'max': float(confidence_array.max()),
                'mean': float(confidence_array.mean()),
                'median': float(np.median(confidence_array)),
                'std': float(confidence_array.std()),
                'count': len(confidence_values)
            }
            
            # Check for range violations
            if stats['confidence']['min'] < 0.0 or stats['confidence']['max'] > 1.0:
                stats['confidence']['warning'] = f'Values outside [0.0, 1.0]: min={stats["confidence"]["min"]:.2f}, max={stats["confidence"]["max"]:.2f}'
                stats['confidence']['severity'] = 'CRITICAL'
            elif stats['confidence']['max'] > 100:
                stats['confidence']['warning'] = f'Likely percentage scale (0-100) instead of 0.0-1.0: max={stats["confidence"]["max"]:.0f}'
                stats['confidence']['severity'] = 'CRITICAL'
        
        # Look for sequence numbers
        sequence_numbers = []
        for record in records:
            if 'sequence_number' in record:
                seq = record['sequence_number']
                if isinstance(seq, (int, float)):
                    sequence_numbers.append(seq)
        
        if sequence_numbers:
            seq_array = np.array(sequence_numbers)
            stats['sequence_number'] = {
                'min': int(seq_array.min()),
                'max': int(seq_array.max()),
                'mean': float(seq_array.mean()),
                'count': len(sequence_numbers)
            }
        
        # Check time fields
        time_fields = []
        for record in records:
            occurred = record.get('occurred_at')
            recorded = record.get('recorded_at')
            if occurred and recorded:
                try:
                    time_fields.append((occurred, recorded))
                except:
                    pass
        
        if time_fields:
            stats['time_order_violations'] = 0
            for occurred, recorded in time_fields:
                try:
                    if recorded < occurred:
                        stats['time_order_violations'] += 1
                except:
                    pass
        
        return stats
    
    def build_contract(self, structural: Dict, statistical: Dict, schema_type: str) -> Dict:
        """Build contract based on detected schema type"""
        
        if schema_type == "week3":
            return self._build_extraction_contract(structural, statistical)
        elif schema_type == "week5":
            return self._build_event_contract(structural, statistical)
        else:
            return self._build_generic_contract(structural, statistical)
    
    def _build_extraction_contract(self, structural: Dict, statistical: Dict) -> Dict:
        """Build contract for extraction data"""
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': 'Document Extraction Contract',
                'version': '1.0.0',
                'owner': 'extraction-team',
                'description': 'Contract for document extraction data'
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': str(self.source_path),
                    'format': 'jsonl'
                }
            },
            'schema': {
                'confidence': {
                    'type': 'number',
                    'minimum': 0.0,
                    'maximum': 1.0,
                    'required': True,
                    'description': 'Confidence score MUST be between 0.0 and 1.0 (float, not integer percentage)'
                }
            },
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks': [
                        {
                            'confidence_range': {
                                'condition': 'confidence BETWEEN 0.0 AND 1.0',
                                'severity': 'CRITICAL',
                                'description': 'Confidence must be float in [0.0, 1.0] - BREAKING if changed to 0-100'
                            }
                        },
                        {
                            'confidence_type': {
                                'condition': "typeof(confidence) = 'float'",
                                'severity': 'CRITICAL',
                                'description': 'Confidence must be float, not integer'
                            }
                        }
                    ]
                }
            },
            'lineage': {
                'downstream': [
                    {
                        'id': 'week4-cartographer',
                        'description': 'Cartographer consumes confidence for filtering low-quality facts',
                        'fields_consumed': ['confidence'],
                        'breaking_if_changed': ['confidence']
                    },
                    {
                        'id': 'week5-event-sourcing',
                        'description': 'Event sourcing may emit confidence-based events',
                        'fields_consumed': ['confidence']
                    }
                ]
            }
        }
        
        # Add statistical check if we have baseline
        if 'confidence' in statistical:
            stats = statistical['confidence']
            if stats.get('warning'):
                contract['quality']['specification']['checks'].append({
                    'statistical_drift': {
                        'condition': f"mean_confidence BETWEEN {max(0, stats['mean'] - 0.2):.2f} AND {min(1, stats['mean'] + 0.2):.2f}",
                        'severity': 'HIGH',
                        'description': f"Statistical drift detection - baseline mean: {stats['mean']:.3f}"
                    }
                })
        
        return contract
    
    def _build_event_contract(self, structural: Dict, statistical: Dict) -> Dict:
        """Build contract for event data"""
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': 'Event Sourcing Contract',
                'version': '1.0.0',
                'owner': 'events-team',
                'description': 'Contract for event sourcing data'
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': str(self.source_path),
                    'format': 'jsonl'
                }
            },
            'schema': {},
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks': []
                }
            }
        }
        
        # Add schema from structural profiling for simple fields
        for col, info in structural['columns'].items():
            if col not in ['extracted_facts', 'entities', 'payload']:  # Skip complex nested fields
                col_schema = {
                    'type': self._infer_type(info['dtype'])
                }
                if info['null_fraction'] == 0:
                    col_schema['required'] = True
                if info.get('pattern'):
                    col_schema['format'] = info['pattern']
                contract['schema'][col] = col_schema
        
        # Add quality checks
        checks = contract['quality']['specification']['checks']
        
        # Check sequence numbers
        if 'sequence_number' in statistical:
            checks.append({
                'sequence_positive': {
                    'condition': 'sequence_number >= 1',
                    'severity': 'HIGH',
                    'description': 'Sequence numbers must be positive'
                }
            })
        
        # Check time order violations
        if statistical.get('time_order_violations', 0) > 0:
            checks.append({
                'time_order': {
                    'condition': 'recorded_at >= occurred_at',
                    'severity': 'CRITICAL',
                    'description': 'recorded_at must be >= occurred_at',
                    'violations_detected': statistical['time_order_violations']
                }
            })
        
        return contract
    
    def _build_generic_contract(self, structural: Dict, statistical: Dict) -> Dict:
        """Build generic contract for any data"""
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': f'Contract for {self.source_path.stem}',
                'version': '1.0.0',
                'owner': 'data-team',
                'description': 'Auto-generated data contract'
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': str(self.source_path),
                    'format': 'jsonl'
                }
            },
            'schema': {},
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks': []
                }
            }
        }
        
        # Add schema for simple fields
        for col, info in structural['columns'].items():
            # Skip complex nested fields that might cause issues
            if info['dtype'] not in ['object'] or len(info['sample_values']) == 0:
                col_schema = {
                    'type': self._infer_type(info['dtype'])
                }
                if info['null_fraction'] == 0:
                    col_schema['required'] = True
                contract['schema'][col] = col_schema
        
        # Add confidence check if confidence values were found
        if 'confidence' in statistical:
            contract['quality']['specification']['checks'].append({
                'confidence_range': {
                    'condition': 'confidence BETWEEN 0.0 AND 1.0',
                    'severity': 'CRITICAL',
                    'description': 'Confidence must be in [0.0, 1.0] range'
                }
            })
        
        return contract
    
    def _infer_type(self, dtype: str) -> str:
        """Infer JSON schema type from pandas dtype"""
        if 'int' in dtype:
            return 'integer'
        elif 'float' in dtype:
            return 'number'
        elif 'bool' in dtype:
            return 'boolean'
        else:
            return 'string'
    
    def save_contract(self, contract: Dict):
        """Save contract as YAML"""
        output_path = self.output_dir / f"{self.source_path.stem}_contract.yaml"
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        print(f"✅ Contract saved to {output_path}")
        
        # Also save as JSON for easier parsing
        json_path = self.output_dir / f"{self.source_path.stem}_contract.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(contract, f, indent=2)
        print(f"✅ JSON contract saved to {json_path}")
        
        return output_path
    
    def save_validation_baseline(self, statistical: Dict):
        """Save statistical baseline for future drift detection"""
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        
        baseline_file = baseline_path / f"{self.source_path.stem}_baseline.json"
        with open(baseline_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'source': str(self.source_path),
                'statistics': statistical
            }, f, indent=2)
        
        print(f"📊 Baseline saved to {baseline_file}")
    
    def run(self):
        """Execute contract generation"""
        print(f"\n{'='*60}")
        print(f"📝 Fixed Contract Generator")
        print(f"{'='*60}")
        print(f"Source: {self.source_path}")
        
        # Load data
        df, records = self.load_data()
        
        # Detect schema type
        schema_type = self.detect_schema_type(records)
        print(f"📋 Detected schema type: {schema_type}")
        
        # Profile data
        structural = self.structural_profiling(df, records)
        
        # Extract confidence values for statistical profiling
        confidence_values = structural.get('confidence_values', [])
        statistical = self.statistical_profiling(records, confidence_values)
        
        print(f"   Found {len(structural['columns'])} columns")
        print(f"   Found {len(confidence_values)} confidence values")
        
        # Build contract
        contract = self.build_contract(structural, statistical, schema_type)
        
        # Save
        self.save_contract(contract)
        self.save_validation_baseline(statistical)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"✨ Generation complete!")
        print(f"{'='*60}")
        
        if 'confidence' in statistical:
            stats = statistical['confidence']
            print(f"\n📊 Confidence Analysis:")
            print(f"   Min: {stats['min']:.4f}")
            print(f"   Max: {stats['max']:.4f}")
            print(f"   Mean: {stats['mean']:.4f}")
            print(f"   Std: {stats['std']:.4f}")
            print(f"   Count: {stats['count']}")
            
            if stats.get('warning'):
                print(f"\n⚠️  WARNING: {stats['warning']}")
                print(f"   Severity: {stats.get('severity', 'UNKNOWN')}")
        
        if statistical.get('time_order_violations', 0) > 0:
            print(f"\n⚠️  Time order violations: {statistical['time_order_violations']} records have recorded_at < occurred_at")


def main():
    parser = argparse.ArgumentParser(description='Generate data contracts from JSONL files')
    parser.add_argument('--source', required=True, help='Path to JSONL source file')
    parser.add_argument('--output', required=True, help='Output directory for contracts')
    
    args = parser.parse_args()
    
    generator = FixedContractGenerator(args.source, args.output)
    generator.run()


if __name__ == '__main__':
    main()