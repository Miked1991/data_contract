#!/usr/bin/env python3
"""
ContractGenerator - Data-Driven Contract Generation with Full Profiling
Generates contracts from JSONL data with structural and statistical profiling
"""

import json
import yaml
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import sys
import os
import random

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataDrivenContractGenerator:
    """
    Generates data contracts by profiling actual JSONL data.
    Includes structural profiling, statistical profiling, LLM annotation,
    dbt schema generation, and lineage-based consumer population.
    """
    
    def __init__(self, source_path: str, output_dir: str, register: bool = False):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.register = register
        
        # Data storage
        self.records = []
        self.df_flat = None
        self.structural_profile = {}
        self.statistical_profile = {}
        self.baselines = {}
        self.lineage_graph = None
        
        # Load lineage graph if available
        self._load_lineage_graph()
    
    def _load_lineage_graph(self):
        """Load Week 4 lineage graph for downstream consumer detection"""
        lineage_path = Path("outputs/week4/lineage_snapshots.jsonl")
        if lineage_path.exists():
            try:
                with open(lineage_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            self.lineage_graph = json.loads(line)
                            break
                print(f"📊 Loaded lineage graph from {lineage_path}")
            except Exception as e:
                print(f"⚠️ Could not load lineage graph: {e}")
    
    # ============================================================
    # DATA LOADING & PROFILING
    # ============================================================
    
    def load_data(self) -> List[Dict]:
        """Load JSONL data and prepare for profiling"""
        self.records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        record = json.loads(line)
                        self.records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Skipping line {line_num}: {e}")
        
        if not self.records:
            raise ValueError(f"No valid records found in {self.source_path}")
        
        print(f"📖 Loaded {len(self.records)} records from {self.source_path}")
        return self.records
    
    def structural_profiling(self) -> Dict[str, Any]:
        """
        Step 1: Structural profiling - derives column names, types, nullability, required fields
        """
        print("\n🔍 Running Structural Profiling...")
        
        profile = {
            'record_count': len(self.records),
            'source_file': str(self.source_path),
            'columns': {}
        }
        
        # Flatten first record to understand structure
        sample_record = self.records[0]
        
        # Analyze each field in the record
        for field_path, field_value in self._flatten_record(sample_record).items():
            col_profile = {
                'original_path': field_path,
                'display_name': field_path.split('.')[-1] if '.' in field_path else field_path,
                'samples': [],
                'null_count': 0,
                'type_counts': {},
                'inferred_type': None,
                'is_required': True,
                'unique_values': set(),
                'is_numeric': False,
                'is_confidence': 'confidence' in field_path.lower()
            }
            
            # Collect samples across records (first 100 for performance)
            for record in self.records[:100]:
                value = self._get_nested_value(record, field_path)
                
                if value is None:
                    col_profile['null_count'] += 1
                else:
                    # Track type
                    value_type = type(value).__name__
                    col_profile['type_counts'][value_type] = col_profile['type_counts'].get(value_type, 0) + 1
                    
                    # Store sample (up to 5)
                    if len(col_profile['samples']) < 5:
                        col_profile['samples'].append(str(value)[:100])
                    
                    # Track unique values (up to 20)
                    if len(col_profile['unique_values']) < 20:
                        col_profile['unique_values'].add(str(value)[:50])
            
            # Determine inferred type (majority type)
            if col_profile['type_counts']:
                col_profile['inferred_type'] = max(col_profile['type_counts'], key=col_profile['type_counts'].get)
            
            # Determine if required (no nulls in sample)
            col_profile['is_required'] = col_profile['null_count'] == 0
            
            # Check if numeric
            col_profile['is_numeric'] = col_profile['inferred_type'] in ['int', 'float']
            
            # Convert unique_values set to list for JSON
            col_profile['unique_values'] = list(col_profile['unique_values'])
            col_profile['cardinality'] = len(col_profile['unique_values'])
            
            profile['columns'][field_path] = col_profile
        
        self.structural_profile = profile
        print(f"   Found {len(profile['columns'])} columns")
        
        # Print summary of confidence fields found
        confidence_cols = [c for c, info in profile['columns'].items() if info['is_confidence']]
        if confidence_cols:
            print(f"   🔍 Confidence fields detected: {len(confidence_cols)}")
            for col in confidence_cols:
                print(f"      - {col}")
        
        return profile
    
    def statistical_profiling(self) -> Dict[str, Any]:
        """
        Step 2: Statistical profiling - derives min, max, mean for numeric columns
        """
        print("\n📈 Running Statistical Profiling...")
        
        stats = {}
        
        # Identify numeric columns from structural profile
        numeric_columns = [col for col, info in self.structural_profile['columns'].items() 
                          if info['is_numeric']]
        
        for col_path in numeric_columns:
            values = []
            for record in self.records:
                value = self._get_nested_value(record, col_path)
                if value is not None and isinstance(value, (int, float)):
                    values.append(float(value))
            
            if values:
                col_stats = {
                    'min': min(values),
                    'max': max(values),
                    'mean': sum(values) / len(values),
                    'median': sorted(values)[len(values)//2],
                    'stddev': self._calculate_stddev(values),
                    'count': len(values),
                    'null_count': len(self.records) - len(values),
                    'range': max(values) - min(values)
                }
                
                # Special handling for confidence columns
                if self.structural_profile['columns'][col_path]['is_confidence']:
                    col_stats['is_confidence'] = True
                    col_stats['expected_range'] = '[0.0, 1.0]'
                    col_stats['has_violation'] = col_stats['min'] < 0.0 or col_stats['max'] > 1.0
                    
                    if col_stats['has_violation']:
                        print(f"   ⚠️ Confidence column '{col_path}' has values outside [0.0, 1.0]")
                        print(f"      Range: {col_stats['min']} - {col_stats['max']}")
                
                stats[col_path] = col_stats
        
        self.statistical_profile = stats
        print(f"   Profiled {len(stats)} numeric columns")
        
        return stats
    
    def _flatten_record(self, record: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dictionary for column analysis"""
        items = {}
        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_record(v, new_key, sep=sep))
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                # For list of dicts, analyze first element structure
                if v:
                    items.update(self._flatten_record(v[0], f"{new_key}[*]", sep=sep))
            else:
                items[new_key] = v
        return items
    
    def _get_nested_value(self, record: Dict, path: str, default=None):
        """Get value from nested dict using dot notation"""
        parts = path.split('.')
        current = record
        for part in parts:
            # Handle array notation [*]
            if '[*]' in part:
                field = part.replace('[*]', '')
                if isinstance(current, dict) and field in current:
                    arr = current[field]
                    if arr and isinstance(arr, list) and len(arr) > 0:
                        current = arr[0]
                    else:
                        return default
                else:
                    return default
            else:
                if isinstance(current, dict):
                    current = current.get(part, default)
                else:
                    return default
        return current
    
    def _calculate_stddev(self, values: List[float]) -> float:
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
    
    # ============================================================
    # BASELINE STORAGE
    # ============================================================
    
    def save_baselines(self):
        """
        Step 4: Baseline write logic - stores mean and stddev per numeric column
        to schema_snapshots/baselines.json
        """
        print("\n💾 Saving Statistical Baselines...")
        
        baseline_path = Path("schema_snapshots/baselines.json")
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing baselines if any
        existing_baselines = {}
        if baseline_path.exists():
            try:
                with open(baseline_path, 'r') as f:
                    existing_baselines = json.load(f)
            except:
                pass
        
        # Prepare new baselines
        contract_id = f"{self.source_path.stem}_contract"
        baselines = {
            'contract_id': contract_id,
            'timestamp': datetime.now().isoformat(),
            'source_file': str(self.source_path),
            'statistics': self.statistical_profile
        }
        
        # Store in existing baselines
        existing_baselines[contract_id] = baselines
        
        # Save to file
        with open(baseline_path, 'w') as f:
            json.dump(existing_baselines, f, indent=2)
        
        print(f"   ✅ Baselines saved to {baseline_path}")
        print(f"   📊 Stored statistics for {len(self.statistical_profile)} numeric columns")
        
        # Print confidence baseline if present
        for col, stats in self.statistical_profile.items():
            if stats.get('is_confidence'):
                print(f"   🔍 Confidence baseline: {col} - mean={stats['mean']:.4f}, stddev={stats['stddev']:.4f}")
        
        self.baselines = existing_baselines
        return baseline_path
    
    # ============================================================
    # DOWNSTREAM CONSUMER POPULATION
    # ============================================================
    
    def get_downstream_consumers(self) -> List[Dict]:
        """
        Step 5: Downstream consumer population from Week 4 lineage graph
        """
        print("\n🔗 Populating Downstream Consumers from Lineage Graph...")
        
        consumers = []
        
        if self.lineage_graph:
            edges = self.lineage_graph.get('edges', [])
            nodes = self.lineage_graph.get('nodes', [])
            
            # Find nodes that consume confidence-related data
            for edge in edges:
                relationship = edge.get('relationship', '')
                if relationship in ['CONSUMES', 'READS', 'PRODUCES']:
                    target_id = edge.get('target')
                    source_id = edge.get('source')
                    
                    # Find node details
                    target_node = next((n for n in nodes if n.get('node_id') == target_id), {})
                    source_node = next((n for n in nodes if n.get('node_id') == source_id), {})
                    
                    consumer = {
                        'id': target_node.get('node_id', 'unknown'),
                        'name': target_node.get('label', target_node.get('node_id', 'unknown')),
                        'type': target_node.get('type', 'unknown'),
                        'source': source_node.get('label', 'unknown'),
                        'relationship': relationship,
                        'fields_consumed': self._detect_consumed_fields(source_node, target_node)
                    }
                    
                    if consumer['id'] != 'unknown':
                        consumers.append(consumer)
            
            # Remove duplicates
            unique_consumers = {c['id']: c for c in consumers}.values()
            consumers = list(unique_consumers)
        
        # If no lineage graph, use default consumers
        if not consumers:
            print("   ⚠️ No lineage graph found, using default consumers")
            consumers = [
                {'id': 'week4-cartographer', 'name': 'Week 4 Cartographer', 'type': 'FILE', 
                 'fields_consumed': ['doc_id', 'extracted_facts[*].confidence']},
                {'id': 'week5-event-sourcing', 'name': 'Week 5 Event Sourcing', 'type': 'SERVICE',
                 'fields_consumed': ['doc_id', 'extracted_at']},
                {'id': 'week2-digital-courtroom', 'name': 'Week 2 Digital Courtroom', 'type': 'SERVICE',
                 'fields_consumed': ['extracted_facts[*].confidence']}
            ]
        
        print(f"   ✅ Found {len(consumers)} downstream consumers")
        for consumer in consumers[:3]:
            print(f"      - {consumer['name']} (consumes: {', '.join(consumer.get('fields_consumed', [])[:2])})")
        
        return consumers
    
    def _detect_consumed_fields(self, source_node: Dict, target_node: Dict) -> List[str]:
        """Detect which fields are consumed based on node metadata"""
        fields = []
        
        # Check source metadata for fields
        source_metadata = source_node.get('metadata', {})
        if 'fields' in source_metadata:
            fields.extend(source_metadata['fields'])
        
        # Add confidence as default for extraction data
        if 'extractor' in str(source_node.get('label', '')).lower():
            fields.append('confidence')
        
        return list(set(fields)) if fields else ['doc_id', 'confidence']
    
    # ============================================================
    # LLM ANNOTATION (Simulated)
    # ============================================================
    
    def llm_annotate_columns(self) -> Dict[str, Dict]:
        """
        Step 5 (continued): LLM annotation for ambiguous columns
        Simulates LLM call to infer business meaning of ambiguous columns
        """
        print("\n🤖 Running LLM Annotation for Ambiguous Columns...")
        
        annotations = {}
        
        for col_path, col_info in self.structural_profile['columns'].items():
            # Identify ambiguous columns (cryptic names, low cardinality, etc.)
            is_ambiguous = (
                len(col_path) > 30 or
                any(c in col_path.lower() for c in ['tmp', 'temp', 'unknown', 'misc']) or
                (col_info['cardinality'] < 10 and col_info['cardinality'] > 1 and 
                 not col_info['is_confidence'])
            )
            
            if is_ambiguous or col_info['is_confidence']:
                # Simulate LLM annotation
                annotation = self._simulate_llm_annotation(col_path, col_info)
                annotations[col_path] = annotation
                
                print(f"   📝 {col_path}: {annotation['description'][:60]}...")
        
        return annotations
    
    def _simulate_llm_annotation(self, col_path: str, col_info: Dict) -> Dict:
        """Simulate LLM call for column annotation"""
        field_name = col_path.split('.')[-1]
        
        # Business rules based on field name patterns
        if 'confidence' in field_name.lower():
            return {
                'description': f"Confidence score for extracted facts. Higher values indicate greater certainty.",
                'business_rule': "MUST be between 0.0 and 1.0 inclusive. Values > 1.0 indicate scale error.",
                'validation_expression': "confidence BETWEEN 0.0 AND 1.0",
                'cross_column_relationship': "Used for filtering in Week 4 Cartographer",
                'sensitivity': 'MEDIUM'
            }
        elif 'doc_id' in field_name.lower():
            return {
                'description': f"Unique document identifier. Primary key for extraction records.",
                'business_rule': "MUST be unique across all records. Format: UUID or system-generated ID.",
                'validation_expression': "doc_id IS NOT NULL AND doc_id != ''",
                'cross_column_relationship': "Referenced by Week 5 Event Sourcing",
                'sensitivity': 'HIGH'
            }
        elif 'source_path' in field_name.lower():
            return {
                'description': f"File system path or URL to source document.",
                'business_rule': "MUST point to accessible storage location.",
                'validation_expression': "source_path IS NOT NULL AND source_path != ''",
                'cross_column_relationship': "Used for source verification",
                'sensitivity': 'MEDIUM'
            }
        elif 'extracted_at' in field_name.lower():
            return {
                'description': f"Timestamp when extraction was performed.",
                'business_rule': "MUST be ISO 8601 format with timezone.",
                'validation_expression': "extracted_at IS NOT NULL",
                'cross_column_relationship': "Used for freshness monitoring",
                'sensitivity': 'LOW'
            }
        else:
            return {
                'description': f"Field '{field_name}' from extraction data. {col_info['samples'][0][:50]}...",
                'business_rule': f"Maintain as {col_info['inferred_type']} type.",
                'validation_expression': f"{field_name} IS NOT NULL" if col_info['is_required'] else "",
                'cross_column_relationship': "No known cross-column dependencies",
                'sensitivity': 'LOW'
            }
    
    # ============================================================
    # CONTRACT CLAUSE CONSTRUCTION
    # ============================================================
    
    def build_confidence_clause(self) -> Dict:
        """
        Step 3: Confidence range constraint - explicitly sets minimum: 0.0 and maximum: 1.0
        """
        return {
            'confidence_range': {
                'condition': 'confidence BETWEEN 0.0 AND 1.0',
                'severity': 'CRITICAL',
                'description': 'Confidence score MUST be in [0.0, 1.0] range',
                'minimum': 0.0,
                'maximum': 1.0,
                'type': 'float',
                'clause_type': 'range_constraint',
                'reason': 'BREAKING CHANGE if changed to 0-100 scale'
            }
        }
    
    def build_contract_clauses(self) -> List[Dict]:
        """
        Build all contract clauses from profiling data
        """
        clauses = []
        
        # 1. Confidence range clause (CRITICAL)
        clauses.append(self.build_confidence_clause())
        
        # 2. Confidence type clause
        clauses.append({
            'confidence_type': {
                'condition': "typeof(confidence) = 'float'",
                'severity': 'CRITICAL',
                'description': 'Confidence must be float, not integer',
                'clause_type': 'type_constraint'
            }
        })
        
        # 3. Required field clauses
        for col_path, col_info in self.structural_profile['columns'].items():
            if col_info['is_required'] and not col_info['is_confidence']:
                field_name = col_path.split('.')[-1]
                clauses.append({
                    f'{field_name}_not_null': {
                        'condition': f'{field_name} IS NOT NULL',
                        'severity': 'HIGH',
                        'description': f'{field_name} is required for all records'
                    }
                })
        
        # 4. Unique constraint clauses
        for col_path, col_info in self.structural_profile['columns'].items():
            if col_info.get('cardinality', 0) == self.structural_profile['record_count']:
                field_name = col_path.split('.')[-1]
                clauses.append({
                    f'{field_name}_unique': {
                        'condition': f'COUNT(DISTINCT {field_name}) = COUNT(*)',
                        'severity': 'HIGH',
                        'description': f'{field_name} must be unique across all records'
                    }
                })
        
        # 5. Statistical drift clause
        for col_path, stats in self.statistical_profile.items():
            if stats.get('is_confidence'):
                clauses.append({
                    'confidence_statistical_drift': {
                        'warn_condition': f"abs(mean(confidence) - {stats['mean']:.3f}) > 2*{stats['stddev']:.3f}",
                        'fail_condition': f"abs(mean(confidence) - {stats['mean']:.3f}) > 3*{stats['stddev']:.3f}",
                        'severity': 'HIGH',
                        'description': 'Detects silent corruption like 0.0-1.0 → 0-100 scale change',
                        'baseline_mean': stats['mean'],
                        'baseline_stddev': stats['stddev']
                    }
                })
        
        # 6. Row count clause
        clauses.append({
            'row_count_minimum': {
                'condition': f'COUNT(*) >= {max(10, self.structural_profile["record_count"] // 2)}',
                'severity': 'MEDIUM',
                'description': 'Sufficient data volume for statistical validity'
            }
        })
        
        return clauses
    
    # ============================================================
    # DBT SCHEMA CONSTRUCTION
    # ============================================================
    
    def build_dbt_schema(self, contract_id: str) -> Dict:
        """
        Build dbt-compatible schema.yml file with tests
        """
        print("\n📄 Building dbt schema.yml...")
        
        dbt_schema = {
            'version': 2,
            'models': [
                {
                    'name': contract_id.replace('-', '_'),
                    'description': 'Auto-generated from actual data profiling',
                    'config': {
                        'materialized': 'table',
                        'tags': ['contract', 'extraction']
                    },
                    'columns': [],
                    'tests': []
                }
            ]
        }
        
        model = dbt_schema['models'][0]
        
        # Add model-level tests
        model['tests'].append({
            'dbt_utils.unique_combination_of_columns': {
                'combination_of_columns': ['doc_id']
            }
        })
        
        # Add column-level tests
        for col_path, col_info in self.structural_profile['columns'].items():
            field_name = col_path.split('.')[-1]
            column = {'name': field_name}
            tests = []
            
            # Not null test for required fields
            if col_info['is_required']:
                tests.append('not_null')
            
            # Unique test for high cardinality fields
            if col_info.get('cardinality', 0) == self.structural_profile['record_count']:
                tests.append('unique')
            
            # Range test for confidence fields
            if col_info['is_confidence']:
                tests.append({
                    'dbt_utils.accepted_range': {
                        'min_value': 0.0,
                        'max_value': 1.0,
                        'inclusive': True
                    }
                })
                tests.append({
                    'dbt_utils.expression_is_true': {
                        'expression': "confidence = CAST(confidence AS FLOAT)",
                        'severity': 'error'
                    }
                })
            
            # Accepted values for enum-like fields
            if 2 <= col_info['cardinality'] <= 20 and not col_info['is_numeric']:
                tests.append({
                    'accepted_values': {
                        'values': col_info['unique_values'][:10],
                        'quote': True
                    }
                })
            
            if tests:
                column['tests'] = tests
            
            model['columns'].append(column)
        
        # Add statistical monitoring test
        model['tests'].append({
            'dbt_utils.expression_is_true': {
                'expression': "COUNT(*) >= 10",
                'severity': 'warn'
            }
        })
        
        print(f"   ✅ Built dbt schema with {len(model['columns'])} columns and {len(model['tests'])} model tests")
        
        return dbt_schema
    
    # ============================================================
    # MAIN CONTRACT GENERATION
    # ============================================================
    
    def generate_contract(self) -> Dict:
        """
        Generate complete contract with all clauses
        """
        print("\n📝 Building Contract...")
        
        # Get downstream consumers from lineage graph
        downstream_consumers = self.get_downstream_consumers()
        
        # Get LLM annotations
        llm_annotations = self.llm_annotate_columns()
        
        # Build all clauses
        clauses = self.build_contract_clauses()
        
        # Build schema section
        schema = {}
        for col_path, col_info in self.structural_profile['columns'].items():
            field_name = col_path.split('.')[-1]
            schema[field_name] = {
                'type': self._map_type_to_json(col_info['inferred_type']),
                'required': col_info['is_required'],
                'description': llm_annotations.get(col_path, {}).get('description', f'Field from {col_path}')
            }
            
            if col_info['is_confidence']:
                schema[field_name]['minimum'] = 0.0
                schema[field_name]['maximum'] = 1.0
                schema[field_name]['format'] = 'float'
        
        # Build full contract
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}_contract",
            'info': {
                'title': 'Week 3 Document Refinery - Extraction Records',
                'version': '1.0.0',
                'owner': 'extraction-team',
                'description': 'Data-driven contract generated from actual extraction data',
                'generated_at': datetime.now().isoformat(),
                'source_records': len(self.records),
                'tags': ['extraction', 'nlp', 'critical', 'auto-generated']
            },
            'compatibility': 'backward',
            'servers': {
                'local': {
                    'type': 'local',
                    'path': str(self.source_path),
                    'format': 'jsonl'
                }
            },
            'schema': schema,
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks': clauses
                }
            },
            'llm_annotations': llm_annotations,
            'lineage': {
                'upstream': [],
                'downstream': downstream_consumers
            },
            'statistics': self.statistical_profile
        }
        
        print(f"   ✅ Built contract with {len(clauses)} quality clauses")
        print(f"   🔒 Confidence range clause: min=0.0, max=1.0")
        print(f"   🔗 Downstream consumers: {len(downstream_consumers)}")
        print(f"   🤖 LLM annotations: {len(llm_annotations)} columns")
        
        return contract
    
    def _map_type_to_json(self, python_type: str) -> str:
        """Map Python type to JSON Schema type"""
        mapping = {
            'str': 'string',
            'int': 'integer',
            'float': 'number',
            'bool': 'boolean',
            'list': 'array',
            'dict': 'object',
            'NoneType': 'null'
        }
        return mapping.get(python_type, 'string')
    
    def save_contract(self, contract: Dict):
        """Save contract and dbt schema to files"""
        
        # Save YAML contract
        contract_path = self.output_dir / f"{self.source_path.stem}_contract.yaml"
        with open(contract_path, 'w') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        print(f"\n✅ Contract saved: {contract_path}")
        
        # Save dbt schema
        dbt_schema = self.build_dbt_schema(contract['id'])
        dbt_path = self.output_dir / f"{self.source_path.stem}_dbt.yml"
        with open(dbt_path, 'w') as f:
            yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)
        print(f"✅ dbt schema saved: {dbt_path}")
        
        # Save JSON version
        json_path = self.output_dir / f"{self.source_path.stem}_contract.json"
        with open(json_path, 'w') as f:
            json.dump(contract, f, indent=2)
        print(f"✅ JSON contract saved: {json_path}")
        
        return contract_path
    
    # ============================================================
    # RUN PIPELINE
    # ============================================================
    
    def run(self):
        """Execute complete contract generation pipeline"""
        print("\n" + "=" * 70)
        print("🔷 DATA-DRIVEN CONTRACT GENERATOR")
        print("=" * 70)
        print(f"Source: {self.source_path}")
        print(f"Output: {self.output_dir}")
        
        # Step 1: Load data
        self.load_data()
        
        # Step 2: Structural profiling
        self.structural_profiling()
        
        # Step 3: Statistical profiling
        self.statistical_profiling()
        
        # Step 4: Save baselines
        self.save_baselines()
        
        # Step 5: Generate contract (includes downstream consumers and LLM annotation)
        contract = self.generate_contract()
        
        # Step 6: Save contract and dbt schema
        self.save_contract(contract)
        
        # Print summary
        print("\n" + "=" * 70)
        print("✅ CONTRACT GENERATION COMPLETE")
        print("=" * 70)
        print(f"\n📊 Summary:")
        print(f"   Records analyzed: {len(self.records)}")
        print(f"   Columns profiled: {len(self.structural_profile['columns'])}")
        print(f"   Numeric columns: {len(self.statistical_profile)}")
        print(f"   Quality clauses: {len(contract['quality']['specification']['checks'])}")
        print(f"   Downstream consumers: {len(contract['lineage']['downstream'])}")
        print(f"   LLM annotations: {len(contract.get('llm_annotations', {}))}")
        
        # Confidence range confirmation
        confidence_clause = None
        for check in contract['quality']['specification']['checks']:
            if 'confidence_range' in check:
                confidence_clause = check['confidence_range']
                break
        
        if confidence_clause:
            print(f"\n🔒 Confidence Range Clause:")
            print(f"   Condition: {confidence_clause['condition']}")
            print(f"   Min: {confidence_clause.get('minimum', 0.0)}")
            print(f"   Max: {confidence_clause.get('maximum', 1.0)}")
            print(f"   Severity: {confidence_clause['severity']}")
        
        return contract


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generate data contracts with full profiling')
    parser.add_argument('--source', required=True, help='Source JSONL file')
    parser.add_argument('--output', required=True, help='Output directory')
    parser.add_argument('--no-register', action='store_true', help='Skip registry registration')
    
    args = parser.parse_args()
    
    generator = DataDrivenContractGenerator(args.source, args.output, register=not args.no_register)
    generator.run()


if __name__ == '__main__':
    main() 