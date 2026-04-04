#!/usr/bin/env python3
"""
Data-Driven Contract Generator - Analyzes actual data to generate contracts
"""

import argparse
import json
import yaml
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import re
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataDrivenContractGenerator:
    """Generates contracts by analyzing actual data"""
    
    def __init__(self, source_path: str, output_dir: str, register: bool = True):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.register = register
        
        if register:
            from contracts.registry import ContractRegistry, EnforcementMode
            self.registry = ContractRegistry()
            self.enforcement_mode = EnforcementMode.MONITOR
    
    def load_and_analyze(self) -> tuple[List[Dict], Dict]:
        """Load and analyze data to infer schema"""
        records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except:
                        pass
        
        if not records:
            raise ValueError(f"No valid records found in {self.source_path}")
        
        # Analyze data patterns
        analysis = self.analyze_data(records)
        return records, analysis
    
    def analyze_data(self, records: List[Dict]) -> Dict:
        """Analyze data to infer schema patterns"""
        analysis = {
            'record_count': len(records),
            'fields': {},
            'confidence_values': [],
            'date_fields': []
        }
        
        for record in records[:100]:  # Sample first 100 records
            for key, value in record.items():
                if key not in analysis['fields']:
                    analysis['fields'][key] = {
                        'type': type(value).__name__,
                        'samples': [],
                        'null_count': 0,
                        'unique_values': set()
                    }
                
                if value is None:
                    analysis['fields'][key]['null_count'] += 1
                else:
                    analysis['fields'][key]['samples'].append(str(value)[:100])
                    analysis['fields'][key]['unique_values'].add(str(value)[:50])
                
                # Extract confidence values from nested structures
                if key == 'extracted_facts' and isinstance(value, list):
                    for fact in value:
                        if isinstance(fact, dict) and 'confidence' in fact:
                            analysis['confidence_values'].append(fact['confidence'])
                
                # Detect date fields
                if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T', value):
                    analysis['date_fields'].append(key)
        
        # Convert sets to lists for JSON serialization
        for field in analysis['fields']:
            analysis['fields'][field]['unique_values'] = list(analysis['fields'][field]['unique_values'])[:10]
            analysis['fields'][field]['samples'] = analysis['fields'][field]['samples'][:5]
        
        return analysis
    
    def generate_contract(self, records: List[Dict], analysis: Dict) -> Dict:
        """Generate contract based on actual data"""
        
        # Determine if this is Week 3 extraction data
        is_extraction = any('extracted_facts' in r for r in records[:5])
        
        if is_extraction:
            return self.generate_extraction_contract(records, analysis)
        else:
            return self.generate_generic_contract(records, analysis)
    
    def generate_extraction_contract(self, records: List[Dict], analysis: Dict) -> Dict:
        """Generate contract for extraction data with confidence validation"""
        
        confidence_values = analysis['confidence_values']
        has_violation = any(c > 1.0 for c in confidence_values) if confidence_values else False
        
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': 'Week 3 Document Refinery - Extraction Records',
                'version': '1.0.0',
                'owner': 'extraction-team',
                'description': 'Data-driven contract generated from actual extraction data',
                'tags': ['extraction', 'nlp', 'critical']
            },
            'compatibility': 'backward',
            'schema': {
                'doc_id': {
                    'type': 'string',
                    'format': 'uuid',
                    'required': True,
                    'unique': True,
                    'description': 'Document identifier'
                },
                'extracted_facts': {
                    'type': 'array',
                    'minItems': 1,
                    'required': True,
                    'items': {
                        'type': 'object',
                        'required': ['fact_id', 'text', 'confidence'],
                        'properties': {
                            'confidence': {
                                'type': 'number',
                                'minimum': 0.0,
                                'maximum': 1.0,
                                'required': True,
                                'description': 'Confidence score MUST be in [0.0, 1.0] - BREAKING if changed'
                            }
                        }
                    }
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
                                'description': 'Confidence must be in [0.0, 1.0] range'
                            }
                        },
                        {
                            'confidence_type': {
                                'condition': "typeof(confidence) = 'float'",
                                'severity': 'CRITICAL',
                                'description': 'Confidence must be float, not integer'
                            }
                        },
                        {
                            'confidence_not_null': {
                                'condition': 'confidence IS NOT NULL',
                                'severity': 'CRITICAL'
                            }
                        },
                        {
                            'doc_id_not_null': {
                                'condition': 'doc_id IS NOT NULL',
                                'severity': 'CRITICAL'
                            }
                        },
                        {
                            'doc_id_unique': {
                                'condition': 'COUNT(DISTINCT doc_id) = COUNT(*)',
                                'severity': 'CRITICAL'
                            }
                        },
                        {
                            'min_confidence': {
                                'condition': 'MIN(confidence) >= 0.0',
                                'severity': 'HIGH'
                            }
                        },
                        {
                            'max_confidence': {
                                'condition': 'MAX(confidence) <= 1.0',
                                'severity': 'HIGH'
                            }
                        },
                        {
                            'row_count': {
                                'condition': 'COUNT(*) >= 10',
                                'severity': 'MEDIUM'
                            }
                        }
                    ]
                }
            },
            'lineage': {
                'downstream': [
                    {'id': 'week4-cartographer', 'fields_consumed': ['confidence', 'doc_id'], 'breaking_if_changed': ['confidence']},
                    {'id': 'week5-event-sourcing', 'fields_consumed': ['doc_id'], 'breaking_if_changed': ['doc_id']}
                ]
            },
            'statistics': {
                'total_records_analyzed': analysis['record_count'],
                'confidence_violations_detected': has_violation,
                'confidence_range': {
                    'min': min(confidence_values) if confidence_values else None,
                    'max': max(confidence_values) if confidence_values else None,
                    'mean': sum(confidence_values)/len(confidence_values) if confidence_values else None
                }
            }
        }
        
        return contract
    
    def generate_generic_contract(self, records: List[Dict], analysis: Dict) -> Dict:
        """Generate generic contract for any data"""
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': f'Contract for {self.source_path.stem}',
                'version': '1.0.0',
                'owner': 'data-team',
                'description': 'Auto-generated contract from actual data'
            },
            'schema': {},
            'quality': {'checks': []}
        }
        
        # Add schema fields based on analysis
        for field, info in analysis['fields'].items():
            if info['null_count'] == 0:
                required = True
            else:
                required = False
            
            contract['schema'][field] = {
                'type': self._infer_json_type(info['type']),
                'required': required
            }
            
            if len(info['unique_values']) == len(records) and len(records) > 0:
                contract['schema'][field]['unique'] = True
            
            # Add null check for required fields
            if required:
                contract['quality']['checks'].append({
                    f'{field}_not_null': {
                        'condition': f'{field} IS NOT NULL',
                        'severity': 'HIGH'
                    }
                })
        
        return contract
    
    def _infer_json_type(self, python_type: str) -> str:
        mapping = {
            'str': 'string',
            'int': 'integer',
            'float': 'number',
            'bool': 'boolean',
            'list': 'array',
            'dict': 'object'
        }
        return mapping.get(python_type, 'string')
    
    def save_contract(self, contract: Dict):
        """Save contract and register if enabled"""
        output_path = self.output_dir / f"{self.source_path.stem}_contract.yaml"
        with open(output_path, 'w') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        print(f"✅ Contract saved: {output_path}")
        
        if self.register:
            from contracts.registry import EnforcementMode
            contract_id = self.registry.register_contract(contract, EnforcementMode.WARN)
            print(f"✅ Contract registered: {contract_id}")
        
        return output_path
    
    def run(self):
        """Generate contract from actual data"""
        print(f"\n📊 Analyzing data from: {self.source_path}")
        records, analysis = self.load_and_analyze()
        print(f"   Records analyzed: {analysis['record_count']}")
        
        if analysis['confidence_values']:
            print(f"   Confidence values: {len(analysis['confidence_values'])} found")
            print(f"   Range: {min(analysis['confidence_values'])} - {max(analysis['confidence_values'])}")
        
        contract = self.generate_contract(records, analysis)
        self.save_contract(contract)
        
        print(f"\n✅ Contract generation complete!")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, help='Source JSONL file')
    parser.add_argument('--output', required=True, help='Output directory')
    parser.add_argument('--no-register', action='store_true', help='Skip registry registration')
    
    args = parser.parse_args()
    
    generator = DataDrivenContractGenerator(args.source, args.output, register=not args.no_register)
    generator.run()


if __name__ == '__main__':
    main()