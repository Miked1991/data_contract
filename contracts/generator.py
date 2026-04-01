# contracts/generator.py
#!/usr/bin/env python3
"""
Enhanced ContractGenerator with complete contract generation for Week 3 and Week 5
"""

import argparse
import json
import yaml
import uuid
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
import random


class CompleteContractGenerator:
    """Generates complete contracts with dbt-compatible outputs"""
    
    def __init__(self, source_path: str, output_dir: str):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.week_type = self.detect_week_type()
        
    def detect_week_type(self) -> str:
        """Detect which week based on filename or content"""
        filename = str(self.source_path).lower()
        
        if 'extraction' in filename or 'ledger' in filename:
            return 'week3'
        elif 'event' in filename:
            return 'week5'
        return 'generic'
    
    def load_data(self) -> tuple[List[Dict], pd.DataFrame]:
        """Load and prepare data for contract generation"""
        records = []
        with open(self.source_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except:
                        pass
        
        if not records:
            records = self.create_sample_data(50)
        
        # Flatten for analysis
        df = pd.json_normalize(records[:100], max_level=1)
        return records[:100], df
    
    def create_sample_data(self, num_records: int) -> List[Dict]:
        """Create sample data if needed"""
        if self.week_type == 'week3':
            return self.create_sample_week3_data(num_records)
        else:
            return self.create_sample_week5_data(num_records)
    
    def create_sample_week3_data(self, num_records: int) -> List[Dict]:
        """Create sample Week 3 data"""
        records = []
        models = ["claude-3-5-sonnet-20241022", "gpt-4", "llama-3-70b"]
        entity_types = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]
        
        for i in range(num_records):
            if i < 5:
                confidence = random.randint(50, 100)  # Violation for testing
            else:
                confidence = round(random.uniform(0.3, 0.99), 3)
            
            facts = []
            for j in range(random.randint(1, 3)):
                facts.append({
                    "fact_id": str(uuid.uuid4()),
                    "text": f"Extracted fact {j+1}",
                    "entity_refs": [str(uuid.uuid4()) for _ in range(random.randint(0, 2))],
                    "confidence": confidence,
                    "page_ref": random.randint(1, 15) if random.random() > 0.3 else None,
                    "source_excerpt": "Original text excerpt..."
                })
            
            entities = []
            for _ in range(random.randint(1, 4)):
                entities.append({
                    "entity_id": str(uuid.uuid4()),
                    "name": f"Entity {_+1}",
                    "type": random.choice(entity_types),
                    "canonical_value": "Canonical value"
                })
            
            record = {
                "doc_id": str(uuid.uuid4()),
                "source_path": f"/data/document_{i+1:04d}.pdf",
                "source_hash": hashlib.sha256(f"content_{i}".encode()).hexdigest(),
                "extracted_facts": facts,
                "entities": entities,
                "extraction_model": random.choice(models),
                "processing_time_ms": random.randint(500, 5000),
                "token_count": {"input": random.randint(1000, 8000), "output": random.randint(200, 1500)},
                "extracted_at": datetime.now().isoformat() + "Z"
            }
            records.append(record)
        
        return records
    
    def create_sample_week5_data(self, num_records: int) -> List[Dict]:
        """Create sample Week 5 data"""
        records = []
        event_types = ["DocumentProcessed", "ExtractionCompleted", "ValidationFailed", "EntityExtracted"]
        sequence_tracker = {}
        
        for i in range(num_records):
            aggregate_id = str(uuid.uuid4())
            key = f"Document:{aggregate_id}"
            if key not in sequence_tracker:
                sequence_tracker[key] = 1
            else:
                sequence_tracker[key] += 1
            
            occurred_at = datetime.now() - timedelta(seconds=random.randint(0, 3600))
            recorded_at = occurred_at + timedelta(seconds=random.randint(1, 30))
            
            if i >= num_records - 2:
                recorded_at = occurred_at - timedelta(seconds=5)  # Violation
            
            record = {
                "event_id": str(uuid.uuid4()),
                "event_type": random.choice(event_types),
                "aggregate_id": aggregate_id,
                "aggregate_type": "Document",
                "sequence_number": sequence_tracker[key],
                "payload": {"status": "success", "data": f"Sample data {i+1}"},
                "metadata": {
                    "causation_id": None,
                    "correlation_id": str(uuid.uuid4()),
                    "user_id": f"user_{random.randint(1, 100)}",
                    "source_service": "week3-document-refinery"
                },
                "schema_version": "1.0",
                "occurred_at": occurred_at.isoformat() + "Z",
                "recorded_at": recorded_at.isoformat() + "Z"
            }
            records.append(record)
        
        return records
    
    def generate_week3_contract(self, records: List[Dict]) -> Dict:
        """Generate comprehensive Week 3 contract with 12+ clauses"""
        
        # Extract confidence values for statistics
        confidence_values = []
        for record in records:
            for fact in record.get('extracted_facts', []):
                if 'confidence' in fact:
                    confidence_values.append(fact['confidence'])
        
        conf_stats = {}
        if confidence_values:
            conf_stats = {
                'min': min(confidence_values),
                'max': max(confidence_values),
                'mean': np.mean(confidence_values),
                'std': np.std(confidence_values)
            }
        
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': 'week3-document-refinery-extractions',
            'info': {
                'title': 'Week 3 Document Refinery - Extraction Records',
                'version': '1.0.0',
                'owner': 'extraction-team',
                'description': 'One record per processed document with extracted facts and entities. Confidence MUST be float in [0.0, 1.0].',
                'maintainers': ['data-quality@example.com'],
                'tags': ['extraction', 'nlp', 'confidence']
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': 'outputs/week3/extractions.jsonl',
                    'format': 'jsonl'
                }
            },
            'terms': {
                'usage': 'Internal inter-system data contract',
                'limitations': 'confidence must remain in [0.0, 1.0] float range',
                'sla': {
                    'freshness': '24 hours',
                    'availability': '99.9%'
                }
            },
            'schema': {
                'doc_id': {
                    'type': 'string',
                    'format': 'uuid',
                    'required': True,
                    'unique': True,
                    'description': 'Primary key, UUIDv4',
                    'example': '123e4567-e89b-12d3-a456-426614174000'
                },
                'source_path': {
                    'type': 'string',
                    'required': True,
                    'minLength': 1,
                    'description': 'Absolute path or URL to source document'
                },
                'source_hash': {
                    'type': 'string',
                    'pattern': '^[a-f0-9]{64}$',
                    'required': True,
                    'description': 'SHA-256 of source file'
                },
                'extracted_facts': {
                    'type': 'array',
                    'minItems': 1,
                    'required': True,
                    'description': 'Array of facts extracted from the document',
                    'items': {
                        'type': 'object',
                        'required': ['fact_id', 'text', 'confidence'],
                        'properties': {
                            'fact_id': {
                                'type': 'string',
                                'format': 'uuid',
                                'description': 'Unique fact identifier'
                            },
                            'text': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 5000,
                                'description': 'Extracted fact text'
                            },
                            'entity_refs': {
                                'type': 'array',
                                'items': {'type': 'string', 'format': 'uuid'},
                                'description': 'References to entities'
                            },
                            'confidence': {
                                'type': 'number',
                                'minimum': 0.0,
                                'maximum': 1.0,
                                'required': True,
                                'description': 'Confidence score in [0.0, 1.0] - BREAKING if changed to 0-100'
                            },
                            'page_ref': {
                                'type': 'integer',
                                'minimum': 1,
                                'nullable': True,
                                'description': 'Page number where fact was found'
                            },
                            'source_excerpt': {
                                'type': 'string',
                                'maxLength': 1000,
                                'description': 'Verbatim source text'
                            }
                        }
                    }
                },
                'entities': {
                    'type': 'array',
                    'required': True,
                    'items': {
                        'type': 'object',
                        'required': ['entity_id', 'name', 'type'],
                        'properties': {
                            'entity_id': {'type': 'string', 'format': 'uuid'},
                            'name': {'type': 'string', 'minLength': 1},
                            'type': {
                                'type': 'string',
                                'enum': ['PERSON', 'ORG', 'LOCATION', 'DATE', 'AMOUNT', 'OTHER']
                            },
                            'canonical_value': {'type': 'string'}
                        }
                    }
                },
                'extraction_model': {
                    'type': 'string',
                    'required': True,
                    'pattern': '^(claude|gpt|llama)-[a-z0-9-]+$',
                    'description': 'Model identifier'
                },
                'processing_time_ms': {
                    'type': 'integer',
                    'minimum': 0,
                    'maximum': 300000,
                    'required': True
                },
                'token_count': {
                    'type': 'object',
                    'required': True,
                    'properties': {
                        'input': {'type': 'integer', 'minimum': 0},
                        'output': {'type': 'integer', 'minimum': 0}
                    }
                },
                'extracted_at': {
                    'type': 'string',
                    'format': 'date-time',
                    'required': True
                }
            },
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks for extractions': [
                        # Structural checks
                        {'missing_doc_id': {'condition': 'missing_count(doc_id) = 0', 'severity': 'CRITICAL'}},
                        {'duplicate_doc_id': {'condition': 'duplicate_count(doc_id) = 0', 'severity': 'CRITICAL'}},
                        {'missing_source_hash': {'condition': 'missing_count(source_hash) = 0', 'severity': 'HIGH'}},
                        
                        # Confidence-specific checks (CRITICAL)
                        {'confidence_range': {
                            'condition': 'confidence BETWEEN 0.0 AND 1.0',
                            'severity': 'CRITICAL',
                            'description': 'Confidence must be float in [0.0, 1.0]'
                        }},
                        {'confidence_type': {
                            'condition': "typeof(confidence) = 'float'",
                            'severity': 'CRITICAL',
                            'description': 'Confidence must be float, not integer'
                        }},
                        {'confidence_not_null': {'condition': 'missing_count(confidence) = 0', 'severity': 'CRITICAL'}},
                        
                        # Entity integrity
                        {'entity_reference_validity': {
                            'condition': 'every entity_refs item exists in entities[].entity_id',
                            'severity': 'HIGH'
                        }},
                        
                        # Value constraints
                        {'min_confidence_floor': {'condition': 'min(confidence) >= 0.0', 'severity': 'HIGH'}},
                        {'max_confidence_ceiling': {'condition': 'max(confidence) <= 1.0', 'severity': 'HIGH'}},
                        {'processing_time_positive': {'condition': 'processing_time_ms >= 0', 'severity': 'MEDIUM'}},
                        
                        # Statistical drift detection
                        {'confidence_statistical_drift': {
                            'warn_condition': f'abs(mean(confidence) - {conf_stats.get("mean", 0.85):.3f}) > 2*{conf_stats.get("std", 0.1):.3f}',
                            'fail_condition': f'abs(mean(confidence) - {conf_stats.get("mean", 0.85):.3f}) > 3*{conf_stats.get("std", 0.1):.3f}',
                            'severity': 'HIGH',
                            'description': 'Detects silent corruption like 0.0-1.0 → 0-100 scale change'
                        }},
                        
                        {'row_count_minimum': {'condition': 'row_count >= 50', 'severity': 'MEDIUM'}}
                    ]
                }
            },
            'lineage': {
                'upstream': [],
                'downstream': [
                    {'id': 'week4-cartographer', 'fields_consumed': ['doc_id', 'extracted_facts', 'confidence'], 'breaking_if_changed': ['confidence']},
                    {'id': 'week5-event-sourcing', 'fields_consumed': ['doc_id', 'extracted_facts'], 'breaking_if_changed': ['doc_id']},
                    {'id': 'week2-digital-courtroom', 'fields_consumed': ['confidence'], 'breaking_if_changed': ['confidence']}
                ]
            }
        }
        
        return contract
    
    def generate_week5_contract(self, records: List[Dict]) -> Dict:
        """Generate comprehensive Week 5 contract with 15+ clauses"""
        
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': 'week5-event-sourcing-platform',
            'info': {
                'title': 'Week 5 Event Sourcing Platform - Event Records',
                'version': '1.0.0',
                'owner': 'events-team',
                'description': 'Immutable event records for event sourcing architecture with full auditability',
                'maintainers': ['events-platform@example.com'],
                'tags': ['event-sourcing', 'audit', 'immutable']
            },
            'servers': {
                'local': {
                    'type': 'local',
                    'path': 'outputs/week5/events.jsonl',
                    'format': 'jsonl'
                }
            },
            'terms': {
                'usage': 'Internal event sourcing contract',
                'limitations': 'Events are immutable once recorded; sequence_number must be monotonic',
                'sla': {
                    'latency': '< 100ms',
                    'availability': '99.99%'
                }
            },
            'schema': {
                'event_id': {
                    'type': 'string',
                    'format': 'uuid',
                    'required': True,
                    'unique': True,
                    'description': 'Unique immutable event identifier'
                },
                'event_type': {
                    'type': 'string',
                    'pattern': '^[A-Z][a-zA-Z0-9]*$',
                    'required': True,
                    'enum': ['DocumentProcessed', 'ExtractionCompleted', 'ValidationFailed', 'EntityExtracted', 'ConfidenceAdjusted'],
                    'description': 'PascalCase event type, must be registered'
                },
                'aggregate_id': {
                    'type': 'string',
                    'format': 'uuid',
                    'required': True,
                    'description': 'Aggregate identifier'
                },
                'aggregate_type': {
                    'type': 'string',
                    'pattern': '^[A-Z][a-zA-Z0-9]*$',
                    'required': True,
                    'enum': ['Document', 'Extraction', 'Validation', 'Entity'],
                    'description': 'Type of aggregate'
                },
                'sequence_number': {
                    'type': 'integer',
                    'minimum': 1,
                    'required': True,
                    'description': 'Monotonically increasing per aggregate, no gaps'
                },
                'payload': {
                    'type': 'object',
                    'required': True,
                    'description': 'Event-type-specific payload',
                    'properties': {
                        'document_id': {'type': 'string', 'format': 'uuid'},
                        'status': {'type': 'string', 'enum': ['success', 'failed', 'pending']}
                    }
                },
                'metadata': {
                    'type': 'object',
                    'required': True,
                    'properties': {
                        'causation_id': {'type': 'string', 'format': 'uuid', 'nullable': True},
                        'correlation_id': {'type': 'string', 'format': 'uuid', 'required': True},
                        'user_id': {'type': 'string', 'required': True, 'minLength': 1},
                        'source_service': {
                            'type': 'string',
                            'required': True,
                            'enum': ['week3-document-refinery', 'week2-digital-courtroom', 'week4-cartographer']
                        }
                    }
                },
                'schema_version': {
                    'type': 'string',
                    'pattern': '^\\d+\\.\\d+$',
                    'required': True,
                    'description': 'Schema version (major.minor)'
                },
                'occurred_at': {
                    'type': 'string',
                    'format': 'date-time',
                    'required': True,
                    'description': 'Business timestamp'
                },
                'recorded_at': {
                    'type': 'string',
                    'format': 'date-time',
                    'required': True,
                    'description': 'System timestamp'
                }
            },
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks for events': [
                        # Identification checks
                        {'event_id_not_null': {'condition': 'missing_count(event_id) = 0', 'severity': 'CRITICAL'}},
                        {'event_id_unique': {'condition': 'duplicate_count(event_id) = 0', 'severity': 'CRITICAL'}},
                        
                        # Event type validation
                        {'event_type_valid': {
                            'condition': "event_type IN ('DocumentProcessed', 'ExtractionCompleted', 'ValidationFailed', 'EntityExtracted', 'ConfidenceAdjusted')",
                            'severity': 'CRITICAL'
                        }},
                        
                        # Temporal consistency (CRITICAL)
                        {'time_order': {
                            'condition': 'recorded_at >= occurred_at',
                            'severity': 'CRITICAL',
                            'description': 'recorded_at must be >= occurred_at'
                        }},
                        {'timestamp_format': {
                            'condition': "occurred_at LIKE '%Z' AND recorded_at LIKE '%Z'",
                            'severity': 'MEDIUM'
                        }},
                        
                        # Sequence number integrity
                        {'sequence_positive': {'condition': 'sequence_number >= 1', 'severity': 'CRITICAL'}},
                        {'sequence_order': {
                            'condition': 'sequence_number > previous_sequence_number',
                            'severity': 'HIGH',
                            'description': 'Sequence numbers must increase'
                        }},
                        {'sequence_no_gaps': {
                            'warn_condition': 'sequence_number != previous_sequence_number + 1',
                            'severity': 'MEDIUM',
                            'description': 'No gaps in sequence numbers'
                        }},
                        
                        # Aggregate integrity
                        {'aggregate_id_not_null': {'condition': 'missing_count(aggregate_id) = 0', 'severity': 'CRITICAL'}},
                        {'aggregate_type_valid': {
                            'condition': "aggregate_type IN ('Document', 'Extraction', 'Validation', 'Entity')",
                            'severity': 'HIGH'
                        }},
                        
                        # Metadata completeness
                        {'correlation_id_present': {'condition': 'missing_count(metadata.correlation_id) = 0', 'severity': 'HIGH'}},
                        {'source_service_valid': {
                            'condition': "metadata.source_service IN ('week3-document-refinery', 'week2-digital-courtroom', 'week4-cartographer')",
                            'severity': 'MEDIUM'
                        }},
                        
                        # Versioning
                        {'schema_version_format': {'condition': "schema_version LIKE '%.%'", 'severity': 'MEDIUM'}},
                        
                        # Statistical monitoring
                        {'processing_latency': {
                            'warn_condition': 'AVG(recorded_at - occurred_at) > 30 seconds',
                            'severity': 'MEDIUM'
                        }},
                        
                        {'row_count_minimum': {'condition': 'row_count >= 50', 'severity': 'MEDIUM'}}
                    ]
                }
            },
            'lineage': {
                'upstream': [
                    {'id': 'week3-document-refinery', 'description': 'Consumes extraction completion events'},
                    {'id': 'week4-cartographer', 'description': 'Consumes lineage graph updates'}
                ],
                'downstream': [
                    {'id': 'week7-data-contract-enforcer', 'fields_consumed': ['event_id', 'event_type', 'payload']},
                    {'id': 'week8-sentinel', 'fields_consumed': ['event_type', 'payload.status']}
                ]
            }
        }
        
        return contract
    
    def generate_dbt_tests(self, contract: Dict, week_type: str) -> Dict:
        """Generate dbt-compatible test file"""
        
        if week_type == 'week3':
            return {
                'version': 2,
                'models': [{
                    'name': 'week3_extractions',
                    'description': 'Document extraction records with confidence scores',
                    'tests': [
                        {'dbt_utils.unique_combination_of_columns': {'combination_of_columns': ['doc_id']}}
                    ],
                    'columns': [
                        {'name': 'doc_id', 'tests': ['not_null', 'unique', 'dbt_utils.uuid_type']},
                        {'name': 'source_path', 'tests': ['not_null', 'dbt_utils.not_empty_string']},
                        {'name': 'source_hash', 'tests': ['not_null', {'dbt_utils.accepted_range': {'min_value': 64, 'max_value': 64}}]},
                        {'name': 'confidence', 'tests': [
                            'not_null',
                            {'dbt_utils.accepted_range': {'min_value': 0.0, 'max_value': 1.0}},
                            {'dbt_utils.expression_is_true': {'expression': "confidence = CAST(confidence AS FLOAT)"}}
                        ]},
                        {'name': 'extraction_model', 'tests': ['not_null', {'accepted_values': {'values': ['claude-3-5-sonnet', 'gpt-4', 'llama-3']}}]},
                        {'name': 'processing_time_ms', 'tests': ['not_null', {'dbt_utils.accepted_range': {'min_value': 0, 'max_value': 300000}}]},
                        {'name': 'extracted_at', 'tests': ['not_null']}
                    ]
                }]
            }
        else:
            return {
                'version': 2,
                'models': [{
                    'name': 'week5_events',
                    'description': 'Event sourcing event records',
                    'tests': [
                        {'dbt_utils.unique_combination_of_columns': {'combination_of_columns': ['event_id']}}
                    ],
                    'columns': [
                        {'name': 'event_id', 'tests': ['not_null', 'unique', 'dbt_utils.uuid_type']},
                        {'name': 'event_type', 'tests': [
                            'not_null',
                            {'accepted_values': {'values': ['DocumentProcessed', 'ExtractionCompleted', 'ValidationFailed', 'EntityExtracted']}}
                        ]},
                        {'name': 'aggregate_id', 'tests': ['not_null', 'dbt_utils.uuid_type']},
                        {'name': 'aggregate_type', 'tests': ['not_null', {'accepted_values': {'values': ['Document', 'Extraction', 'Validation', 'Entity']}}]},
                        {'name': 'sequence_number', 'tests': ['not_null', {'dbt_utils.accepted_range': {'min_value': 1}}]},
                        {'name': 'schema_version', 'tests': ['not_null', {'dbt_utils.expression_is_true': {'expression': "schema_version ~ '^\\d+\\.\\d+$'"}}]},
                        {'name': 'occurred_at', 'tests': ['not_null']},
                        {'name': 'recorded_at', 'tests': [
                            'not_null',
                            {'dbt_utils.expression_is_true': {'expression': "recorded_at >= occurred_at", 'severity': 'error'}}
                        ]}
                    ]
                }]
            }
    
    def save_contract(self, contract: Dict, week_type: str):
        """Save contract and dbt files"""
        
        # Save main contract
        contract_file = self.output_dir / f"{week_type}_extractions.yaml" if week_type == 'week3' else self.output_dir / f"{week_type}_events.yaml"
        with open(contract_file, 'w') as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)
        print(f"✅ Contract saved: {contract_file}")
        
        # Save dbt tests
        dbt_tests = self.generate_dbt_tests(contract, week_type)
        dbt_file = self.output_dir / f"{week_type}_extractions_dbt.yml" if week_type == 'week3' else self.output_dir / f"{week_type}_events_dbt.yml"
        with open(dbt_file, 'w') as f:
            yaml.dump(dbt_tests, f, default_flow_style=False, sort_keys=False)
        print(f"✅ dbt tests saved: {dbt_file}")
        
        # Save JSON version for programmatic access
        json_file = self.output_dir / f"{week_type}_extractions.json" if week_type == 'week3' else self.output_dir / f"{week_type}_events.json"
        with open(json_file, 'w') as f:
            json.dump(contract, f, indent=2)
        print(f"✅ JSON contract saved: {json_file}")
    
    def run(self):
        """Generate all contracts"""
        print(f"\n{'='*60}")
        print(f"📝 Generating {self.week_type.upper()} Contract")
        print(f"{'='*60}")
        
        records, df = self.load_data()
        
        if self.week_type == 'week3':
            contract = self.generate_week3_contract(records)
            self.save_contract(contract, 'week3')
            
            # Count clauses
            checks = contract['quality']['specification']['checks for extractions']
            print(f"\n📊 Contract Statistics:")
            print(f"   Total quality checks: {len(checks)}")
            print(f"   Required fields: {len([k for k, v in contract['schema'].items() if v.get('required')])}")
            print(f"   Downstream consumers: {len(contract['lineage']['downstream'])}")
            
        else:
            contract = self.generate_week5_contract(records)
            self.save_contract(contract, 'week5')
            
            checks = contract['quality']['specification']['checks for events']
            print(f"\n📊 Contract Statistics:")
            print(f"   Total quality checks: {len(checks)}")
            print(f"   Required fields: {len([k for k, v in contract['schema'].items() if v.get('required')])}")
            print(f"   Event types: {contract['schema']['event_type']['enum']}")
        
        print(f"\n✨ Generation complete!")


def main():
    parser = argparse.ArgumentParser(description='Generate complete contracts with dbt tests')
    parser.add_argument('--source', required=True, help='Path to source JSONL file')
    parser.add_argument('--output', required=True, help='Output directory')
    
    args = parser.parse_args()
    
    generator = CompleteContractGenerator(args.source, args.output)
    generator.run()


if __name__ == '__main__':
    main()