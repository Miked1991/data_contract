# contracts/generator.py (UPDATED)
#!/usr/bin/env python3
"""
Contract Generator with Registry Integration
"""

import argparse
import json
import yaml
from pathlib import Path
from typing import Dict, List
from registry import ContractRegistry, EnforcementMode


class ContractGeneratorWithRegistry:
    """Generates contracts and registers them in the registry"""
    
    def __init__(self, source_path: str, output_dir: str, register: bool = True):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.register = register
        self.registry = ContractRegistry() if register else None
    
    def generate_contract(self) -> Dict:
        """Generate contract from data"""
        
        # Sample contract generation (simplified for demo)
        contract = {
            'kind': 'DataContract',
            'apiVersion': 'v3.0.0',
            'id': f"{self.source_path.stem}-contract",
            'info': {
                'title': f'Contract for {self.source_path.stem}',
                'version': '1.0.0',
                'owner': 'data-team',
                'description': 'Auto-generated contract with registry integration',
                'tags': ['auto-generated', 'production']
            },
            'compatibility': 'backward',
            'schema': {
                'confidence': {
                    'type': 'number',
                    'minimum': 0.0,
                    'maximum': 1.0,
                    'required': True
                }
            },
            'quality': {
                'type': 'SodaChecks',
                'specification': {
                    'checks': [
                        {
                            'confidence_range': {
                                'condition': 'confidence BETWEEN 0.0 AND 1.0',
                                'severity': 'CRITICAL'
                            }
                        }
                    ]
                }
            },
            'lineage': {
                'downstream': [
                    {'id': 'week4-cartographer', 'fields_consumed': ['confidence']},
                    {'id': 'week5-event-sourcing', 'fields_consumed': ['doc_id']}
                ]
            }
        }
        
        # Save contract file
        output_path = self.output_dir / f"{self.source_path.stem}_contract.yaml"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            yaml.dump(contract, f, default_flow_style=False)
        
        print(f"✅ Contract saved to {output_path}")
        
        # Register in registry
        if self.register and self.registry:
            contract_id = self.registry.register_contract(
                contract, 
                enforcement_mode=EnforcementMode.MONITOR
            )
            print(f"✅ Contract registered with ID: {contract_id}")
        
        return contract


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--no-register', action='store_true', help='Skip registry registration')
    parser.add_argument('--mode', default='monitor', choices=['monitor', 'warn', 'block', 'enforce', 'audit'])
    
    args = parser.parse_args()
    
    generator = ContractGeneratorWithRegistry(
        args.source, 
        args.output, 
        register=not args.no_register
    )
    generator.generate_contract()


if __name__ == '__main__':
    main()