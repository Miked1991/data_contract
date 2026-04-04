# contracts/registry.py
#!/usr/bin/env python3
"""
Contract Registry - Central registry for all data contracts
Supports versioning, consumer tracking, and enforcement modes
"""

import json
import yaml
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from enum import Enum
from dataclasses import dataclass, asdict
import hashlib


class EnforcementMode(Enum):
    """Enforcement modes for contract validation"""
    MONITOR = "monitor"      # Log violations only, no blocking
    WARN = "warn"            # Warn but allow, send alerts
    BLOCK = "block"          # Block deployment on violation
    ENFORCE = "enforce"      # Enforce and quarantine bad data
    AUDIT = "audit"          # Full audit mode with detailed reporting


class ContractStatus(Enum):
    """Status of a registered contract"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    BREAKING = "breaking"


@dataclass
class ContractMetadata:
    """Metadata for a registered contract"""
    contract_id: str
    name: str
    version: str
    owner: str
    status: ContractStatus
    registered_at: str
    last_validated: str
    enforcement_mode: EnforcementMode
    consumers: List[str]
    dependencies: List[str]
    schema_hash: str
    compatibility: str  # backward, forward, full
    tags: List[str]


@dataclass
class ConsumerContract:
    """Contract for a specific consumer"""
    consumer_id: str
    consumer_name: str
    contract_id: str
    fields_consumed: List[str]
    required_freshness: str  # e.g., "24h"
    sla_tolerance: float  # e.g., 0.95 for 95% compliance
    alert_channels: List[str]
    last_breach: Optional[str]
    breach_count: int


class ContractRegistry:
    """Central registry for managing all data contracts"""
    
    def __init__(self, registry_dir: str = "contract_registry"):
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        
        self.registry_file = self.registry_dir / "registry.json"
        self.consumers_file = self.registry_dir / "consumers.json"
        self.policies_file = self.registry_dir / "enforcement_policies.json"
        
        self.registry = self._load_registry()
        self.consumers = self._load_consumers()
        self.policies = self._load_policies()
    
    def _load_registry(self) -> Dict:
        """Load contract registry"""
        if self.registry_file.exists():
            with open(self.registry_file, 'r') as f:
                return json.load(f)
        return {'contracts': {}, 'version': '2.0', 'last_updated': None}
    
    def _load_consumers(self) -> Dict:
        """Load consumer registry"""
        if self.consumers_file.exists():
            with open(self.consumers_file, 'r') as f:
                return json.load(f)
        return {'consumers': {}}
    
    def _load_policies(self) -> Dict:
        """Load enforcement policies"""
        if self.policies_file.exists():
            with open(self.policies_file, 'r') as f:
                return json.load(f)
        return {
            'default_mode': 'monitor',
            'global_thresholds': {
                'max_violation_rate': 0.05,
                'max_critical_violations': 0,
                'min_health_score': 70
            },
            'per_contract_overrides': {}
        }
    
    def _save_registry(self):
        """Save contract registry"""
        self.registry['last_updated'] = datetime.now().isoformat()
        with open(self.registry_file, 'w') as f:
            json.dump(self.registry, f, indent=2)
    
    def _save_consumers(self):
        """Save consumer registry"""
        with open(self.consumers_file, 'w') as f:
            json.dump(self.consumers, f, indent=2)
    
    def register_contract(self, contract: Dict, enforcement_mode: EnforcementMode = EnforcementMode.MONITOR) -> str:
        """Register a new contract in the registry"""
        contract_id = contract.get('id', str(uuid.uuid4()))
        
        # Calculate schema hash
        schema_str = json.dumps(contract.get('schema', {}), sort_keys=True)
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
        
        metadata = ContractMetadata(
            contract_id=contract_id,
            name=contract.get('info', {}).get('title', contract_id),
            version=contract.get('info', {}).get('version', '1.0.0'),
            owner=contract.get('info', {}).get('owner', 'unknown'),
            status=ContractStatus.ACTIVE,
            registered_at=datetime.now().isoformat(),
            last_validated=datetime.now().isoformat(),
            enforcement_mode=enforcement_mode,
            consumers=self._extract_consumers(contract),
            dependencies=self._extract_dependencies(contract),
            schema_hash=schema_hash,
            compatibility=contract.get('compatibility', 'backward'),
            tags=contract.get('info', {}).get('tags', [])
        )
        
        self.registry['contracts'][contract_id] = {
            'metadata': asdict(metadata),
            'schema': contract.get('schema', {}),
            'quality_checks': contract.get('quality', {}),
            'lineage': contract.get('lineage', {})
        }
        
        self._save_registry()
        print(f"✅ Registered contract: {contract_id} (v{metadata.version})")
        return contract_id
    
    def _extract_consumers(self, contract: Dict) -> List[str]:
        """Extract downstream consumers from contract"""
        downstream = contract.get('lineage', {}).get('downstream', [])
        return [c.get('id') for c in downstream if c.get('id')]
    
    def _extract_dependencies(self, contract: Dict) -> List[str]:
        """Extract upstream dependencies from contract"""
        upstream = contract.get('lineage', {}).get('upstream', [])
        return [u.get('id') for u in upstream if u.get('id')]
    
    def register_consumer(self, consumer: Dict) -> str:
        """Register a data consumer with their contract requirements"""
        consumer_id = consumer.get('consumer_id', str(uuid.uuid4()))
        
        consumer_contract = ConsumerContract(
            consumer_id=consumer_id,
            consumer_name=consumer.get('name', 'unknown'),
            contract_id=consumer.get('contract_id'),
            fields_consumed=consumer.get('fields_consumed', []),
            required_freshness=consumer.get('required_freshness', '24h'),
            sla_tolerance=consumer.get('sla_tolerance', 0.95),
            alert_channels=consumer.get('alert_channels', ['slack', 'email']),
            last_breach=None,
            breach_count=0
        )
        
        self.consumers['consumers'][consumer_id] = asdict(consumer_contract)
        self._save_consumers()
        
        print(f"✅ Registered consumer: {consumer_contract.consumer_name}")
        return consumer_id
    
    def get_contract(self, contract_id: str) -> Optional[Dict]:
        """Retrieve a contract from the registry"""
        return self.registry['contracts'].get(contract_id)
    
    def get_enforcement_mode(self, contract_id: str) -> EnforcementMode:
        """Get enforcement mode for a contract"""
        contract = self.get_contract(contract_id)
        if contract:
            mode_str = contract['metadata'].get('enforcement_mode', 'monitor')
            return EnforcementMode(mode_str)
        
        # Check for override
        if contract_id in self.policies.get('per_contract_overrides', {}):
            mode_str = self.policies['per_contract_overrides'][contract_id].get('mode', 'monitor')
            return EnforcementMode(mode_str)
        
        return EnforcementMode(self.policies.get('default_mode', 'monitor'))
    
    def update_contract_status(self, contract_id: str, status: ContractStatus):
        """Update contract status"""
        if contract_id in self.registry['contracts']:
            self.registry['contracts'][contract_id]['metadata']['status'] = status.value
            self._save_registry()
            print(f"✅ Contract {contract_id} status updated to {status.value}")
    
    def record_breach(self, consumer_id: str, violation: Dict):
        """Record a contract breach for a consumer"""
        if consumer_id in self.consumers['consumers']:
            self.consumers['consumers'][consumer_id]['last_breach'] = datetime.now().isoformat()
            self.consumers['consumers'][consumer_id]['breach_count'] += 1
            self._save_consumers()
            
            # Check if SLA breached
            consumer = self.consumers['consumers'][consumer_id]
            breach_rate = consumer['breach_count'] / (consumer.get('total_validations', 1))
            
            if breach_rate > (1 - consumer['sla_tolerance']):
                self._trigger_alert(consumer, violation)
    
    def _trigger_alert(self, consumer: Dict, violation: Dict):
        """Trigger alerts for SLA breach"""
        print(f"\n⚠️  SLA BREACH for consumer: {consumer['consumer_name']}")
        print(f"   Breach rate: {consumer['breach_count']} violations")
        print(f"   Channels: {consumer['alert_channels']}")
        print(f"   Violation: {violation.get('message', 'unknown')}")
    
    def get_consumer_impact_analysis(self, contract_id: str, violation: Dict) -> Dict:
        """Analyze impact of a violation on registered consumers"""
        contract = self.get_contract(contract_id)
        if not contract:
            return {'error': 'Contract not found'}
        
        affected_consumers = []
        for consumer_id, consumer in self.consumers['consumers'].items():
            if consumer['contract_id'] == contract_id:
                # Check if violation affects this consumer's consumed fields
                affected_fields = set(consumer['fields_consumed']) & set(violation.get('affected_fields', []))
                if affected_fields:
                    affected_consumers.append({
                        'consumer_id': consumer_id,
                        'consumer_name': consumer['consumer_name'],
                        'affected_fields': list(affected_fields),
                        'sla_tolerance': consumer['sla_tolerance'],
                        'breach_count': consumer['breach_count']
                    })
        
        return {
            'contract_id': contract_id,
            'violation': violation,
            'affected_consumers': affected_consumers,
            'total_impact_score': len(affected_consumers) * violation.get('severity_score', 1),
            'requires_rollback': self.get_enforcement_mode(contract_id) == EnforcementMode.BLOCK
        }
    
    def list_contracts(self) -> List[Dict]:
        """List all registered contracts"""
        return [
            {
                'contract_id': cid,
                'name': data['metadata']['name'],
                'version': data['metadata']['version'],
                'status': data['metadata']['status'],
                'consumers': len(data['metadata']['consumers'])
            }
            for cid, data in self.registry['contracts'].items()
        ]
    
    def generate_registry_report(self) -> Dict:
        """Generate a report of all registered contracts and consumers"""
        return {
            'generated_at': datetime.now().isoformat(),
            'total_contracts': len(self.registry['contracts']),
            'total_consumers': len(self.consumers['consumers']),
            'contracts': self.list_contracts(),
            'consumers': list(self.consumers['consumers'].values()),
            'enforcement_policies': self.policies
        }


# CLI for registry management
def main():
    import argparse
    parser = argparse.ArgumentParser(description='Contract Registry Management')
    parser.add_argument('--register-contract', help='Register a contract from YAML file')
    parser.add_argument('--register-consumer', help='Register a consumer from JSON file')
    parser.add_argument('--list', action='store_true', help='List all contracts')
    parser.add_argument('--report', action='store_true', help='Generate registry report')
    parser.add_argument('--set-mode', nargs=2, metavar=('CONTRACT_ID', 'MODE'), 
                        help='Set enforcement mode for contract')
    
    args = parser.parse_args()
    
    registry = ContractRegistry()
    
    if args.register_contract:
        with open(args.register_contract, 'r') as f:
            contract = yaml.safe_load(f)
        registry.register_contract(contract)
    
    if args.register_consumer:
        with open(args.register_consumer, 'r') as f:
            consumer = json.load(f)
        registry.register_consumer(consumer)
    
    if args.list:
        print("\n📋 Registered Contracts:")
        for c in registry.list_contracts():
            print(f"   {c['contract_id']}: {c['name']} (v{c['version']}) - {c['status']} - {c['consumers']} consumers")
    
    if args.report:
        report = registry.generate_registry_report()
        report_path = Path('contract_registry/registry_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"✅ Registry report saved to {report_path}")
    
    if args.set_mode:
        contract_id, mode = args.set_mode
        registry.policies['per_contract_overrides'][contract_id] = {'mode': mode}
        with open(registry.policies_file, 'w') as f:
            json.dump(registry.policies, f, indent=2)
        print(f"✅ Enforcement mode for {contract_id} set to {mode}")


if __name__ == '__main__':
    main()