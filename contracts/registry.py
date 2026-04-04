#!/usr/bin/env python3
"""
Complete Contract Registry - Central registry for all contracts
"""

import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from enum import Enum
from dataclasses import dataclass, asdict, field
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class EnforcementMode(Enum):
    MONITOR = "monitor"
    WARN = "warn"
    BLOCK = "block"
    ENFORCE = "enforce"
    AUDIT = "audit"
    
    def to_json(self):
        return self.value
    
    @classmethod
    def from_json(cls, value):
        return cls(value)


class ContractStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    BREAKING = "breaking"
    
    def to_json(self):
        return self.value
    
    @classmethod
    def from_json(cls, value):
        return cls(value)


class CompatibilityType(Enum):
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"
    
    def to_json(self):
        return self.value
    
    @classmethod
    def from_json(cls, value):
        return cls(value)


@dataclass
class ContractMetadata:
    contract_id: str
    name: str
    version: str
    owner: str
    status: str  # Store as string for JSON serialization
    registered_at: str
    last_validated: str
    enforcement_mode: str  # Store as string for JSON serialization
    consumers: List[str]
    dependencies: List[str]
    schema_hash: str
    compatibility: str  # Store as string for JSON serialization
    tags: List[str]
    description: str = ""
    slo: Dict = field(default_factory=lambda: {"max_latency_ms": 100, "availability": 99.9})


@dataclass
class ConsumerContract:
    consumer_id: str
    consumer_name: str
    contract_id: str
    fields_consumed: List[str]
    required_freshness: str
    sla_tolerance: float
    alert_channels: List[str]
    last_breach: Optional[str]
    breach_count: int
    criticality: str = "high"


class ContractRegistry:
    """Complete contract registry with full lifecycle management"""
    
    def __init__(self, registry_dir: str = "contract_registry"):
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        
        self.registry_file = self.registry_dir / "registry.json"
        self.consumers_file = self.registry_dir / "consumers.json"
        self.policies_file = self.registry_dir / "enforcement_policies.json"
        self.snapshots_dir = self.registry_dir / "snapshots"
        self.snapshots_dir.mkdir(exist_ok=True)
        
        self._load_all()
    
    def _load_all(self):
        """Load all registry data"""
        self.registry = self._load_json(self.registry_file, {'version': '2.0', 'contracts': {}})
        self.consumers = self._load_json(self.consumers_file, {'consumers': {}})
        self.policies = self._load_json(self.policies_file, {
            'default_mode': 'monitor',
            'global_thresholds': {'max_violation_rate': 0.05, 'max_critical_violations': 0, 'min_health_score': 70},
            'per_contract_overrides': {},
            'alert_routing': {'critical': ['pagerduty', 'slack'], 'high': ['slack'], 'medium': ['log'], 'low': []}
        })
    
    def _load_json(self, path: Path, default: Dict) -> Dict:
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return default
    
    def _save_json(self, path: Path, data: Dict):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=self._json_serializer)
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for enums and non-serializable objects"""
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        raise TypeError(f"Type {type(obj)} not serializable")
    
    def _save_all(self):
        self._save_json(self.registry_file, self.registry)
        self._save_json(self.consumers_file, self.consumers)
        self._save_json(self.policies_file, self.policies)
    
    def compute_schema_hash(self, schema: Dict) -> str:
        """Compute SHA256 hash of schema for version tracking"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]
    
    def register_contract(self, contract: Dict, enforcement_mode: EnforcementMode = None) -> str:
        """Register a contract with full metadata"""
        contract_id = contract.get('id')
        if not contract_id:
            raise ValueError("Contract must have an 'id' field")
        
        schema = contract.get('schema', {})
        schema_hash = self.compute_schema_hash(schema)
        
        # Get enforcement mode as string
        if enforcement_mode:
            mode_str = enforcement_mode.value
        else:
            mode_str = self.policies.get('default_mode', 'monitor')
        
        metadata = ContractMetadata(
            contract_id=contract_id,
            name=contract.get('info', {}).get('title', contract_id),
            version=contract.get('info', {}).get('version', '1.0.0'),
            owner=contract.get('info', {}).get('owner', 'unknown'),
            status=ContractStatus.ACTIVE.value,
            registered_at=datetime.now().isoformat(),
            last_validated=datetime.now().isoformat(),
            enforcement_mode=mode_str,
            consumers=self._extract_consumers(contract),
            dependencies=self._extract_dependencies(contract),
            schema_hash=schema_hash,
            compatibility=contract.get('compatibility', 'backward'),
            tags=contract.get('info', {}).get('tags', []),
            description=contract.get('info', {}).get('description', '')
        )
        
        # Store contract in registry
        self.registry['contracts'][contract_id] = {
            'metadata': asdict(metadata),
            'schema': schema,
            'quality_checks': contract.get('quality', {}),
            'lineage': contract.get('lineage', {}),
            'version_history': [{
                'version': metadata.version,
                'schema_hash': schema_hash,
                'registered_at': metadata.registered_at
            }]
        }
        
        # Save snapshot
        snapshot_path = self.snapshots_dir / f"{contract_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(snapshot_path, 'w', encoding='utf-8') as f:
            json.dump({
                'contract_id': contract_id,
                'version': metadata.version,
                'schema': schema,
                'timestamp': metadata.registered_at
            }, f, indent=2, default=self._json_serializer)
        
        self._save_all()
        print(f"✅ Registered contract: {contract_id} v{metadata.version}")
        return contract_id
    
    def _extract_consumers(self, contract: Dict) -> List[str]:
        downstream = contract.get('lineage', {}).get('downstream', [])
        return [c.get('id') for c in downstream if c.get('id')]
    
    def _extract_dependencies(self, contract: Dict) -> List[str]:
        upstream = contract.get('lineage', {}).get('upstream', [])
        return [u.get('id') for u in upstream if u.get('id')]
    
    def register_consumer(self, consumer_data: Dict) -> str:
        """Register a data consumer"""
        consumer_id = consumer_data.get('consumer_id')
        if not consumer_id:
            consumer_id = f"consumer_{len(self.consumers['consumers']) + 1}"
        
        consumer = ConsumerContract(
            consumer_id=consumer_id,
            consumer_name=consumer_data.get('name', consumer_id),
            contract_id=consumer_data.get('contract_id'),
            fields_consumed=consumer_data.get('fields_consumed', []),
            required_freshness=consumer_data.get('required_freshness', '24h'),
            sla_tolerance=consumer_data.get('sla_tolerance', 0.95),
            alert_channels=consumer_data.get('alert_channels', ['slack']),
            last_breach=None,
            breach_count=0,
            criticality=consumer_data.get('criticality', 'high')
        )
        
        self.consumers['consumers'][consumer_id] = asdict(consumer)
        self._save_all()
        print(f"✅ Registered consumer: {consumer.consumer_name}")
        return consumer_id
    
    def get_contract(self, contract_id: str) -> Optional[Dict]:
        return self.registry['contracts'].get(contract_id)
    
    def get_consumer(self, consumer_id: str) -> Optional[Dict]:
        return self.consumers['consumers'].get(consumer_id)
    
    def get_enforcement_mode(self, contract_id: str) -> str:
        """Get enforcement mode as string"""
        if contract_id in self.policies.get('per_contract_overrides', {}):
            return self.policies['per_contract_overrides'][contract_id].get('mode', 'monitor')
        
        contract = self.get_contract(contract_id)
        if contract:
            return contract['metadata'].get('enforcement_mode', 'monitor')
        
        return self.policies.get('default_mode', 'monitor')
    
    def update_enforcement_mode(self, contract_id: str, mode: str):
        """Update enforcement mode for a contract"""
        self.policies['per_contract_overrides'][contract_id] = {'mode': mode}
        self._save_all()
        print(f"✅ Enforcement mode for {contract_id} set to {mode}")
    
    def record_violation(self, contract_id: str, violation: Dict):
        """Record a violation and update consumer breach counts"""
        affected_consumers = self.get_affected_consumers(contract_id, violation.get('affected_fields', []))
        
        for consumer in affected_consumers:
            consumer_id = consumer['consumer_id']
            if consumer_id in self.consumers['consumers']:
                self.consumers['consumers'][consumer_id]['last_breach'] = datetime.now().isoformat()
                self.consumers['consumers'][consumer_id]['breach_count'] += 1
        
        self._save_all()
    
    def get_affected_consumers(self, contract_id: str, affected_fields: List[str]) -> List[Dict]:
        """Get consumers affected by a violation"""
        affected = []
        for consumer_id, consumer in self.consumers['consumers'].items():
            if consumer['contract_id'] == contract_id:
                if any(field in consumer['fields_consumed'] for field in affected_fields):
                    affected.append(consumer)
        return affected
    
    def list_contracts(self) -> List[Dict]:
        return [
            {
                'contract_id': cid,
                'name': data['metadata']['name'],
                'version': data['metadata']['version'],
                'status': data['metadata']['status'],
                'enforcement_mode': data['metadata']['enforcement_mode']
            }
            for cid, data in self.registry['contracts'].items()
        ]
    
    def generate_registry_report(self) -> Dict:
        """Generate complete registry report"""
        return {
            'generated_at': datetime.now().isoformat(),
            'total_contracts': len(self.registry['contracts']),
            'total_consumers': len(self.consumers['consumers']),
            'contracts': self.list_contracts(),
            'consumers': list(self.consumers['consumers'].values()),
            'enforcement_policies': self.policies
        }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', action='store_true', help='List all contracts')
    parser.add_argument('--report', action='store_true', help='Generate registry report')
    args = parser.parse_args()
    
    registry = ContractRegistry()
    if args.list:
        for c in registry.list_contracts():
            print(f"{c['contract_id']}: {c['name']} v{c['version']} [{c['enforcement_mode']}]")
    if args.report:
        report = registry.generate_registry_report()
        with open('contract_registry/registry_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        print("✅ Registry report saved")