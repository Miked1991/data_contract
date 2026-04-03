# contracts/schema_analyzer.py
#!/usr/bin/env python3
"""
SchemaEvolutionAnalyzer - Detects and classifies schema changes between snapshots
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import yaml


class SchemaEvolutionAnalyzer:
    """Analyzes schema changes between snapshots"""
    
    def __init__(self, contract_id: str, snapshot_dir: str = "schema_snapshots"):
        self.contract_id = contract_id
        self.snapshot_dir = Path(snapshot_dir)
    
    def load_snapshot(self, snapshot_path: Path) -> Dict:
        """Load a schema snapshot"""
        with open(snapshot_path, 'r') as f:
            if snapshot_path.suffix == '.json':
                return json.load(f)
            else:
                return yaml.safe_load(f)
    
    def find_snapshots(self) -> List[Path]:
        """Find all snapshots for this contract"""
        pattern = f"{self.contract_id}_*.json"
        snapshots = sorted(self.snapshot_dir.glob(pattern))
        return snapshots
    
    def extract_schema(self, snapshot: Dict) -> Dict:
        """Extract schema from snapshot"""
        if 'schema' in snapshot:
            return snapshot['schema']
        elif 'fields' in snapshot:
            return snapshot['fields']
        return snapshot
    
    def compare_schemas(self, old_schema: Dict, new_schema: Dict) -> Dict:
        """Compare two schemas and classify changes"""
        changes = []
        breaking_changes = []
        
        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())
        
        # Detect removed fields
        removed = old_fields - new_fields
        for field in removed:
            change = {
                'type': 'REMOVED',
                'field': field,
                'old_type': old_schema[field].get('type', 'unknown'),
                'breaking': True,
                'reason': f"Field '{field}' removed - consumers expecting it will fail"
            }
            changes.append(change)
            breaking_changes.append(change)
        
        # Detect added fields
        added = new_fields - old_fields
        for field in added:
            nullable = new_schema[field].get('nullable', False) or not new_schema[field].get('required', False)
            change = {
                'type': 'ADDED',
                'field': field,
                'new_type': new_schema[field].get('type', 'unknown'),
                'nullable': nullable,
                'breaking': not nullable,
                'reason': f"Non-nullable field '{field}' added - consumers must provide it"
            }
            changes.append(change)
            if not nullable:
                breaking_changes.append(change)
        
        # Detect type changes
        common = old_fields & new_fields
        for field in common:
            old_type = old_schema[field].get('type')
            new_type = new_schema[field].get('type')
            
            if old_type != new_type:
                is_breaking = self.is_breaking_type_change(old_type, new_type)
                change = {
                    'type': 'TYPE_CHANGE',
                    'field': field,
                    'old_type': old_type,
                    'new_type': new_type,
                    'breaking': is_breaking,
                    'reason': f"Type changed from {old_type} to {new_type}"
                }
                changes.append(change)
                if is_breaking:
                    breaking_changes.append(change)
            
            # Check range changes for numeric fields
            if 'minimum' in old_schema[field] or 'maximum' in old_schema[field]:
                old_min = old_schema[field].get('minimum')
                old_max = old_schema[field].get('maximum')
                new_min = new_schema[field].get('minimum')
                new_max = new_schema[field].get('maximum')
                
                if old_min != new_min or old_max != new_max:
                    is_narrowing = (new_min is not None and new_min > (old_min or 0)) or \
                                   (new_max is not None and new_max < (old_max or 1))
                    change = {
                        'type': 'RANGE_CHANGE',
                        'field': field,
                        'old_range': f"[{old_min}, {old_max}]" if old_min or old_max else "unbounded",
                        'new_range': f"[{new_min}, {new_max}]" if new_min or new_max else "unbounded",
                        'breaking': is_narrowing,
                        'reason': f"Range narrowed from {old_min}-{old_max} to {new_min}-{new_max}"
                    }
                    changes.append(change)
                    if is_narrowing:
                        breaking_changes.append(change)
            
            # Check enum changes
            if 'enum' in old_schema[field] or 'enum' in new_schema[field]:
                old_enum = set(old_schema[field].get('enum', []))
                new_enum = set(new_schema[field].get('enum', []))
                
                removed_values = old_enum - new_enum
                if removed_values:
                    change = {
                        'type': 'ENUM_REMOVAL',
                        'field': field,
                        'removed_values': list(removed_values),
                        'breaking': True,
                        'reason': f"Enum values {removed_values} removed"
                    }
                    changes.append(change)
                    breaking_changes.append(change)
        
        return {
            'changes': changes,
            'breaking_changes': breaking_changes,
            'compatibility_verdict': 'BREAKING' if breaking_changes else 'COMPATIBLE'
        }
    
    def is_breaking_type_change(self, old_type: str, new_type: str) -> bool:
        """Determine if a type change is breaking"""
        # Widening conversions are safe
        widening = {
            ('integer', 'number'): True,
            ('integer', 'float'): True,
            ('float', 'number'): True,
            ('string', 'any'): True
        }
        
        if (old_type, new_type) in widening:
            return False
        
        # Narrowing conversions are breaking
        narrowing = {
            ('number', 'integer'): True,
            ('float', 'integer'): True,
            ('any', 'string'): True,
            ('string', 'integer'): True
        }
        
        if (old_type, new_type) in narrowing:
            return True
        
        # Different types are breaking
        return old_type != new_type
    
    def generate_migration_report(self, comparison: Dict, snapshot_names: Tuple[str, str]) -> Dict:
        """Generate migration impact report"""
        return {
            'snapshot_from': snapshot_names[0],
            'snapshot_to': snapshot_names[1],
            'analyzed_at': datetime.now().isoformat(),
            'compatibility_verdict': comparison['compatibility_verdict'],
            'total_changes': len(comparison['changes']),
            'breaking_changes': len(comparison['breaking_changes']),
            'changes': comparison['changes'],
            'migration_checklist': self.generate_checklist(comparison['breaking_changes']),
            'rollback_plan': {
                'steps': [
                    "1. Revert to previous schema version",
                    "2. Restore from snapshot before change",
                    "3. Validate downstream consumers",
                    "4. Replay affected events"
                ],
                'estimated_downtime': '30 minutes'
            }
        }
    
    def generate_checklist(self, breaking_changes: List[Dict]) -> List[str]:
        """Generate migration checklist from breaking changes"""
        checklist = []
        for change in breaking_changes:
            if change['type'] == 'REMOVED':
                checklist.append(f"⚠️ Update all consumers to stop using field '{change['field']}'")
            elif change['type'] == 'ADDED' and not change.get('nullable', True):
                checklist.append(f"⚠️ Ensure all producers provide non-nullable field '{change['field']}'")
            elif change['type'] == 'TYPE_CHANGE':
                checklist.append(f"⚠️ Update consumer code to handle {change['new_type']} instead of {change['old_type']}")
            elif change['type'] == 'RANGE_CHANGE':
                checklist.append(f"⚠️ Validate all data falls within new range {change['new_range']}")
            elif change['type'] == 'ENUM_REMOVAL':
                checklist.append(f"⚠️ Remove references to {change['removed_values']} from consumer code")
        
        return checklist
    
    def run(self, snapshot1: Path = None, snapshot2: Path = None) -> Dict:
        """Run schema evolution analysis"""
        snapshots = self.find_snapshots()
        
        if len(snapshots) < 2:
            print(f"❌ Need at least 2 snapshots, found {len(snapshots)}")
            return None
        
        if snapshot1 and snapshot2:
            snap1 = snapshot1
            snap2 = snapshot2
        else:
            snap1 = snapshots[-2]  # Second newest
            snap2 = snapshots[-1]  # Newest
        
        print(f"\n📊 Analyzing schema evolution:")
        print(f"   From: {snap1.name}")
        print(f"   To:   {snap2.name}")
        
        old_snapshot = self.load_snapshot(snap1)
        new_snapshot = self.load_snapshot(snap2)
        
        old_schema = self.extract_schema(old_snapshot)
        new_schema = self.extract_schema(new_snapshot)
        
        comparison = self.compare_schemas(old_schema, new_schema)
        report = self.generate_migration_report(comparison, (snap1.name, snap2.name))
        
        print(f"\n📋 Analysis Results:")
        print(f"   Verdict: {report['compatibility_verdict']}")
        print(f"   Total changes: {report['total_changes']}")
        print(f"   Breaking changes: {report['breaking_changes']}")
        
        for change in comparison['changes']:
            status = "🔴 BREAKING" if change['breaking'] else "🟢 Compatible"
            print(f"   {status}: {change['type']} - {change.get('field', change.get('reason', ''))}")
        
        # Save report
        output_path = Path(f"validation_reports/schema_evolution_{self.contract_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n💾 Report saved to {output_path}")
        
        return report


def main():
    parser = argparse.ArgumentParser(description='Analyze schema evolution')
    parser.add_argument('--contract-id', required=True, help='Contract ID to analyze')
    parser.add_argument('--snapshot-dir', default='schema_snapshots', help='Snapshot directory')
    parser.add_argument('--snapshot1', help='First snapshot (older)')
    parser.add_argument('--snapshot2', help='Second snapshot (newer)')
    
    args = parser.parse_args()
    
    analyzer = SchemaEvolutionAnalyzer(args.contract_id, args.snapshot_dir)
    
    snapshot1 = Path(args.snapshot1) if args.snapshot1 else None
    snapshot2 = Path(args.snapshot2) if args.snapshot2 else None
    
    analyzer.run(snapshot1, snapshot2)


if __name__ == '__main__':
    main()