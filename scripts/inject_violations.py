# scripts/inject_violations.py
#!/usr/bin/env python3
"""
Inject intentional violations for testing the contract enforcer
"""

import json
import uuid
import random
from datetime import datetime
from pathlib import Path


def inject_confidence_violation(extractions_path: Path):
    """Inject confidence scale violation (0-100 instead of 0.0-1.0)"""
    
    records = []
    with open(extractions_path, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    
    # Modify record 10 to have confidence in 0-100 scale
    if len(records) > 10:
        for fact in records[10].get('extracted_facts', []):
            fact['confidence'] = 95.0  # Intentional violation (should be 0.95)
        
        print("✅ Injected confidence violation in record 10")
    
    # Save back
    with open(extractions_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')


def inject_time_order_violation(events_path: Path):
    """Inject time order violation (recorded_at < occurred_at)"""
    
    records = []
    with open(events_path, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    
    # Modify record 15 to have recorded_at before occurred_at
    if len(records) > 15:
        occurred = records[15].get('occurred_at')
        if occurred:
            # Set recorded_at to 1 hour before occurred_at
            from datetime import datetime, timedelta
            occurred_dt = datetime.fromisoformat(occurred.replace('Z', '+00:00'))
            recorded_dt = occurred_dt - timedelta(hours=1)
            records[15]['recorded_at'] = recorded_dt.isoformat().replace('+00:00', 'Z')
            print("✅ Injected time order violation in record 15")
    
    # Save back
    with open(events_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')


def main():
    """Inject violations for testing"""
    print("="*60)
    print("💉 Injecting Intentional Violations")
    print("="*60)
    
    # Inject confidence violation
    extractions_path = Path('outputs/week3/extractions.jsonl')
    if extractions_path.exists():
        inject_confidence_violation(extractions_path)
    else:
        print("⚠️  Extractions file not found")
    
    # Inject time order violation
    events_path = Path('outputs/week5/events.jsonl')
    if events_path.exists():
        inject_time_order_violation(events_path)
    else:
        print("⚠️  Events file not found")
    
    print("\n✅ Violations injected successfully!")
    print("   Note: These are intentional violations for testing the enforcer.")


if __name__ == '__main__':
    main()