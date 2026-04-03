# scripts/create_violation_log.py
#!/usr/bin/env python3
"""
Create violation log with real and injected violations
"""

import json
import uuid
from datetime import datetime
from pathlib import Path


def create_violation_log():
    """Create violations.jsonl with real and injected violations"""
    
    violations = []
    
    # Real violation detected from validation (confidence scale issue)
    violations.append({
        "violation_id": str(uuid.uuid4()),
        "check_id": "confidence.range",
        "detected_at": "2026-04-01T17:49:36",
        "severity": "CRITICAL",
        "message": "8 confidence values outside [0.0, 1.0] range (values up to 92.0)",
        "records_failing": 8,
        "sample_failing": [92.0, 92.0, 92.0, 73.0, 73.0],
        "source": "REAL - Confidence scale mismatch in extraction pipeline",
        "injection_note": None
    })
    
    # Second real violation from validation (type mismatch)
    violations.append({
        "violation_id": str(uuid.uuid4()),
        "check_id": "confidence.type",
        "detected_at": "2026-04-01T17:49:36",
        "severity": "CRITICAL",
        "message": "8 integer confidence values found (expected float)",
        "records_failing": 8,
        "sample_failing": [92.0, 92.0, 73.0],
        "source": "REAL - Confidence stored as integer instead of float",
        "injection_note": None
    })
    
    # Injected violation - time order
    violations.append({
        "violation_id": str(uuid.uuid4()),
        "check_id": "time_order",
        "detected_at": "2026-04-01T18:00:00",
        "severity": "CRITICAL",
        "message": "recorded_at < occurred_at for 2 records",
        "records_failing": 2,
        "sample_failing": ["Event 15: recorded_at before occurred_at"],
        "source": "INJECTED - Intentional time order violation for testing",
        "injection_note": "Injected via scripts/inject_violations.py on 2026-04-01 for testing attribution"
    })
    
    # Injected violation - sequence number gap
    violations.append({
        "violation_id": str(uuid.uuid4()),
        "check_id": "sequence.order",
        "detected_at": "2026-04-01T18:00:00",
        "severity": "HIGH",
        "message": "Sequence number gap detected in aggregate Document:abc123",
        "records_failing": 1,
        "sample_failing": ["Sequence jumped from 5 to 7, missing 6"],
        "source": "INJECTED - Intentional sequence gap for testing",
        "injection_note": "Injected via scripts/inject_violations.py on 2026-04-01 to test schema evolution"
    })
    
    # Save to violation log
    violation_log = Path('violation_log/violations.jsonl')
    violation_log.parent.mkdir(exist_ok=True)
    
    with open(violation_log, 'w') as f:
        for violation in violations:
            f.write(json.dumps(violation) + '\n')
    
    print(f"✅ Created violation log with {len(violations)} violations")
    print("   - 2 real violations (confidence scale/type)")
    print("   - 2 injected violations (time order, sequence gap)")
    
    return violation_log


if __name__ == '__main__':
    create_violation_log()