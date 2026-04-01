# contracts/runner.py
#!/usr/bin/env python3
"""
Enhanced ValidationRunner with comprehensive checks
"""

import argparse
import json
import uuid
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import numpy as np
import yaml


class CompleteValidationRunner:
    """Executes complete contract validation"""
    
    def __init__(self, contract_path: str, data_path: str, output_dir: str):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.contract = None
        self.records = []
        
    def load_contract(self):
        with open(self.contract_path, 'r') as f:
            self.contract = yaml.safe_load(f)
        print(f"✅ Loaded contract: {self.contract.get('id')}")
    
    def load_data(self):
        self.records = []
        with open(self.data_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        self.records.append(json.loads(line))
                    except:
                        pass
        
        print(f"✅ Loaded {len(self.records)} records")
        return len(self.records) > 0
    
    def extract_confidence_values(self) -> List[float]:
        """Extract all confidence values from nested structures"""
        values = []
        for record in self.records:
            if 'extracted_facts' in record:
                for fact in record['extracted_facts']:
                    if 'confidence' in fact and isinstance(fact['confidence'], (int, float)):
                        values.append(float(fact['confidence']))
        return values
    
    def check_confidence_range(self) -> Dict:
        """Check confidence values are within [0.0, 1.0]"""
        values = self.extract_confidence_values()
        
        if not values:
            return {'check_id': 'confidence.range', 'status': 'SKIP', 'message': 'No confidence values'}
        
        failing = [v for v in values if v < 0.0 or v > 1.0]
        return {
            'check_id': 'confidence.range',
            'status': 'FAIL' if failing else 'PASS',
            'records_failing': len(failing),
            'sample_failing': failing[:5],
            'actual_value': f'min={min(values):.2f}, max={max(values):.2f}',
            'expected': '0.0-1.0',
            'severity': 'CRITICAL',
            'message': f'{len(failing)} values outside range'
        }
    
    def check_confidence_type(self) -> Dict:
        """Check confidence values are floats (not integers)"""
        values = self.extract_confidence_values()
        
        if not values:
            return {'check_id': 'confidence.type', 'status': 'SKIP'}
        
        integers = [v for v in values if v == int(v) and v > 1]
        return {
            'check_id': 'confidence.type',
            'status': 'FAIL' if integers else 'PASS',
            'records_failing': len(integers),
            'sample_failing': integers[:5],
            'actual_value': f'{len(integers)} integer values',
            'expected': 'float',
            'severity': 'CRITICAL',
            'message': f'Found {len(integers)} integer confidence values (possibly 0-100 scale)'
        }
    
    def check_time_order(self) -> Dict:
        """Check recorded_at >= occurred_at"""
        violations = []
        for i, record in enumerate(self.records):
            occurred = record.get('occurred_at')
            recorded = record.get('recorded_at')
            if occurred and recorded:
                try:
                    occurred_dt = datetime.fromisoformat(occurred.replace('Z', '+00:00'))
                    recorded_dt = datetime.fromisoformat(recorded.replace('Z', '+00:00'))
                    if recorded_dt < occurred_dt:
                        violations.append(i)
                except:
                    pass
        
        return {
            'check_id': 'time_order',
            'status': 'PASS' if not violations else 'FAIL',
            'records_failing': len(violations),
            'message': f'{len(violations)} records with recorded_at < occurred_at'
        }
    
    def check_sequence_order(self) -> Dict:
        """Check sequence numbers are increasing per aggregate"""
        sequences = {}
        violations = []
        
        for i, record in enumerate(self.records):
            agg_id = record.get('aggregate_id')
            seq = record.get('sequence_number')
            if agg_id and seq:
                if agg_id in sequences:
                    if seq <= sequences[agg_id]:
                        violations.append(i)
                sequences[agg_id] = seq
        
        return {
            'check_id': 'sequence.order',
            'status': 'PASS' if not violations else 'FAIL',
            'records_failing': len(violations),
            'message': f'{len(violations)} sequence number violations'
        }
    
    def check_statistical_drift(self) -> Dict:
        """Detect statistical drift in confidence"""
        values = self.extract_confidence_values()
        
        if not values:
            return {'check_id': 'confidence.drift', 'status': 'SKIP'}
        
        current_mean = np.mean(values)
        baseline_path = Path('schema_snapshots/baseline.json')
        
        if baseline_path.exists():
            with open(baseline_path) as f:
                baseline = json.load(f)
            
            baseline_mean = baseline.get('confidence_mean', 0.85)
            baseline_std = baseline.get('confidence_std', 0.1)
            
            drift = abs(current_mean - baseline_mean) / baseline_std if baseline_std > 0 else 0
            
            if drift > 3:
                status = 'FAIL'
                severity = 'CRITICAL'
            elif drift > 2:
                status = 'WARN'
                severity = 'HIGH'
            else:
                status = 'PASS'
                severity = 'INFO'
            
            return {
                'check_id': 'confidence.drift',
                'status': status,
                'severity': severity,
                'actual_value': f'mean={current_mean:.3f}',
                'expected': f'mean={baseline_mean:.3f} ± 3σ',
                'message': f'Drift: {drift:.1f} sigma'
            }
        
        # Save baseline
        baseline_path.parent.mkdir(exist_ok=True)
        with open(baseline_path, 'w') as f:
            json.dump({'confidence_mean': current_mean, 'confidence_std': np.std(values)}, f)
        
        return {'check_id': 'confidence.drift', 'status': 'PASS', 'message': 'Baseline established'}
    
    def run(self) -> Dict:
        """Run all validation checks"""
        self.load_contract()
        if not self.load_data():
            return {'error': 'No data loaded'}
        
        results = []
        
        # Run checks based on contract type
        if 'confidence' in str(self.contract):
            results.append(self.check_confidence_range())
            results.append(self.check_confidence_type())
            results.append(self.check_statistical_drift())
        
        if 'occurred_at' in str(self.contract):
            results.append(self.check_time_order())
        
        if 'sequence_number' in str(self.contract):
            results.append(self.check_sequence_order())
        
        # Calculate summary
        total = len(results)
        passed = len([r for r in results if r.get('status') == 'PASS'])
        failed = len([r for r in results if r.get('status') == 'FAIL'])
        warned = len([r for r in results if r.get('status') == 'WARN'])
        
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract.get('id'),
            'run_timestamp': datetime.now().isoformat(),
            'total_checks': total,
            'passed': passed,
            'failed': failed,
            'warned': warned,
            'results': results
        }
        
        # Save report
        report_file = self.output_dir / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📊 Validation Complete:")
        print(f"   Total: {total} | ✅ Passed: {passed} | ❌ Failed: {failed} | ⚠️  Warnings: {warned}")
        print(f"   Report: {report_file}")
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', required=True)
    parser.add_argument('--data', required=True)
    parser.add_argument('--output', default='validation_reports')
    
    args = parser.parse_args()
    
    runner = CompleteValidationRunner(args.contract, args.data, args.output)
    runner.run()


if __name__ == '__main__':
    main()