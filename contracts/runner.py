# contracts/runner_final.py
#!/usr/bin/env python3
"""
Final ValidationRunner with robust data handling
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


class FinalValidationRunner:
    """Executes contract validations with robust data handling"""
    
    def __init__(self, contract_path: str, data_path: str, output_dir: str):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.contract = None
        self.records = []
        self.df = None
        
    def load_contract(self):
        """Load contract YAML"""
        with open(self.contract_path, 'r', encoding='utf-8') as f:
            self.contract = yaml.safe_load(f)
        print(f"✅ Loaded contract: {self.contract.get('id', 'unknown')}")
        return self.contract
    
    def load_data(self):
        """Load data from JSONL safely"""
        self.records = []
        
        print(f"\n📖 Loading data from: {self.data_path}")
        
        if not self.data_path.exists():
            print(f"   ❌ File not found: {self.data_path}")
            return False
        
        with open(self.data_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        # Only add dictionary records
                        if isinstance(record, dict):
                            self.records.append(record)
                        else:
                            print(f"   ⚠️  Line {line_num}: Record is {type(record)}, skipping")
                    except json.JSONDecodeError as e:
                        print(f"   ⚠️  Line {line_num}: Invalid JSON - {e}")
                        continue
        
        if not self.records:
            print(f"   ❌ No valid records found in {self.data_path}")
            return False
        
        print(f"   Loaded {len(self.records)} valid records")
        
        # Create DataFrame safely
        try:
            # Create a simplified DataFrame by extracting first-level keys
            simple_records = []
            for record in self.records:
                simple_record = {}
                for key, value in record.items():
                    # Only include non-nested values
                    if not isinstance(value, (list, dict)):
                        simple_record[key] = value
                simple_records.append(simple_record)
            
            self.df = pd.DataFrame(simple_records)
            print(f"   Created DataFrame with {len(self.df)} records and {len(self.df.columns)} columns")
        except Exception as e:
            print(f"   ⚠️  Error creating DataFrame: {e}")
            self.df = pd.DataFrame()
        
        return True
    
    def extract_confidence_values(self) -> List[float]:
        """Extract confidence values from records (handles nested structures)"""
        confidence_values = []
        
        for record in self.records:
            if not isinstance(record, dict):
                continue
                
            # Check for direct confidence field
            if 'confidence' in record:
                val = record['confidence']
                if isinstance(val, (int, float)):
                    confidence_values.append(float(val))
            
            # Check for nested in extracted_facts
            if 'extracted_facts' in record and isinstance(record['extracted_facts'], list):
                for fact in record['extracted_facts']:
                    if isinstance(fact, dict) and 'confidence' in fact:
                        val = fact['confidence']
                        if isinstance(val, (int, float)):
                            confidence_values.append(float(val))
            
            # Check for nested in payload
            if 'payload' in record and isinstance(record['payload'], dict):
                if 'confidence' in record['payload']:
                    val = record['payload']['confidence']
                    if isinstance(val, (int, float)):
                        confidence_values.append(float(val))
        
        return confidence_values
    
    def check_confidence_range(self, min_val: float = 0.0, max_val: float = 1.0) -> Dict:
        """Check if confidence values are within expected range"""
        confidence_values = self.extract_confidence_values()
        
        if not confidence_values:
            return {
                'check_id': 'confidence.range',
                'column_name': 'confidence',
                'check_type': 'range',
                'status': 'WARN',
                'actual_value': 'no confidence values found',
                'expected': f'min={min_val}, max={max_val}',
                'severity': 'MEDIUM',
                'records_failing': 0,
                'sample_failing': [],
                'message': 'No confidence values found in data'
            }
        
        failing_values = [v for v in confidence_values if v < min_val or v > max_val]
        failing_count = len(failing_values)
        
        status = 'PASS' if failing_count == 0 else 'FAIL'
        
        return {
            'check_id': 'confidence.range',
            'column_name': 'confidence',
            'check_type': 'range',
            'status': status,
            'actual_value': f'min={min(confidence_values):.4f}, max={max(confidence_values):.4f}, mean={np.mean(confidence_values):.4f}',
            'expected': f'min={min_val}, max={max_val}',
            'severity': 'CRITICAL' if failing_count > 0 else 'INFO',
            'records_failing': failing_count,
            'sample_failing': failing_values[:5],
            'message': f'{failing_count} confidence values outside [{min_val}, {max_val}]' if failing_count > 0 else 'All confidence values within range'
        }
    
    def check_confidence_type(self) -> Dict:
        """Check if confidence values are floats (not integers)"""
        confidence_values = self.extract_confidence_values()
        
        if not confidence_values:
            return {
                'check_id': 'confidence.type',
                'column_name': 'confidence',
                'check_type': 'type',
                'status': 'WARN',
                'actual_value': 'no values',
                'expected': 'float',
                'severity': 'MEDIUM',
                'records_failing': 0,
                'sample_failing': [],
                'message': 'No confidence values to check'
            }
        
        # Check if any values are integers (0-100 scale would appear as integers)
        integer_values = [v for v in confidence_values if v == int(v) and v > 1]
        failing_count = len(integer_values)
        
        status = 'FAIL' if failing_count > 0 else 'PASS'
        
        return {
            'check_id': 'confidence.type',
            'column_name': 'confidence',
            'check_type': 'type',
            'status': status,
            'actual_value': f'{failing_count} integer values found' if failing_count > 0 else 'all floats',
            'expected': 'float (0.0-1.0)',
            'severity': 'CRITICAL' if failing_count > 0 else 'INFO',
            'records_failing': failing_count,
            'sample_failing': integer_values[:5],
            'message': f'Found {failing_count} confidence values that appear to be integers (possibly 0-100 scale)' if failing_count > 0 else 'All confidence values are floats'
        }
    
    def check_row_count(self, min_records: int = 50) -> Dict:
        """Check if we have enough records"""
        record_count = len(self.records)
        
        status = 'PASS' if record_count >= min_records else 'FAIL'
        
        return {
            'check_id': 'row_count',
            'column_name': 'records',
            'check_type': 'count',
            'status': status,
            'actual_value': str(record_count),
            'expected': f'>= {min_records}',
            'severity': 'MEDIUM' if record_count < min_records else 'INFO',
            'records_failing': max(0, min_records - record_count),
            'sample_failing': [],
            'message': f'Found {record_count} records, expected at least {min_records}' if record_count < min_records else f'Sufficient records: {record_count}'
        }
    
    def check_time_order(self) -> Dict:
        """Check if recorded_at >= occurred_at"""
        violations = []
        
        for i, record in enumerate(self.records):
            if not isinstance(record, dict):
                continue
                
            occurred = record.get('occurred_at')
            recorded = record.get('recorded_at')
            
            if occurred and recorded:
                try:
                    # Parse ISO timestamps
                    occurred_str = str(occurred).replace('Z', '+00:00')
                    recorded_str = str(recorded).replace('Z', '+00:00')
                    
                    occurred_dt = datetime.fromisoformat(occurred_str)
                    recorded_dt = datetime.fromisoformat(recorded_str)
                    
                    if recorded_dt < occurred_dt:
                        violations.append((i, occurred, recorded))
                except Exception as e:
                    # Skip if we can't parse dates
                    pass
        
        status = 'PASS' if not violations else 'FAIL'
        
        return {
            'check_id': 'time_order',
            'column_name': 'occurred_at/recorded_at',
            'check_type': 'temporal',
            'status': status,
            'actual_value': f'{len(violations)} violations' if violations else 'all records have recorded_at >= occurred_at',
            'expected': 'recorded_at >= occurred_at',
            'severity': 'CRITICAL' if violations else 'INFO',
            'records_failing': len(violations),
            'sample_failing': [f'record {v[0]}: occurred={v[1]}, recorded={v[2]}' for v in violations[:5]],
            'message': f'Found {len(violations)} records where recorded_at < occurred_at' if violations else 'Time order is correct'
        }
    
    def check_sequence_numbers(self) -> Dict:
        """Check sequence number ordering"""
        sequence_values = []
        for record in self.records:
            if isinstance(record, dict) and 'sequence_number' in record:
                seq = record['sequence_number']
                if isinstance(seq, (int, float)):
                    sequence_values.append(seq)
        
        if not sequence_values:
            return {
                'check_id': 'sequence_number.order',
                'column_name': 'sequence_number',
                'check_type': 'sequence',
                'status': 'SKIP',
                'actual_value': 'no sequence numbers',
                'expected': 'monotonically increasing',
                'severity': 'INFO',
                'records_failing': 0,
                'sample_failing': [],
                'message': 'No sequence numbers found to check'
            }
        
        # Check if sequence numbers are increasing
        violations = []
        for i in range(1, len(sequence_values)):
            if sequence_values[i] <= sequence_values[i-1]:
                violations.append((i, sequence_values[i-1], sequence_values[i]))
        
        status = 'PASS' if not violations else 'FAIL'
        
        return {
            'check_id': 'sequence_number.order',
            'column_name': 'sequence_number',
            'check_type': 'sequence',
            'status': status,
            'actual_value': f'{len(violations)} ordering violations' if violations else 'monotonically increasing',
            'expected': 'sequence_number > previous',
            'severity': 'HIGH' if violations else 'INFO',
            'records_failing': len(violations),
            'sample_failing': [f'position {v[0]}: {v[1]} -> {v[2]}' for v in violations[:5]],
            'message': f'Found {len(violations)} sequence number violations' if violations else 'Sequence numbers are ordered correctly'
        }
    
    def check_statistical_drift(self) -> Dict:
        """Check statistical drift against baseline"""
        confidence_values = self.extract_confidence_values()
        
        if not confidence_values:
            return {
                'check_id': 'confidence.statistical_drift',
                'column_name': 'confidence',
                'check_type': 'statistical',
                'status': 'SKIP',
                'actual_value': 'no data',
                'expected': 'baseline distribution',
                'severity': 'INFO',
                'records_failing': 0,
                'sample_failing': [],
                'message': 'No confidence values for statistical analysis'
            }
        
        current_mean = np.mean(confidence_values)
        current_std = np.std(confidence_values)
        
        # Try to load baseline
        baseline_path = Path('schema_snapshots') / f"{self.contract.get('id', 'contract')}_baseline.json"
        
        if baseline_path.exists():
            try:
                with open(baseline_path, 'r') as f:
                    baseline = json.load(f)
                
                baseline_mean = baseline.get('statistics', {}).get('confidence', {}).get('mean', current_mean)
                baseline_std = baseline.get('statistics', {}).get('confidence', {}).get('std', current_std)
                
                # Calculate drift in standard deviations
                if baseline_std > 0:
                    drift = abs(current_mean - baseline_mean) / baseline_std
                else:
                    drift = 0
                
                if drift > 3:
                    status = 'FAIL'
                    severity = 'CRITICAL'
                    message = f'Mean drift > 3 sigma: {current_mean:.4f} vs baseline {baseline_mean:.4f}'
                elif drift > 2:
                    status = 'WARN'
                    severity = 'HIGH'
                    message = f'Mean drift > 2 sigma: {current_mean:.4f} vs baseline {baseline_mean:.4f}'
                else:
                    status = 'PASS'
                    severity = 'INFO'
                    message = f'Statistical drift within bounds: {drift:.2f} sigma'
                
                return {
                    'check_id': 'confidence.statistical_drift',
                    'column_name': 'confidence',
                    'check_type': 'statistical',
                    'status': status,
                    'actual_value': f'mean={current_mean:.4f}, std={current_std:.4f}',
                    'expected': f'mean={baseline_mean:.4f} ± 3σ',
                    'severity': severity,
                    'records_failing': 0,
                    'sample_failing': [],
                    'message': message
                }
            except Exception as e:
                pass
        
        # Save baseline for future runs
        self.save_baseline(confidence_values)
        
        return {
            'check_id': 'confidence.statistical_drift',
            'column_name': 'confidence',
            'check_type': 'statistical',
            'status': 'PASS',
            'actual_value': f'mean={current_mean:.4f}, std={current_std:.4f}',
            'expected': 'baseline established',
            'severity': 'INFO',
            'records_failing': 0,
            'sample_failing': [],
            'message': 'Baseline established for future drift detection'
        }
    
    def save_baseline(self, confidence_values: List[float]):
        """Save baseline statistics for future drift detection"""
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        
        baseline_file = baseline_path / f"{self.contract.get('id', 'contract')}_baseline.json"
        
        baseline_data = {
            'timestamp': datetime.now().isoformat(),
            'source': str(self.data_path),
            'statistics': {
                'confidence': {
                    'mean': np.mean(confidence_values) if confidence_values else None,
                    'std': np.std(confidence_values) if confidence_values else None,
                    'count': len(confidence_values)
                }
            }
        }
        
        with open(baseline_file, 'w') as f:
            json.dump(baseline_data, f, indent=2)
        
        print(f"\n💾 Baseline saved to {baseline_file}")
    
    def compute_snapshot_id(self) -> str:
        """Compute SHA256 of input data"""
        if not self.data_path.exists():
            return "file-not-found"
        
        with open(self.data_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    
    def run(self) -> Dict:
        """Execute all validation checks"""
        self.load_contract()
        
        if not self.load_data():
            print("❌ Failed to load data")
            return {
                'report_id': str(uuid.uuid4()),
                'contract_id': self.contract.get('id', 'unknown'),
                'snapshot_id': 'no-data',
                'run_timestamp': datetime.now().isoformat(),
                'total_checks': 0,
                'passed': 0,
                'failed': 0,
                'warned': 0,
                'errored': 1,
                'results': []
            }
        
        results = []
        total_checks = 0
        passed = 0
        failed = 0
        warned = 0
        errored = 0
        
        print("\n🔍 Running validation checks...")
        
        # Check row count
        total_checks += 1
        result = self.check_row_count(50)
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
            print(f"   ✅ {result['check_id']}: {result['message']}")
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check confidence range
        total_checks += 1
        result = self.check_confidence_range(0.0, 1.0)
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
            print(f"   ✅ {result['check_id']}: {result['message']}")
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        elif result['status'] == 'WARN':
            warned += 1
            print(f"   ⚠️  {result['check_id']}: {result['message']}")
        
        # Check confidence type
        total_checks += 1
        result = self.check_confidence_type()
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
            print(f"   ✅ {result['check_id']}: {result['message']}")
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check time order if applicable
        if any('occurred_at' in r for r in self.records):
            total_checks += 1
            result = self.check_time_order()
            results.append(result)
            if result['status'] == 'PASS':
                passed += 1
                print(f"   ✅ {result['check_id']}: {result['message']}")
            elif result['status'] == 'FAIL':
                failed += 1
                print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check sequence numbers if applicable
        if any('sequence_number' in r for r in self.records):
            total_checks += 1
            result = self.check_sequence_numbers()
            results.append(result)
            if result['status'] == 'PASS':
                passed += 1
                print(f"   ✅ {result['check_id']}: {result['message']}")
            elif result['status'] == 'FAIL':
                failed += 1
                print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Statistical drift detection
        total_checks += 1
        result = self.check_statistical_drift()
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
            print(f"   ✅ {result['check_id']}: {result['message']}")
        elif result['status'] == 'WARN':
            warned += 1
            print(f"   ⚠️  {result['check_id']}: {result['message']}")
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Build final report
        report = {
            'report_id': str(uuid.uuid4()),
            'contract_id': self.contract.get('id', 'unknown'),
            'snapshot_id': self.compute_snapshot_id(),
            'run_timestamp': datetime.now().isoformat(),
            'total_checks': total_checks,
            'passed': passed,
            'failed': failed,
            'warned': warned,
            'errored': errored,
            'results': results
        }
        
        # Save report
        report_file = self.output_dir / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n{'='*60}")
        print(f"📊 VALIDATION COMPLETE")
        print(f"{'='*60}")
        print(f"Total checks: {total_checks}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Warnings: {warned}")
        print(f"🔴 Errors: {errored}")
        print(f"\n💾 Report saved to {report_file}")
        
        return report


def main():
    parser = argparse.ArgumentParser(description='Validate data against contract')
    parser.add_argument('--contract', required=True, help='Path to contract YAML')
    parser.add_argument('--data', required=True, help='Path to data JSONL')
    parser.add_argument('--output', default='validation_reports', help='Output directory')
    
    args = parser.parse_args()
    
    runner = FinalValidationRunner(args.contract, args.data, args.output)
    report = runner.run()
    
    # Exit with error code if any failures
    if report['failed'] > 0:
        print(f"\n❌ Validation failed with {report['failed']} failures")
        exit(1)
    else:
        print(f"\n✅ All validation checks passed!")


if __name__ == '__main__':
    main()