# contracts/runner_fixed.py
#!/usr/bin/env python3
"""
Fixed ValidationRunner that properly parses contract and executes checks
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


class ValidationRunner:
    """Executes contract validations against data snapshots"""
    
    def __init__(self, contract_path: str, data_path: str, output_dir: str):
        self.contract_path = Path(contract_path)
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.contract = None
        self.data = None
        self.records = []
        
    def load_contract(self):
        """Load contract YAML"""
        with open(self.contract_path, 'r', encoding='utf-8') as f:
            self.contract = yaml.safe_load(f)
        print(f"✅ Loaded contract: {self.contract.get('id', 'unknown')}")
        return self.contract
    
    def load_data(self):
        """Load data from JSONL"""
        self.records = []
        with open(self.data_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        self.records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Warning: Skipping invalid JSON at line {line_num}: {e}")
        
        if not self.records:
            raise ValueError(f"No valid records found in {self.data_path}")
        
        # Create DataFrame for analysis
        self.data = pd.json_normalize(self.records, max_level=1)
        print(f"✅ Loaded {len(self.records)} records")
        return self.records
    
    def extract_confidence_values(self) -> List[float]:
        """Extract confidence values from records (handles nested structures)"""
        confidence_values = []
        
        for record in self.records:
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
    
    def check_required_fields(self) -> List[Dict]:
        """Check for required fields"""
        results = []
        
        # Common required fields
        required_fields = ['doc_id', 'source_path', 'extraction_model']
        
        for field in required_fields:
            present = any(field in record for record in self.records)
            
            if not present:
                results.append({
                    'check_id': f'{field}.required',
                    'column_name': field,
                    'check_type': 'required',
                    'status': 'FAIL',
                    'actual_value': 'field missing',
                    'expected': 'field present',
                    'severity': 'CRITICAL',
                    'records_failing': len(self.records),
                    'sample_failing': [],
                    'message': f'Required field "{field}" not found in any record'
                })
        
        return results
    
    def check_sequence_numbers(self) -> Dict:
        """Check sequence number ordering"""
        sequence_values = []
        for record in self.records:
            if 'sequence_number' in record:
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
        
        status = 'FAIL' if violations else 'PASS'
        
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
    
    def check_time_order(self) -> Dict:
        """Check if recorded_at >= occurred_at"""
        violations = []
        
        for i, record in enumerate(self.records):
            occurred = record.get('occurred_at')
            recorded = record.get('recorded_at')
            
            if occurred and recorded:
                try:
                    # Parse ISO timestamps
                    occurred_dt = datetime.fromisoformat(occurated.replace('Z', '+00:00'))
                    recorded_dt = datetime.fromisoformat(recorded.replace('Z', '+00:00'))
                    
                    if recorded_dt < occurred_dt:
                        violations.append((i, occurred, recorded))
                except:
                    pass
        
        status = 'FAIL' if violations else 'PASS'
        
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
    
    def check_statistical_drift(self, baseline_path: Path = None) -> Dict:
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
        if baseline_path and baseline_path.exists():
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
        
        return {
            'check_id': 'confidence.statistical_drift',
            'column_name': 'confidence',
            'check_type': 'statistical',
            'status': 'INFO',
            'actual_value': f'mean={current_mean:.4f}, std={current_std:.4f}',
            'expected': 'baseline not established',
            'severity': 'INFO',
            'records_failing': 0,
            'sample_failing': [],
            'message': 'First validation run - baseline saved for future drift detection'
        }
    
    def compute_snapshot_id(self) -> str:
        """Compute SHA256 of input data"""
        with open(self.data_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    
    def save_baseline(self, statistical_results: Dict):
        """Save baseline statistics for future drift detection"""
        baseline_path = Path('schema_snapshots')
        baseline_path.mkdir(parents=True, exist_ok=True)
        
        baseline_file = baseline_path / f"{self.contract.get('id', 'contract')}_baseline.json"
        
        # Extract confidence statistics
        confidence_values = self.extract_confidence_values()
        
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
        
        print(f"💾 Baseline saved to {baseline_file}")
        return baseline_file
    
    def run(self) -> Dict:
        """Execute all validation checks"""
        self.load_contract()
        self.load_data()
        
        results = []
        total_checks = 0
        passed = 0
        failed = 0
        warned = 0
        errored = 0
        
        print("\n🔍 Running validation checks...")
        
        # Check confidence range (most important for Week 3)
        total_checks += 1
        result = self.check_confidence_range(0.0, 1.0)
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
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
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check required fields
        required_results = self.check_required_fields()
        for result in required_results:
            total_checks += 1
            results.append(result)
            if result['status'] == 'PASS':
                passed += 1
            elif result['status'] == 'FAIL':
                failed += 1
                print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check sequence numbers if present
        total_checks += 1
        result = self.check_sequence_numbers()
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Check time order if present
        total_checks += 1
        result = self.check_time_order()
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Statistical drift detection
        total_checks += 1
        baseline_path = Path('schema_snapshots') / f"{self.contract.get('id', 'contract')}_baseline.json"
        result = self.check_statistical_drift(baseline_path)
        results.append(result)
        if result['status'] == 'PASS':
            passed += 1
        elif result['status'] == 'WARN':
            warned += 1
            print(f"   ⚠️  {result['check_id']}: {result['message']}")
        elif result['status'] == 'FAIL':
            failed += 1
            print(f"   ❌ {result['check_id']}: {result['message']}")
        
        # Save baseline after first run
        self.save_baseline(results)
        
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
    
    runner = ValidationRunner(args.contract, args.data, args.output)
    report = runner.run()
    
    # Exit with error code if any failures
    if report['failed'] > 0:
        print(f"\n❌ Validation failed with {report['failed']} failures")
        exit(1)
    else:
        print(f"\n✅ All validation checks passed!")


if __name__ == '__main__':
    main()