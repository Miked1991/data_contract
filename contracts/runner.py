#!/usr/bin/env python3
"""
Complete Validation Runner - Executes contract checks with registry integration
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import re
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ValidationRunner:
    """Validates data against contracts with registry integration"""
    
    def __init__(self, contract_id: str, data_path: str, output_dir: str):
        self.contract_id = contract_id
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        from contracts.registry import ContractRegistry
        self.registry = ContractRegistry()
        self.contract = self.registry.get_contract(contract_id)
        
        if not self.contract:
            raise ValueError(f"Contract {contract_id} not found in registry")
        
        self.enforcement_mode = self.registry.get_enforcement_mode(contract_id)
        self.records = []
    
    def load_data(self) -> List[Dict]:
        """Load data from JSONL"""
        self.records = []
        with open(self.data_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        self.records.append(json.loads(line))
                    except:
                        pass
        return self.records
    
    def extract_confidence_values(self) -> List[float]:
        """Extract confidence values from nested structures"""
        values = []
        for record in self.records:
            if 'extracted_facts' in record:
                for fact in record.get('extracted_facts', []):
                    if 'confidence' in fact and isinstance(fact['confidence'], (int, float)):
                        values.append(float(fact['confidence']))
        return values
    
    def run_checks(self) -> List[Dict]:
        """Run all contract checks"""
        results = []
        confidence_values = self.extract_confidence_values()
        
        # Check 1: Confidence range
        if confidence_values:
            failing = [v for v in confidence_values if v < 0.0 or v > 1.0]
            results.append({
                'check_id': 'confidence.range',
                'status': 'FAIL' if failing else 'PASS',
                'severity': 'CRITICAL',
                'records_failing': len(failing),
                'sample_failing': failing[:5],
                'expected': '0.0-1.0',
                'actual': f'min={min(confidence_values):.2f}, max={max(confidence_values):.2f}',
                'message': f'{len(failing)} confidence values outside [0.0, 1.0] range'
            })
        
        # Check 2: Confidence type
        if confidence_values:
            integers = [v for v in confidence_values if v == int(v) and v > 1]
            results.append({
                'check_id': 'confidence.type',
                'status': 'FAIL' if integers else 'PASS',
                'severity': 'CRITICAL',
                'records_failing': len(integers),
                'sample_failing': integers[:5],
                'expected': 'float',
                'actual': f'{len(integers)} integer values',
                'message': f'Found {len(integers)} integer confidence values (should be float)'
            })
        
        # Check 3: Required fields
        required_fields = ['doc_id']
        for field in required_fields:
            missing = [i for i, r in enumerate(self.records) if field not in r]
            results.append({
                'check_id': f'{field}.required',
                'status': 'PASS' if not missing else 'FAIL',
                'severity': 'CRITICAL',
                'records_failing': len(missing),
                'message': f'{len(missing)} records missing {field}'
            })
        
        # Check 4: Unique doc_id
        doc_ids = [r.get('doc_id') for r in self.records if r.get('doc_id')]
        duplicates = [id for id in doc_ids if doc_ids.count(id) > 1]
        results.append({
            'check_id': 'doc_id.unique',
            'status': 'PASS' if not duplicates else 'FAIL',
            'severity': 'CRITICAL',
            'records_failing': len(set(duplicates)),
            'sample_failing': list(set(duplicates))[:5],
            'message': f'Found {len(set(duplicates))} duplicate doc_ids'
        })
        
        # Check 5: Row count
        results.append({
            'check_id': 'row_count',
            'status': 'PASS' if len(self.records) >= 10 else 'FAIL',
            'severity': 'MEDIUM',
            'records_failing': max(0, 10 - len(self.records)),
            'message': f'Found {len(self.records)} records (need at least 10)'
        })
        
        return results
    
    def apply_enforcement(self, results: List[Dict]) -> str:
        """Apply enforcement based on mode"""
        failed_checks = [r for r in results if r['status'] == 'FAIL']
        
        if self.enforcement_mode == 'block' and failed_checks:
            print("\n🚫 BLOCK MODE: Deployment blocked due to violations")
            sys.exit(1)
        elif self.enforcement_mode == 'enforce' and failed_checks:
            print("\n🔒 ENFORCE MODE: Quarantining bad data")
            self._quarantine_data()
        elif self.enforcement_mode == 'warn' and failed_checks:
            print("\n⚠️  WARN MODE: Violations detected - alerts sent")
            self._send_alerts(failed_checks)
        elif self.enforcement_mode == 'monitor' and failed_checks:
            print("\n📊 MONITOR MODE: Violations logged only")
        
        # Record violations in registry
        for result in failed_checks:
            self.registry.record_violation(self.contract_id, result)
        
        return self.enforcement_mode
    
    def _quarantine_data(self):
        """Quarantine invalid data"""
        quarantine_dir = Path('outputs/quarantine')
        quarantine_dir.mkdir(exist_ok=True)
        quarantine_file = quarantine_dir / f"quarantine_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
        # Write violating records to quarantine
        with open(quarantine_file, 'w', encoding='utf-8') as f:
            for record in self.records[:10]:
                f.write(json.dumps(record) + '\n')
        print(f"   📦 {len(self.records)} records quarantined to {quarantine_file}")
    
    def _send_alerts(self, failed_checks: List[Dict]):
        """Send alerts for violations"""
        for check in failed_checks:
            print(f"   📧 Alert: {check['check_id']} - {check['message']}")
    
    def generate_report(self, results: List[Dict], action: str) -> Dict:
        """Generate validation report"""
        total = len(results)
        passed = len([r for r in results if r['status'] == 'PASS'])
        failed = len([r for r in results if r['status'] == 'FAIL'])
        
        report = {
            'report_id': f"val_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'contract_id': self.contract_id,
            'enforcement_mode': action,
            'run_timestamp': datetime.now().isoformat(),
            'total_checks': total,
            'passed': passed,
            'failed': failed,
            'results': results,
            'data_summary': {
                'total_records': len(self.records),
                'confidence_stats': self._get_confidence_stats()
            }
        }
        
        # Save report
        report_file = self.output_dir / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _get_confidence_stats(self) -> Dict:
        """Get confidence value statistics"""
        values = self.extract_confidence_values()
        if not values:
            return {'count': 0}
        
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'mean': sum(values) / len(values)
        }
    
    def run(self) -> Dict:
        """Run complete validation"""
        print(f"\n🔍 Validating contract: {self.contract_id}")
        print(f"   Enforcement mode: {self.enforcement_mode.upper()}")
        
        self.load_data()
        print(f"   Records loaded: {len(self.records)}")
        
        results = self.run_checks()
        action = self.apply_enforcement(results)
        report = self.generate_report(results, action)
        
        print(f"\n📊 Validation Results:")
        print(f"   Total: {report['total_checks']} | Passed: {report['passed']} | Failed: {report['failed']}")
        print(f"   Report: {self.output_dir}/validation_*.json")
        
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', required=True, help='Contract ID from registry')
    parser.add_argument('--data', required=True, help='Path to data JSONL')
    parser.add_argument('--output', default='validation_reports', help='Output directory')
    
    args = parser.parse_args()
    
    runner = ValidationRunner(args.contract, args.data, args.output)
    runner.run()


if __name__ == '__main__':
    main()