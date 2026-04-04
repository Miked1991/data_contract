# contracts/runner.py (UPDATED)
#!/usr/bin/env python3
"""
Validation Runner with Registry Integration and Enforcement Modes
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from registry import ContractRegistry, EnforcementMode


class ValidationRunnerWithEnforcement:
    """Validates data against contracts with enforcement modes"""
    
    def __init__(self, contract_id: str, data_path: str, output_dir: str):
        self.contract_id = contract_id
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.registry = ContractRegistry()
        self.enforcement_mode = self.registry.get_enforcement_mode(contract_id)
        
    def run_validation(self) -> Dict:
        """Run validation with enforcement"""
        
        print(f"\n🔍 Validating contract: {self.contract_id}")
        print(f"   Enforcement Mode: {self.enforcement_mode.value.upper()}")
        
        # Load contract from registry
        contract = self.registry.get_contract(self.contract_id)
        if not contract:
            print(f"❌ Contract {self.contract_id} not found in registry")
            return {'error': 'Contract not found'}
        
        # Simulate validation results (for demo)
        # In production, this would actually validate the data
        results = self._simulate_validation()
        
        # Apply enforcement based on mode
        action = self._apply_enforcement(results)
        
        # Record breaches for affected consumers
        if results['failed'] > 0:
            self._record_consumer_breaches(results)
        
        # Generate impact analysis
        impact = self.registry.get_consumer_impact_analysis(self.contract_id, results)
        
        report = {
            'contract_id': self.contract_id,
            'enforcement_mode': self.enforcement_mode.value,
            'run_timestamp': datetime.now().isoformat(),
            'total_checks': results['total'],
            'passed': results['passed'],
            'failed': results['failed'],
            'action_taken': action,
            'impact_analysis': impact,
            'violations': results.get('violations', [])
        }
        
        # Save report
        output_path = self.output_dir / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📊 Validation Report saved to {output_path}")
        print(f"   Action Taken: {action}")
        
        return report
    
    def _simulate_validation(self) -> Dict:
        """Simulate validation results (replace with actual validation)"""
        return {
            'total': 5,
            'passed': 3,
            'failed': 2,
            'violations': [
                {
                    'check_id': 'confidence.range',
                    'severity': 'CRITICAL',
                    'message': '8 confidence values outside [0.0, 1.0] range',
                    'affected_fields': ['confidence'],
                    'severity_score': 10
                },
                {
                    'check_id': 'confidence.type',
                    'severity': 'CRITICAL',
                    'message': 'Integer confidence values found',
                    'affected_fields': ['confidence'],
                    'severity_score': 10
                }
            ]
        }
    
    def _apply_enforcement(self, results: Dict) -> str:
        """Apply enforcement based on mode"""
        
        if self.enforcement_mode == EnforcementMode.AUDIT:
            print("   📋 AUDIT MODE: Full logging enabled")
            return "AUDIT_COMPLETE"
        
        elif self.enforcement_mode == EnforcementMode.BLOCK:
            if results['failed'] > 0:
                print("   🚫 BLOCK MODE: Deployment blocked due to violations")
                sys.exit(1)
            return "BLOCK_PASS"
        
        elif self.enforcement_mode == EnforcementMode.ENFORCE:
            if results['failed'] > 0:
                print("   🔒 ENFORCE MODE: Quarantining bad data")
                self._quarantine_data(results)
            return "ENFORCE_QUARANTINED"
        
        elif self.enforcement_mode == EnforcementMode.WARN:
            if results['failed'] > 0:
                print("   ⚠️  WARN MODE: Violations detected but continuing")
                self._send_warning(results)
            return "WARN_CONTINUE"
        
        else:  # MONITOR mode
            if results['failed'] > 0:
                print("   📊 MONITOR MODE: Violations logged only")
            return "MONITOR_LOG"
    
    def _quarantine_data(self, results: Dict):
        """Quarantine invalid data"""
        quarantine_dir = Path('outputs/quarantine')
        quarantine_dir.mkdir(exist_ok=True)
        print(f"   📦 Data quarantined to {quarantine_dir}")
    
    def _send_warning(self, results: Dict):
        """Send warning alerts"""
        print(f"   📧 Warning sent to data-quality@example.com")
    
    def _record_consumer_breaches(self, results: Dict):
        """Record breaches for affected consumers"""
        for violation in results.get('violations', []):
            # In production, would identify specific consumers
            print(f"   📝 Recorded breach for violation: {violation['check_id']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', help='Contract ID (for registry lookup)')
    parser.add_argument('--contract-file', help='Contract file path (legacy mode)')
    parser.add_argument('--data', required=True)
    parser.add_argument('--output', default='validation_reports')
    
    args = parser.parse_args()
    
    # Determine contract source
    if args.contract:
        contract_id = args.contract
    elif args.contract_file:
        # Legacy mode - register first
        from generator import ContractGeneratorWithRegistry
        gen = ContractGeneratorWithRegistry(args.contract_file, 'generated_contracts')
        contract = gen.generate_contract()
        contract_id = contract['id']
    else:
        print("❌ Either --contract or --contract-file required")
        sys.exit(1)
    
    runner = ValidationRunnerWithEnforcement(contract_id, args.data, args.output)
    report = runner.run_validation()
    
    # Exit with error if block mode failed
    if report.get('action_taken') == 'BLOCK_PASS' and report['failed'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()