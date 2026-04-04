# contracts/report_generator.py
#!/usr/bin/env python3
"""
Complete Report Generator - Generates full Enforcer Report with all required sections
"""

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List


class ReportGenerator:
    """Generates complete Enforcer Report"""
    
    def __init__(self, validation_dir: str, violation_log: str, ai_metrics: str, output_dir: str):
        self.validation_dir = Path(validation_dir)
        self.violation_log = Path(violation_log)
        self.ai_metrics = Path(ai_metrics)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        from contracts.registry import ContractRegistry
        self.registry = ContractRegistry()
    
    def load_validation_results(self) -> List[Dict]:
        """Load all validation results"""
        results = []
        for f in sorted(self.validation_dir.glob('validation_*.json')):
            with open(f, 'r') as fp:
                results.append(json.load(fp))
        return results
    
    def load_violations(self) -> List[Dict]:
        """Load all violations"""
        violations = []
        if self.violation_log.exists():
            with open(self.violation_log, 'r') as f:
                for line in f:
                    if line.strip():
                        violations.append(json.loads(line))
        return violations
    
    def load_ai_metrics(self) -> Dict:
        """Load AI metrics"""
        if self.ai_metrics.exists():
            with open(self.ai_metrics, 'r') as f:
                return json.load(f)
        return {}
    
    def calculate_health_score(self, validations: List[Dict], violations: List[Dict]) -> int:
        """Calculate data health score (0-100)"""
        if not validations:
            return 50
        
        latest = validations[-1]
        total = latest.get('total_checks', 1)
        passed = latest.get('passed', 0)
        
        base_score = (passed / total) * 100
        
        # Penalty for critical violations
        critical_count = len([v for v in violations if v.get('severity') == 'CRITICAL'])
        penalty = min(30, critical_count * 10)
        
        # Bonus for good AI metrics
        ai_metrics = self.load_ai_metrics()
        ai_bonus = 0
        if ai_metrics.get('embedding_drift', {}).get('status') == 'PASS':
            ai_bonus += 5
        
        score = max(0, min(100, base_score - penalty + ai_bonus))
        return int(score)
    
    def generate_report(self) -> Dict:
        """Generate complete report"""
        validations = self.load_validation_results()
        violations = self.load_violations()
        ai_metrics = self.load_ai_metrics()
        
        health_score = self.calculate_health_score(validations, violations)
        
        # Determine narrative
        if health_score >= 80:
            narrative = "Excellent data health. All systems operating within contract boundaries."
        elif health_score >= 60:
            narrative = "Good data health. Minor violations detected but under control."
        elif health_score >= 40:
            narrative = "Fair data health. Several violations require attention."
        else:
            narrative = "Poor data health. Critical violations need immediate remediation."
        
        # Prepare top violations
        top_violations = []
        for v in violations[:3]:
            top_violations.append({
                'check_id': v.get('check_id'),
                'severity': v.get('severity'),
                'message': v.get('message'),
                'records_affected': v.get('records_failing', 0)
            })
        
        report = {
            'report_id': f"enforcer_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'generated_at': datetime.now().isoformat(),
            'data_health_score': health_score,
            'narrative': narrative,
            'statistics': {
                'total_validations': len(validations),
                'total_violations': len(violations),
                'critical_violations': len([v for v in violations if v.get('severity') == 'CRITICAL']),
                'high_violations': len([v for v in violations if v.get('severity') == 'HIGH']),
                'contracts_in_registry': len(self.registry.list_contracts())
            },
            'violations_this_week': {
                'count': len(violations),
                'by_severity': {
                    'CRITICAL': len([v for v in violations if v.get('severity') == 'CRITICAL']),
                    'HIGH': len([v for v in violations if v.get('severity') == 'HIGH']),
                    'MEDIUM': len([v for v in violations if v.get('severity') == 'MEDIUM'])
                },
                'top_violations': top_violations
            },
            'ai_risk_assessment': {
                'embedding_drift': ai_metrics.get('embedding_drift', {}),
                'llm_output': ai_metrics.get('llm_output', {}),
                'overall_status': ai_metrics.get('overall_status', 'UNKNOWN')
            },
            'registry_summary': self.registry.generate_registry_report(),
            'recommended_actions': self.generate_recommendations(violations, ai_metrics)
        }
        
        return report
    
    def generate_recommendations(self, violations: List[Dict], ai_metrics: Dict) -> List[Dict]:
        """Generate prioritized recommendations"""
        recommendations = []
        
        # Priority 1: Confidence scale issues
        confidence_violations = [v for v in violations if 'confidence' in v.get('check_id', '')]
        if confidence_violations:
            recommendations.append({
                'priority': 1,
                'action': 'Fix confidence scale in extraction pipeline',
                'details': 'Update extractor.py to output float in [0.0, 1.0] instead of integer 0-100',
                'risk_reduction': 'Eliminates CRITICAL violations affecting downstream consumers',
                'owner': 'extraction-team'
            })
        
        # Priority 2: Schema drift
        recommendations.append({
            'priority': 2,
            'action': 'Add schema validation to CI/CD pipeline',
            'details': 'Prevent schema changes from reaching production without validation',
            'risk_reduction': 'Catches breaking changes before deployment',
            'owner': 'platform-team'
        })
        
        # Priority 3: AI model monitoring
        if ai_metrics.get('embedding_drift', {}).get('drift_score', 0) > 0.1:
            recommendations.append({
                'priority': 3,
                'action': 'Investigate embedding drift',
                'details': 'Current drift score indicates potential quality degradation',
                'risk_reduction': 'Maintains AI extraction quality',
                'owner': 'ml-team'
            })
        
        return recommendations
    
    def save_report(self, report: Dict):
        """Save report to file"""
        output_file = self.output_dir / 'report_data.json'
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"✅ Report saved to {output_file}")
        
        # Also save readable version
        readable_file = self.output_dir / f"report_{datetime.now().strftime('%Y%m%d')}.json"
        with open(readable_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"✅ Readable report saved to {readable_file}")
        
        return output_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--validation-dir', default='validation_reports')
    parser.add_argument('--violation-log', default='violation_log/violations.jsonl')
    parser.add_argument('--ai-metrics', default='validation_reports/ai_metrics.json')
    parser.add_argument('--output', default='enforcer_report')
    
    args = parser.parse_args()
    
    generator = ReportGenerator(args.validation_dir, args.violation_log, args.ai_metrics, args.output)
    report = generator.generate_report()
    generator.save_report(report)
    
    print(f"\n📊 Report Summary:")
    print(f"   Health Score: {report['data_health_score']}/100")
    print(f"   Total Violations: {report['statistics']['total_violations']}")
    print(f"   Critical: {report['statistics']['critical_violations']}")


if __name__ == '__main__':
    main()