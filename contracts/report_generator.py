# contracts/report_generator.py
#!/usr/bin/env python3
"""
ReportGenerator - Auto-generates Enforcer Report from validation data
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import numpy as np


class ReportGenerator:
    """Generates the Enforcer Report from live validation data"""
    
    def __init__(self, validation_dir: str, violation_log: str, ai_metrics: str, output_dir: str):
        self.validation_dir = Path(validation_dir)
        self.violation_log = Path(violation_log)
        self.ai_metrics = Path(ai_metrics)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_validation_reports(self) -> List[Dict]:
        """Load all validation reports"""
        reports = []
        for report_file in sorted(self.validation_dir.glob('validation_*.json')):
            with open(report_file, 'r') as f:
                reports.append(json.load(f))
        return reports
    
    def load_violations(self) -> List[Dict]:
        """Load violation log"""
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
    
    def calculate_health_score(self, reports: List[Dict], violations: List[Dict]) -> int:
        """Calculate data health score (0-100)"""
        if not reports:
            return 0
        
        # Get latest report
        latest = reports[-1]
        total = latest.get('total_checks', 1)
        passed = latest.get('passed', 0)
        
        # Base score from pass rate
        base_score = (passed / total) * 100 if total > 0 else 0
        
        # Penalties for critical violations
        critical_penalty = 0
        for violation in violations:
            if violation.get('severity') == 'CRITICAL':
                critical_penalty += 20
        
        # Bonus for good AI metrics
        ai_bonus = 0
        ai_metrics = self.load_ai_metrics()
        if ai_metrics.get('embedding_drift', {}).get('status') == 'PASS':
            ai_bonus += 5
        if ai_metrics.get('llm_output', {}).get('violation_rate', 1) < 0.02:
            ai_bonus += 5
        
        health_score = max(0, min(100, base_score - critical_penalty + ai_bonus))
        return int(health_score)
    
    def generate_report_data(self) -> Dict:
        """Generate complete report data"""
        reports = self.load_validation_reports()
        violations = self.load_violations()
        ai_metrics = self.load_ai_metrics()
        
        health_score = self.calculate_health_score(reports, violations)
        
        # Determine narrative based on health score
        if health_score >= 90:
            narrative = "Excellent data health. All systems operating within contract boundaries."
        elif health_score >= 70:
            narrative = "Good data health. Minor violations detected but under control."
        elif health_score >= 50:
            narrative = "Fair data health. Several violations require attention."
        else:
            narrative = "Poor data health. Critical violations need immediate remediation."
        
        # Top violations
        top_violations = []
        for v in violations[:3]:
            top_violations.append({
                'check_id': v.get('check_id'),
                'severity': v.get('severity'),
                'message': v.get('message'),
                'records_affected': v.get('records_failing', 0)
            })
        
        report_data = {
            'report_id': str(hash(datetime.now())),
            'generated_at': datetime.now().isoformat(),
            'data_health_score': health_score,
            'narrative': narrative,
            'statistics': {
                'total_validation_runs': len(reports),
                'total_violations': len(violations),
                'critical_violations': len([v for v in violations if v.get('severity') == 'CRITICAL']),
                'high_violations': len([v for v in violations if v.get('severity') == 'HIGH'])
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
            'recommended_actions': self.generate_recommendations(violations, ai_metrics)
        }
        
        return report_data
    
    def generate_recommendations(self, violations: List[Dict], ai_metrics: Dict) -> List[Dict]:
        """Generate prioritized recommendations"""
        recommendations = []
        
        # Priority 1: Fix confidence scale violations
        confidence_violations = [v for v in violations if 'confidence' in v.get('check_id', '')]
        if confidence_violations:
            recommendations.append({
                'priority': 1,
                'action': 'Fix confidence scale in extraction pipeline',
                'details': 'Update extractor.py to output confidence as float in [0.0, 1.0] instead of integer 0-100',
                'risk_reduction': 'Eliminates critical violations affecting 3 downstream systems'
            })
        
        # Priority 2: Address embedding drift
        if ai_metrics.get('embedding_drift', {}).get('status') == 'FAIL':
            recommendations.append({
                'priority': 2,
                'action': 'Investigate embedding drift in extraction texts',
                'details': 'Text embeddings have drifted beyond threshold - check for changes in extraction logic',
                'risk_reduction': 'Prevents degradation in semantic search and retrieval quality'
            })
        
        # Priority 3: Improve contract coverage
        recommendations.append({
            'priority': 3,
            'action': 'Add contract for Week 4 Cartographer lineage graph',
            'details': 'Currently partial coverage - add validation for nodes, edges, and relationship types',
            'risk_reduction': 'Prevents silent failures in lineage graph construction'
        })
        
        return recommendations
    
    def save_report(self, report_data: Dict):
        """Save report to file"""
        output_file = self.output_dir / 'report_data.json'
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        print(f"✅ Report saved to {output_file}")
        
        # Also save a human-readable version
        readable_file = self.output_dir / f'report_{datetime.now().strftime("%Y%m%d")}.json'
        with open(readable_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        print(f"✅ Human-readable report saved to {readable_file}")
        
        return output_file
    
    def run(self):
        """Generate the enforcer report"""
        print("\n" + "="*60)
        print("📊 Generating Enforcer Report")
        print("="*60)
        
        report_data = self.generate_report_data()
        
        print(f"\n📈 Data Health Score: {report_data['data_health_score']}/100")
        print(f"   {report_data['narrative']}")
        print(f"\n   Total violations: {report_data['statistics']['total_violations']}")
        print(f"   Critical violations: {report_data['statistics']['critical_violations']}")
        
        self.save_report(report_data)
        
        print("\n✨ Report generation complete!")


def main():
    parser = argparse.ArgumentParser(description='Generate Enforcer Report')
    parser.add_argument('--validation-dir', default='validation_reports', help='Validation reports directory')
    parser.add_argument('--violation-log', default='violation_log/violations.jsonl', help='Violation log path')
    parser.add_argument('--ai-metrics', default='validation_reports/ai_metrics.json', help='AI metrics path')
    parser.add_argument('--output', default='enforcer_report', help='Output directory')
    
    args = parser.parse_args()
    
    generator = ReportGenerator(args.validation_dir, args.violation_log, args.ai_metrics, args.output)
    generator.run()


if __name__ == '__main__':
    main()