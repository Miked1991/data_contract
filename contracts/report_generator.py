#!/usr/bin/env python3
"""
ReportGenerator - Data-Driven Construction & Section Completeness
Generates complete Enforcer Report from validation results and violation logs
"""

import json
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys
import os
import glob

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ReportGenerator:
    """
    Generates complete Enforcer Report with all required sections.
    Reads from violation_log/ and validation_reports/ directories.
    Calculates health score with penalty per CRITICAL violation.
    """
    
    def __init__(self, validation_dir: str = None, violation_log: str = None, 
                 ai_metrics: str = None, output_dir: str = None):
        """
        Initialize ReportGenerator with configurable paths.
        Uses environment variables or defaults for paths.
        
        Args:
            validation_dir: Path to validation reports (default: validation_reports/)
            violation_log: Path to violation log file (default: violation_log/violations.jsonl)
            ai_metrics: Path to AI metrics file (default: validation_reports/ai_metrics.json)
            output_dir: Output directory for report (default: enforcer_report/)
        """
        # Use environment variables or defaults - no hardcoded strings
        self.validation_dir = Path(
            validation_dir or os.environ.get('VALIDATION_REPORTS_DIR', 'validation_reports')
        )
        self.violation_log = Path(
            violation_log or os.environ.get('VIOLATION_LOG_PATH', 'violation_log/violations.jsonl')
        )
        self.ai_metrics = Path(
            ai_metrics or os.environ.get('AI_METRICS_PATH', 'validation_reports/ai_metrics.json')
        )
        self.output_dir = Path(
            output_dir or os.environ.get('ENFORCER_REPORT_DIR', 'enforcer_report')
        )
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Data stores
        self.validation_reports = []
        self.violations = []
        self.ai_metrics_data = {}
        self.schema_snapshots = []
        
    def load_data(self):
        """Load all data from configured paths"""
        
        # Load validation reports
        if self.validation_dir.exists():
            report_files = list(self.validation_dir.glob("validation_*.json"))
            for report_file in sorted(report_files):
                try:
                    with open(report_file, 'r') as f:
                        self.validation_reports.append(json.load(f))
                except Exception as e:
                    print(f"Warning: Could not load {report_file}: {e}")
        
        # Load violation log
        if self.violation_log.exists():
            try:
                with open(self.violation_log, 'r') as f:
                    for line in f:
                        if line.strip():
                            self.violations.append(json.loads(line))
            except Exception as e:
                print(f"Warning: Could not load violation log: {e}")
        
        # Load AI metrics
        if self.ai_metrics.exists():
            try:
                with open(self.ai_metrics, 'r') as f:
                    self.ai_metrics_data = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load AI metrics: {e}")
        
        # Load schema snapshots
        snapshots_dir = Path("schema_snapshots")
        if snapshots_dir.exists():
            snapshot_files = list(snapshots_dir.glob("*.json"))
            for snapshot_file in sorted(snapshot_files):
                try:
                    with open(snapshot_file, 'r') as f:
                        self.schema_snapshots.append(json.load(f))
                except Exception as e:
                    print(f"Warning: Could not load snapshot {snapshot_file}: {e}")
    
    def calculate_health_score(self) -> Dict[str, Any]:
        """
        Calculate data health score using formula:
        (checks_passed / total_checks) × 100 adjusted down by 20 points per CRITICAL violation
        """
        if not self.validation_reports:
            return {
                'score': 0,
                'narrative': 'No validation data available',
                'calculation': 'No data'
            }
        
        # Get latest validation report
        latest_report = self.validation_reports[-1]
        total_checks = latest_report.get('total_checks', 1)
        passed_checks = latest_report.get('passed', 0)
        
        # Calculate base score
        base_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        
        # Count CRITICAL violations
        critical_count = len([v for v in self.violations if v.get('severity') == 'CRITICAL'])
        
        # Apply penalty: 20 points per CRITICAL violation
        penalty = critical_count * 20
        final_score = max(0, min(100, base_score - penalty))
        
        # Generate narrative
        if final_score >= 90:
            narrative = f"Excellent (Score: {final_score:.0f}) - All systems operating within contract boundaries."
        elif final_score >= 70:
            narrative = f"Good (Score: {final_score:.0f}) - Minor issues detected, monitoring active."
        elif final_score >= 50:
            narrative = f"Fair (Score: {final_score:.0f}) - Multiple violations require attention."
        else:
            narrative = f"Poor (Score: {final_score:.0f}) - Critical violations need immediate remediation."
        
        return {
            'score': int(final_score),
            'narrative': narrative,
            'calculation': {
                'base_score': base_score,
                'critical_violations': critical_count,
                'penalty': penalty,
                'total_checks': total_checks,
                'passed_checks': passed_checks
            }
        }
    
    def build_violations_section(self) -> Dict[str, Any]:
        """
        Build Violations This Week section with detailed descriptions.
        Each violation includes: failing system, failing field, downstream impact.
        """
        violations_by_severity = {
            'CRITICAL': [],
            'HIGH': [],
            'MEDIUM': [],
            'LOW': []
        }
        
        # Map check IDs to system and field information
        violation_details = {
            'confidence.range': {
                'system': 'Week 3 Document Refinery',
                'field': 'extracted_facts[].confidence',
                'impact': 'Downstream consumers (Week 4 Cartographer, Week 2 Digital Courtroom) will misinterpret confidence values, causing incorrect filtering and verdict weighting.'
            },
            'confidence.type': {
                'system': 'Week 3 Document Refinery',
                'field': 'extracted_facts[].confidence',
                'impact': 'Type mismatch causes silent failures in all downstream consumers expecting float values.'
            },
            'doc_id.required': {
                'system': 'Week 3 Document Refinery',
                'field': 'doc_id',
                'impact': 'Missing document IDs break lineage tracking and event correlation in Week 4 Cartographer and Week 5 Event Sourcing.'
            },
            'doc_id.unique': {
                'system': 'Week 3 Document Refinery',
                'field': 'doc_id',
                'impact': 'Duplicate document IDs cause incorrect aggregation and duplicate events in downstream systems.'
            },
            'time_order': {
                'system': 'Week 5 Event Sourcing Platform',
                'field': 'recorded_at / occurred_at',
                'impact': 'Event ordering violations break event sourcing guarantees and audit trails.'
            },
            'sequence.order': {
                'system': 'Week 5 Event Sourcing Platform',
                'field': 'sequence_number',
                'impact': 'Sequence gaps or duplicates break event replay and state reconstruction.'
            }
        }
        
        for violation in self.violations:
            check_id = violation.get('check_id', 'unknown')
            severity = violation.get('severity', 'MEDIUM')
            
            details = violation_details.get(check_id, {
                'system': 'Unknown System',
                'field': violation.get('affected_fields', ['unknown'])[0],
                'impact': 'Review violation details for specific impact.'
            })
            
            violation_entry = {
                'check_id': check_id,
                'severity': severity,
                'message': violation.get('message', 'No message'),
                'records_affected': violation.get('records_failing', 0),
                'sample_values': violation.get('sample_failing', [])[:5],
                'failing_system': details['system'],
                'failing_field': details['field'],
                'downstream_impact': details['impact'],
                'detected_at': violation.get('detected_at', 'unknown')
            }
            
            violations_by_severity[severity].append(violation_entry)
        
        # Generate plain language descriptions of top 3 violations
        top_violations = []
        for v in self.violations[:3]:
            check_id = v.get('check_id', 'unknown')
            details = violation_details.get(check_id, {})
            
            plain_description = self._generate_violation_description(v, details)
            top_violations.append({
                'check_id': check_id,
                'severity': v.get('severity', 'UNKNOWN'),
                'plain_description': plain_description,
                'records_affected': v.get('records_failing', 0),
                'failing_system': details.get('system', 'Unknown'),
                'failing_field': details.get('field', 'Unknown')
            })
        
        return {
            'count': len(self.violations),
            'by_severity': {
                'CRITICAL': len(violations_by_severity['CRITICAL']),
                'HIGH': len(violations_by_severity['HIGH']),
                'MEDIUM': len(violations_by_severity['MEDIUM']),
                'LOW': len(violations_by_severity['LOW'])
            },
            'top_violations': top_violations,
            'all_violations': violations_by_severity
        }
    
    def _generate_violation_description(self, violation: Dict, details: Dict) -> str:
        """Generate plain language violation description"""
        check_id = violation.get('check_id', 'unknown')
        records = violation.get('records_failing', 0)
        
        if check_id == 'confidence.range':
            sample = violation.get('sample_failing', [])
            sample_str = ', '.join(str(s) for s in sample[:3])
            return f"The {details.get('system', 'extraction system')} is outputting {records} confidence values on a 0-100 scale (e.g., {sample_str}) instead of the required 0.0-1.0 scale. This will cause {details.get('impact', 'downstream failures')}"
        
        elif check_id == 'confidence.type':
            return f"The {details.get('system', 'extraction system')} is producing integer confidence values where floats are expected. This type mismatch causes {details.get('impact', 'silent failures')}"
        
        elif check_id == 'doc_id.required':
            return f"Found {records} records missing the doc_id field in {details.get('system', 'extraction data')}. This breaks {details.get('impact', 'downstream lineage tracking')}"
        
        else:
            return f"Violation {check_id} in {details.get('system', 'unknown system')}: {violation.get('message', 'No details')} - {details.get('impact', 'Affects downstream consumers')}"
    
    def build_schema_changes_section(self) -> Dict[str, Any]:
        """Build Schema Changes Detected section"""
        schema_changes = []
        
        # Compare snapshots if we have at least 2
        if len(self.schema_snapshots) >= 2:
            # Sort by timestamp
            sorted_snapshots = sorted(self.schema_snapshots, 
                                      key=lambda x: x.get('timestamp', ''))
            old = sorted_snapshots[-2]
            new = sorted_snapshots[-1]
            
            old_schema = old.get('schema', {})
            new_schema = new.get('schema', {})
            
            # Detect changes
            for field in set(list(old_schema.keys()) + list(new_schema.keys())):
                if field not in old_schema:
                    schema_changes.append({
                        'type': 'ADDED',
                        'field': field,
                        'old_value': None,
                        'new_value': new_schema.get(field),
                        'compatibility': 'COMPATIBLE' if new_schema.get(field, {}).get('nullable', False) else 'BREAKING'
                    })
                elif field not in new_schema:
                    schema_changes.append({
                        'type': 'REMOVED',
                        'field': field,
                        'old_value': old_schema.get(field),
                        'new_value': None,
                        'compatibility': 'BREAKING'
                    })
                elif old_schema[field] != new_schema[field]:
                    schema_changes.append({
                        'type': 'MODIFIED',
                        'field': field,
                        'old_value': old_schema[field],
                        'new_value': new_schema[field],
                        'compatibility': self._check_compatibility(old_schema[field], new_schema[field])
                    })
        
        return {
            'detected': len(schema_changes) > 0,
            'changes': schema_changes,
            'summary': f"Found {len(schema_changes)} schema change(s) in the monitored period."
        }
    
    def _check_compatibility(self, old: Dict, new: Dict) -> str:
        """Check if a schema change is compatible"""
        if old.get('type') != new.get('type'):
            return 'BREAKING'
        
        # Check range narrowing
        old_min = old.get('minimum')
        old_max = old.get('maximum')
        new_min = new.get('minimum')
        new_max = new.get('maximum')
        
        if new_min is not None and old_min is not None and new_min > old_min:
            return 'BREAKING'
        if new_max is not None and old_max is not None and new_max < old_max:
            return 'BREAKING'
        
        return 'COMPATIBLE'
    
    def build_ai_risk_assessment(self) -> Dict[str, Any]:
        """Build AI System Risk Assessment section"""
        embedding_drift = self.ai_metrics_data.get('embedding_drift', {})
        llm_output = self.ai_metrics_data.get('llm_output', {})
        
        risk_level = 'LOW'
        risk_factors = []
        
        # Assess embedding drift risk
        drift_score = embedding_drift.get('drift_score', 0)
        if drift_score > 0.15:
            risk_level = 'HIGH'
            risk_factors.append(f"Embedding drift high ({drift_score})")
        elif drift_score > 0.10:
            if risk_level != 'HIGH':
                risk_level = 'MEDIUM'
            risk_factors.append(f"Embedding drift elevated ({drift_score})")
        
        # Assess LLM output violation rate
        violation_rate = llm_output.get('violation_rate', 0)
        if violation_rate > 0.05:
            risk_level = 'HIGH'
            risk_factors.append(f"LLM output violation rate high ({violation_rate:.2%})")
        elif violation_rate > 0.02:
            if risk_level != 'HIGH':
                risk_level = 'MEDIUM'
            risk_factors.append(f"LLM output violation rate elevated ({violation_rate:.2%})")
        
        return {
            'overall_risk_level': risk_level,
            'risk_factors': risk_factors,
            'embedding_drift': {
                'score': drift_score,
                'threshold': 0.15,
                'status': embedding_drift.get('status', 'UNKNOWN'),
                'is_acceptable': drift_score <= 0.15
            },
            'llm_output': {
                'violation_rate': violation_rate,
                'threshold': 0.05,
                'status': llm_output.get('status', 'UNKNOWN'),
                'trend': llm_output.get('trend', 'stable'),
                'is_acceptable': violation_rate <= 0.05
            },
            'narrative': self._generate_ai_narrative(risk_level, risk_factors)
        }
    
    def _generate_ai_narrative(self, risk_level: str, risk_factors: List[str]) -> str:
        """Generate AI risk narrative"""
        if risk_level == 'HIGH':
            return f"⚠️ HIGH RISK: {', '.join(risk_factors)}. Immediate investigation required."
        elif risk_level == 'MEDIUM':
            return f"⚠️ MEDIUM RISK: {', '.join(risk_factors)}. Monitor closely and plan remediation."
        else:
            return "✅ LOW RISK: All AI systems operating within acceptable thresholds. Continue monitoring."
    
    def build_recommended_actions(self) -> List[Dict[str, Any]]:
        """
        Build Recommended Actions section with specific, actionable items.
        Each action includes exact file path and contract clause reference.
        """
        actions = []
        
        # Analyze violations to generate specific actions
        for violation in self.violations:
            check_id = violation.get('check_id', '')
            records = violation.get('records_failing', 0)
            
            if check_id == 'confidence.range':
                actions.append({
                    'priority': 1,
                    'action': f'Fix confidence scale in extraction pipeline',
                    'file_path': 'src/week3/extractor.py',
                    'line_reference': 'lines 150-160',
                    'contract_clause': 'week3-extractions-contract: schema.extracted_facts.items.confidence',
                    'specific_change': 'Change from: confidence = score * 100  # 0-100 scale → To: confidence = score  # 0.0-1.0 scale',
                    'details': f'Found {records} confidence values on 0-100 scale. Update extractor.py to output float in [0.0, 1.0].',
                    'risk_reduction': 'Eliminates CRITICAL violations affecting Week 4 Cartographer and Week 2 Digital Courtroom',
                    'owner': 'extraction-team',
                    'due_days': 3
                })
            
            elif check_id == 'confidence.type':
                actions.append({
                    'priority': 1,
                    'action': 'Convert confidence from integer to float type',
                    'file_path': 'src/week3/extractor.py',
                    'line_reference': 'line 156',
                    'contract_clause': 'week3-extractions-contract: quality.specification.checks.confidence_type',
                    'specific_change': 'Ensure confidence = float(score) instead of int(score * 100)',
                    'details': f'Found {records} integer confidence values. Update type casting to produce floats.',
                    'risk_reduction': 'Prevents type mismatch errors in downstream consumers',
                    'owner': 'extraction-team',
                    'due_days': 3
                })
            
            elif check_id == 'time_order':
                actions.append({
                    'priority': 2,
                    'action': 'Fix event timestamp ordering',
                    'file_path': 'src/week5/event_publisher.py',
                    'line_reference': 'lines 200-220',
                    'contract_clause': 'week5-events-contract: quality.specification.checks.time_order',
                    'specific_change': 'Ensure recorded_at is set AFTER occurred_at, not before',
                    'details': f'Found {records} events with recorded_at < occurred_at. Fix timestamp assignment order.',
                    'risk_reduction': 'Restores event sourcing immutability guarantees',
                    'owner': 'events-team',
                    'due_days': 5
                })
        
        # Add generic actions if no specific violations found
        if not actions:
            actions.append({
                'priority': 1,
                'action': 'Run full validation suite',
                'file_path': 'scripts/run_validation.py',
                'line_reference': 'all',
                'contract_clause': 'all active contracts',
                'specific_change': 'Execute python contracts/runner.py --all',
                'details': 'No violations detected, but proactive validation is recommended',
                'risk_reduction': 'Prevents future undetected schema drift',
                'owner': 'data-platform',
                'due_days': 7
            })
        
        # Add preventive action
        actions.append({
            'priority': 3,
            'action': 'Add pre-commit hook for contract validation',
            'file_path': '.pre-commit-config.yaml',
            'line_reference': 'new file',
            'contract_clause': 'all contracts',
            'specific_change': 'Add: - repo: local hooks: - id: contract-validation',
            'details': 'Prevent confidence scale changes from reaching production without validation',
            'risk_reduction': 'Catches violations before commit, preventing silent failures',
            'owner': 'devops',
            'due_days': 7
        })
        
        # Sort by priority and remove duplicates
        seen_actions = set()
        unique_actions = []
        for action in sorted(actions, key=lambda x: x['priority']):
            action_key = action['action']
            if action_key not in seen_actions:
                seen_actions.add(action_key)
                unique_actions.append(action)
        
        return unique_actions[:5]  # Return top 5 actions
    
    def generate_report(self) -> Dict[str, Any]:
        """
        Generate complete report with all required sections.
        Returns dictionary with all five sections.
        """
        # Load all data first
        self.load_data()
        
        # Calculate health score
        health = self.calculate_health_score()
        
        # Build all required sections
        violations_section = self.build_violations_section()
        schema_changes = self.build_schema_changes_section()
        ai_risk = self.build_ai_risk_assessment()
        recommendations = self.build_recommended_actions()
        
        # Construct complete report
        report = {
            'report_id': f"enforcer_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'generated_at': datetime.now().isoformat(),
            'data_health_score': health['score'],
            'data_health_narrative': health['narrative'],
            'health_calculation': health.get('calculation', {}),
            
            # Section 1: Data Health Score (already included above)
            
            # Section 2: Violations this week
            'violations_this_week': {
                'total_count': violations_section['count'],
                'by_severity': violations_section['by_severity'],
                'top_violations': violations_section['top_violations'],
                'all_violations_by_severity': violations_section['all_violations']
            },
            
            # Section 3: Schema changes detected
            'schema_changes_detected': schema_changes,
            
            # Section 4: AI system risk assessment
            'ai_risk_assessment': ai_risk,
            
            # Section 5: Recommended actions
            'recommended_actions': recommendations,
            
            # Metadata for completeness
            'metadata': {
                'validation_reports_processed': len(self.validation_reports),
                'violations_processed': len(self.violations),
                'schema_snapshots_processed': len(self.schema_snapshots),
                'ai_metrics_loaded': bool(self.ai_metrics_data)
            }
        }
        
        return report
    
    def save_report(self, report: Dict[str, Any]) -> Path:
        """Save report to JSON file"""
        output_file = self.output_dir / 'report_data.json'
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"✅ Enforcer report saved to {output_file}")
        return output_file
    
    def run(self) -> Dict[str, Any]:
        """Execute the report generation pipeline"""
        print("\n" + "=" * 60)
        print("📊 Generating Enforcer Report")
        print("=" * 60)
        
        print(f"   Validation reports from: {self.validation_dir}")
        print(f"   Violation log from: {self.violation_log}")
        print(f"   AI metrics from: {self.ai_metrics}")
        
        report = self.generate_report()
        self.save_report(report)
        
        # Print summary
        print(f"\n📈 Report Summary:")
        print(f"   Data Health Score: {report['data_health_score']}/100")
        print(f"   Total Violations: {report['violations_this_week']['total_count']}")
        print(f"   Critical Violations: {report['violations_this_week']['by_severity']['CRITICAL']}")
        print(f"   AI Risk Level: {report['ai_risk_assessment']['overall_risk_level']}")
        print(f"   Recommended Actions: {len(report['recommended_actions'])}")
        
        return report


def main():
    """Main entry point with command line argument support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Data Contract Enforcer Report')
    parser.add_argument('--validation-dir', default=None, 
                        help='Path to validation reports directory')
    parser.add_argument('--violation-log', default=None,
                        help='Path to violation log file')
    parser.add_argument('--ai-metrics', default=None,
                        help='Path to AI metrics file')
    parser.add_argument('--output', default=None,
                        help='Output directory for report')
    
    args = parser.parse_args()
    
    generator = ReportGenerator(
        validation_dir=args.validation_dir,
        violation_log=args.violation_log,
        ai_metrics=args.ai_metrics,
        output_dir=args.output
    )
    
    report = generator.run()
    
    # Exit with error if health score is too low
    if report['data_health_score'] < 50:
        sys.exit(1)


if __name__ == '__main__':
    main()