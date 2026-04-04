# contracts/attributor.py (UPDATED)
#!/usr/bin/env python3
"""
Violation Attributor with Registry Integration and Per-Consumer Analysis
"""

import argparse
import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from registry import ContractRegistry


class AttributorWithRegistry:
    """Attributor with registry-driven consumer analysis"""
    
    def __init__(self, violation_log_path: str):
        self.violation_log_path = Path(violation_log_path)
        self.registry = ContractRegistry()
    
    def attribute_violations(self) -> List[Dict]:
        """Attribute violations with consumer impact"""
        
        attributions = []
        
        with open(self.violation_log_path, 'r') as f:
            for line in f:
                if line.strip():
                    violation = json.loads(line)
                    attribution = self._attribute_single(violation)
                    attributions.append(attribution)
        
        return attributions
    
    def _attribute_single(self, violation: Dict) -> Dict:
        """Attribute a single violation"""
        
        contract_id = violation.get('contract_id', 'unknown')
        
        # Get contract from registry
        contract = self.registry.get_contract(contract_id)
        
        # Get consumer impact analysis
        impact = self.registry.get_consumer_impact_analysis(contract_id, violation)
        
        # Build blame chain
        blame_chain = self._build_blame_chain(violation, contract)
        
        # Get enforcement action
        enforcement_mode = self.registry.get_enforcement_mode(contract_id)
        
        return {
            'violation_id': violation.get('violation_id'),
            'contract_id': contract_id,
            'detected_at': datetime.now().isoformat(),
            'blame_chain': blame_chain,
            'consumer_impact': impact,
            'recommended_action': self._get_recommended_action(impact, enforcement_mode),
            'enforcement_mode': enforcement_mode.value
        }
    
    def _build_blame_chain(self, violation: Dict, contract: Dict) -> List[Dict]:
        """Build blame chain for violation"""
        
        # In production, this would use git blame and lineage graph
        # For demo, return simulated blame chain
        
        return [
            {
                'rank': 1,
                'file_path': 'src/extractor.py',
                'commit_hash': 'abc1234def56789',
                'author': 'extraction-team@example.com',
                'commit_timestamp': '2025-03-15T14:23:00',
                'commit_message': 'feat: change confidence to percentage scale',
                'confidence_score': 0.85,
                'hop_distance': 0
            },
            {
                'rank': 2,
                'file_path': 'src/confidence_scorer.py',
                'commit_hash': 'def56789abc1234',
                'author': 'ml-team@example.com',
                'commit_timestamp': '2025-03-14T10:00:00',
                'commit_message': 'refactor: update scoring logic',
                'confidence_score': 0.72,
                'hop_distance': 1
            }
        ]
    
    def _get_recommended_action(self, impact: Dict, enforcement_mode) -> str:
        """Get recommended action based on impact"""
        
        if impact.get('requires_rollback'):
            return "IMMEDIATE_ROLLBACK_REQUIRED"
        elif len(impact.get('affected_consumers', [])) > 3:
            return "CRITICAL_HIGH_IMPACT"
        elif len(impact.get('affected_consumers', [])) > 0:
            return "NOTIFY_CONSUMERS"
        else:
            return "LOG_ONLY"
    
    def save_attributions(self, attributions: List[Dict]):
        """Save attributions with consumer analysis"""
        
        output_path = Path('violation_log/attributions_with_impact.json')
        with open(output_path, 'w') as f:
            json.dump(attributions, f, indent=2)
        
        print(f"✅ Attributions saved to {output_path}")
        
        # Print summary
        print("\n📊 Attribution Summary:")
        for attr in attributions:
            print(f"\n   Violation: {attr['violation_id']}")
            print(f"   Contract: {attr['contract_id']}")
            print(f"   Enforcement Mode: {attr['enforcement_mode'].upper()}")
            print(f"   Recommended Action: {attr['recommended_action']}")
            print(f"   Affected Consumers: {len(attr['consumer_impact'].get('affected_consumers', []))}")
            
            if attr['blame_chain']:
                top = attr['blame_chain'][0]
                print(f"   Blamed Commit: {top['commit_hash'][:8]} by {top['author']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--violation-log', default='violation_log/violations.jsonl')
    parser.add_argument('--with-consumer-analysis', action='store_true', default=True)
    
    args = parser.parse_args()
    
    attributor = AttributorWithRegistry(args.violation_log)
    attributions = attributor.attribute_violations()
    attributor.save_attributions(attributions)


if __name__ == '__main__':
    main()