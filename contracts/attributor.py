# contracts/attributor.py
#!/usr/bin/env python3
"""
Complete Violation Attributor - Uses real lineage graph and git blame
"""

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import deque


class ViolationAttributor:
    """Complete attribution using lineage graph and git blame"""
    
    def __init__(self, violation_log: str, lineage_graph: str, repo_path: str = "."):
        self.violation_log = Path(violation_log)
        self.lineage_graph_path = Path(lineage_graph)
        self.repo_path = Path(repo_path)
        self.lineage = None
        self.attributions = []
        
        from contracts.registry import ContractRegistry
        self.registry = ContractRegistry()
    
    def load_lineage(self) -> Optional[Dict]:
        """Load lineage graph from Week 4 output"""
        if self.lineage_graph_path.exists():
            with open(self.lineage_graph_path, 'r') as f:
                for line in f:
                    if line.strip():
                        self.lineage = json.loads(line)
                        return self.lineage
        return None
    
    def traverse_upstream(self, field_name: str) -> List[str]:
        """Traverse lineage graph to find upstream files"""
        if not self.lineage:
            return []
        
        nodes = self.lineage.get('nodes', [])
        edges = self.lineage.get('edges', [])
        
        # Build reverse adjacency
        upstream_map = {}
        for edge in edges:
            target = edge.get('target')
            source = edge.get('source')
            if target not in upstream_map:
                upstream_map[target] = []
            upstream_map[target].append(source)
        
        # Find nodes containing the field
        start_nodes = []
        for node in nodes:
            node_id = node.get('node_id', '')
            metadata = node.get('metadata', {})
            label = node.get('label', '')
            
            if field_name in str(metadata) or field_name in label:
                start_nodes.append(node_id)
        
        # BFS upstream
        visited = set()
        queue = deque(start_nodes)
        upstream_files = []
        
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            
            if node.startswith('file::') and ('.py' in node or '.java' in node):
                file_path = node.replace('file::', '')
                upstream_files.append(file_path)
            
            for upstream in upstream_map.get(node, []):
                if upstream not in visited:
                    queue.append(upstream)
        
        return upstream_files
    
    def git_blame(self, file_path: str, lines: tuple = None) -> Optional[Dict]:
        """Run git blame to find commit information"""
        try:
            if lines:
                cmd = ['git', 'blame', '-L', f"{lines[0]},{lines[1]}", '--porcelain', file_path]
            else:
                cmd = ['git', 'blame', '--porcelain', file_path]
            
            result = subprocess.run(cmd, cwd=self.repo_path, capture_output=True, text=True)
            
            if result.returncode == 0 and result.stdout:
                lines_out = result.stdout.split('\n')
                commit_hash = None
                author = None
                email = None
                timestamp = None
                message = None
                
                for line in lines_out:
                    if line.startswith('author '):
                        author = line.replace('author ', '')
                    elif line.startswith('author-mail '):
                        email = line.replace('author-mail ', '')
                    elif line.startswith('author-time '):
                        timestamp = int(line.replace('author-time ', ''))
                    elif line.startswith('summary '):
                        message = line.replace('summary ', '')
                    elif not commit_hash and line and not line.startswith(' '):
                        commit_hash = line.split()[0]
                
                return {
                    'commit_hash': commit_hash[:40] if commit_hash else 'unknown',
                    'author': author or 'unknown',
                    'email': email or 'unknown',
                    'timestamp': datetime.fromtimestamp(timestamp).isoformat() if timestamp else None,
                    'message': message or 'unknown'
                }
        except Exception as e:
            pass
        return None
    
    def calculate_confidence(self, days_since: int, hop_distance: int) -> float:
        """Calculate confidence score for attribution"""
        base = max(0.5, 1.0 - (days_since * 0.02))
        hop_penalty = hop_distance * 0.15
        return round(max(0.5, base - hop_penalty), 2)
    
    def get_blast_radius(self, field_name: str) -> Dict:
        """Calculate blast radius from lineage graph"""
        if not self.lineage:
            return {'affected_nodes': [], 'affected_consumers': [], 'estimated_records': 0}
        
        # Get affected consumers from registry
        affected_consumers = self.registry.get_affected_consumers('week3-document-refinery-extractions', [field_name])
        
        return {
            'affected_nodes': [c['consumer_name'] for c in affected_consumers],
            'affected_consumers': affected_consumers,
            'estimated_records': len(affected_consumers) * 5
        }
    
    def attribute_violation(self, violation: Dict) -> Dict:
        """Attribute a single violation"""
        field_name = violation.get('affected_fields', ['confidence'])[0]
        
        # Find upstream files
        upstream_files = self.traverse_upstream(field_name)
        
        blame_chain = []
        for hop, file_path in enumerate(upstream_files[:3]):
            blame_info = self.git_blame(file_path)
            if blame_info:
                commit_date = datetime.fromisoformat(blame_info['timestamp']) if blame_info.get('timestamp') else None
                days_since = (datetime.now() - commit_date).days if commit_date else 30
                confidence = self.calculate_confidence(days_since, hop)
                
                blame_chain.append({
                    'rank': hop + 1,
                    'file_path': file_path,
                    'commit_hash': blame_info['commit_hash'],
                    'author': blame_info['author'],
                    'email': blame_info['email'],
                    'commit_timestamp': blame_info['timestamp'],
                    'commit_message': blame_info['message'],
                    'confidence_score': confidence,
                    'hop_distance': hop
                })
        
        blast_radius = self.get_blast_radius(field_name)
        
        return {
            'violation_id': violation.get('violation_id'),
            'check_id': violation.get('check_id'),
            'detected_at': datetime.now().isoformat(),
            'blame_chain': blame_chain,
            'blast_radius': blast_radius,
            'recommended_action': self._get_action(blame_chain, blast_radius)
        }
    
    def _get_action(self, blame_chain: List, blast_radius: Dict) -> str:
        """Determine recommended action based on attribution"""
        if not blame_chain:
            return "MANUAL_INVESTIGATION_REQUIRED"
        
        top_score = blame_chain[0]['confidence_score']
        affected_count = len(blast_radius.get('affected_consumers', []))
        
        if top_score > 0.8 and affected_count > 2:
            return "ROLLBACK_RECOMMENDED"
        elif top_score > 0.7:
            return "NOTIFY_CONSUMERS"
        else:
            return "LOG_FOR_AUDIT"
    
    def process_violations(self) -> List[Dict]:
        """Process all violations in log"""
        if not self.violation_log.exists():
            print(f"❌ Violation log not found: {self.violation_log}")
            return []
        
        self.load_lineage()
        
        with open(self.violation_log, 'r') as f:
            for line in f:
                if line.strip():
                    violation = json.loads(line)
                    attribution = self.attribute_violation(violation)
                    self.attributions.append(attribution)
                    
                    print(f"\n🔍 Attributed: {violation.get('check_id')}")
                    if attribution['blame_chain']:
                        top = attribution['blame_chain'][0]
                        print(f"   → Commit: {top['commit_hash'][:8]} by {top['author']}")
                        print(f"   → Confidence: {top['confidence_score']}")
                    print(f"   → Affected consumers: {len(attribution['blast_radius']['affected_consumers'])}")
        
        return self.attributions
    
    def save_attributions(self):
        """Save attributions to file"""
        output_path = Path('violation_log/attributions.json')
        with open(output_path, 'w') as f:
            json.dump(self.attributions, f, indent=2)
        print(f"\n💾 Attributions saved to {output_path}")
        return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--violation-log', default='violation_log/violations.jsonl')
    parser.add_argument('--lineage-graph', default='outputs/week4/lineage_snapshots.jsonl')
    parser.add_argument('--repo-path', default='.')
    
    args = parser.parse_args()
    
    attributor = ViolationAttributor(args.violation_log, args.lineage_graph, args.repo_path)
    attributor.process_violations()
    attributor.save_attributions()


if __name__ == '__main__':
    main()