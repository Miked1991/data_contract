# contracts/attributor.py
#!/usr/bin/env python3
"""
ViolationAttributor - Traces violations back to source commits using lineage graph
"""

import argparse
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import deque
import hashlib


class ViolationAttributor:
    """Traces validation failures to source commits using Week 4 lineage graph"""
    
    def __init__(self, violation_log_path: str, lineage_graph_path: str, repo_path: str = "."):
        self.violation_log_path = Path(violation_log_path)
        self.lineage_graph_path = Path(lineage_graph_path)
        self.repo_path = Path(repo_path)
        self.lineage_graph = None
        
    def load_lineage_graph(self) -> Dict:
        """Load Week 4 lineage graph"""
        if self.lineage_graph_path.exists():
            with open(self.lineage_graph_path, 'r') as f:
                # Read first record as lineage snapshot
                first_line = f.readline()
                if first_line:
                    self.lineage_graph = json.loads(first_line)
                    return self.lineage_graph
        return None
    
    def find_upstream_nodes(self, target_field: str) -> List[str]:
        """Traverse lineage graph to find upstream nodes"""
        if not self.lineage_graph:
            return []
        
        nodes = self.lineage_graph.get('nodes', [])
        edges = self.lineage_graph.get('edges', [])
        
        # Build adjacency list (reverse for upstream traversal)
        upstream_map = {}
        for edge in edges:
            target = edge.get('target')
            source = edge.get('source')
            if target not in upstream_map:
                upstream_map[target] = []
            upstream_map[target].append(source)
        
        # Find node containing target field
        start_nodes = []
        for node in nodes:
            metadata = node.get('metadata', {})
            if target_field in str(metadata) or target_field in node.get('label', ''):
                start_nodes.append(node.get('node_id'))
        
        # BFS upstream
        visited = set()
        queue = deque(start_nodes)
        upstream_files = []
        
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            
            # Check if this is a source file
            if node.startswith('file::') and 'py' in node:
                file_path = node.replace('file::', '')
                upstream_files.append(file_path)
            
            # Add upstream dependencies
            for upstream in upstream_map.get(node, []):
                if upstream not in visited:
                    queue.append(upstream)
        
        return upstream_files
    
    def git_blame(self, file_path: str, line_number: int = None) -> Dict:
        """Run git blame to find commit information"""
        try:
            if line_number:
                cmd = ['git', 'blame', '-L', f'{line_number},{line_number}', '--porcelain', file_path]
            else:
                cmd = ['git', 'blame', '--porcelain', file_path]
            
            result = subprocess.run(cmd, cwd=self.repo_path, capture_output=True, text=True)
            
            if result.returncode == 0 and result.stdout:
                lines = result.stdout.split('\n')
                for line in lines:
                    if line.startswith('author '):
                        author = line.replace('author ', '')
                    elif line.startswith('author-mail '):
                        email = line.replace('author-mail ', '')
                    elif line.startswith('author-time '):
                        timestamp = int(line.replace('author-time ', ''))
                    elif line.startswith('summary '):
                        summary = line.replace('summary ', '')
                
                commit_hash = result.stdout.split('\n')[0].split()[0] if result.stdout else 'unknown'
                
                return {
                    'commit_hash': commit_hash[:40],
                    'author': author if 'author' in locals() else 'unknown',
                    'email': email if 'email' in locals() else 'unknown',
                    'timestamp': datetime.fromtimestamp(timestamp).isoformat() if 'timestamp' in locals() else None,
                    'message': summary if 'summary' in locals() else 'unknown'
                }
        except Exception as e:
            print(f"⚠️  Git blame failed for {file_path}: {e}")
        
        return None
    
    def calculate_confidence_score(self, commit_date: datetime, hop_distance: int) -> float:
        """Calculate confidence score based on recency and distance"""
        days_since = (datetime.now() - commit_date).days if commit_date else 30
        base_score = max(0.5, 1.0 - (days_since * 0.02))
        hop_penalty = hop_distance * 0.15
        return round(max(0.5, base_score - hop_penalty), 2)
    
    def compute_blast_radius(self, field_name: str) -> Dict:
        """Compute downstream impact using lineage graph"""
        if not self.lineage_graph:
            return {'affected_nodes': [], 'affected_pipelines': [], 'estimated_records': 0}
        
        nodes = self.lineage_graph.get('nodes', [])
        edges = self.lineage_graph.get('edges', [])
        
        # Find nodes consuming this field
        affected_nodes = []
        for edge in edges:
            if edge.get('relationship') in ['CONSUMES', 'READS']:
                if field_name in str(edge):
                    target = edge.get('target')
                    if target:
                        affected_nodes.append(target)
        
        affected_pipelines = list(set([n.split('::')[0] for n in affected_nodes if '::' in n]))
        
        return {
            'affected_nodes': affected_nodes[:10],
            'affected_pipelines': affected_pipelines,
            'estimated_records': len(affected_nodes) * 5  # Estimate
        }
    
    def attribute_violation(self, violation: Dict) -> Dict:
        """Generate blame chain for a single violation"""
        check_id = violation.get('check_id', 'unknown')
        field_name = violation.get('column_name', 'confidence')
        
        # Find upstream files
        upstream_files = self.find_upstream_nodes(field_name)
        
        blame_chain = []
        for hop, file_path in enumerate(upstream_files[:3]):  # Limit to 3 candidates
            blame_info = self.git_blame(file_path)
            if blame_info:
                commit_date = datetime.fromisoformat(blame_info['timestamp']) if blame_info.get('timestamp') else None
                confidence = self.calculate_confidence_score(commit_date, hop)
                
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
        
        blast_radius = self.compute_blast_radius(field_name)
        
        return {
            'violation_id': violation.get('violation_id', str(hash(json.dumps(violation)))),
            'check_id': check_id,
            'detected_at': datetime.now().isoformat(),
            'blame_chain': blame_chain,
            'blast_radius': blast_radius
        }
    
    def process_violation_log(self) -> List[Dict]:
        """Process all violations in the log"""
        if not self.violation_log_path.exists():
            print(f"❌ Violation log not found: {self.violation_log_path}")
            return []
        
        self.load_lineage_graph()
        
        attributions = []
        with open(self.violation_log_path, 'r') as f:
            for line in f:
                if line.strip():
                    violation = json.loads(line)
                    attribution = self.attribute_violation(violation)
                    attributions.append(attribution)
                    
                    print(f"\n🔍 Attributed violation: {violation.get('check_id')}")
                    if attribution['blame_chain']:
                        top = attribution['blame_chain'][0]
                        print(f"   → Commit: {top['commit_hash'][:8]} by {top['author']}")
                        print(f"   → Confidence: {top['confidence_score']}")
                    print(f"   → Blast radius: {len(attribution['blast_radius']['affected_nodes'])} nodes")
        
        return attributions
    
    def save_attributions(self, attributions: List[Dict]):
        """Save attributions to file"""
        output_path = Path('violation_log/attributions.json')
        with open(output_path, 'w') as f:
            json.dump(attributions, f, indent=2)
        print(f"\n💾 Attributions saved to {output_path}")
        return output_path


def main():
    parser = argparse.ArgumentParser(description='Attribute violations to source commits')
    parser.add_argument('--violation-log', default='violation_log/violations.jsonl', help='Path to violation log')
    parser.add_argument('--lineage-graph', default='outputs/week4/lineage_snapshots.jsonl', help='Path to lineage graph')
    parser.add_argument('--repo-path', default='.', help='Path to git repository')
    
    args = parser.parse_args()
    
    attributor = ViolationAttributor(args.violation_log, args.lineage_graph, args.repo_path)
    attributions = attributor.process_violation_log()
    attributor.save_attributions(attributions)
    
    # Exit with error if no attributions found
    if not attributions:
        exit(1)


if __name__ == '__main__':
    main()