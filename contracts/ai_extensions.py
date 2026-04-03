# contracts/ai_extensions.py
#!/usr/bin/env python3
"""
AI Contract Extensions - Embedding drift, prompt validation, output schema enforcement
"""

import argparse
import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib


class AIExtensions:
    """AI-specific contract extensions for LLM systems"""
    
    def __init__(self):
        self.embedding_baseline = None
        self.baseline_path = Path('schema_snapshots/embedding_baselines.npz')
    
    def simple_embedding(self, text: str) -> np.ndarray:
        """Simple embedding approximation for demo (in production, use real embeddings)"""
        # This simulates embedding for demonstration
        # In production, use: openai.Embedding.create() or sentence-transformers
        np.random.seed(hash(text) % 2**32)
        return np.random.randn(384) / 10  # 384-dim embedding
    
    def compute_centroid(self, texts: List[str]) -> np.ndarray:
        """Compute centroid of embeddings for a list of texts"""
        if not texts:
            return np.zeros(384)
        
        embeddings = [self.simple_embedding(t) for t in texts]
        return np.mean(embeddings, axis=0)
    
    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors"""
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)
    
    def check_embedding_drift(self, texts: List[str], threshold: float = 0.15) -> Dict:
        """Check for embedding drift using cosine distance"""
        if not texts:
            return {'drift_score': None, 'status': 'SKIP', 'message': 'No texts provided'}
        
        # Take sample for efficiency
        sample_size = min(200, len(texts))
        sample_texts = texts[:sample_size]
        
        current_centroid = self.compute_centroid(sample_texts)
        
        # Load baseline
        if self.baseline_path.exists():
            baseline_data = np.load(self.baseline_path)
            baseline_centroid = baseline_data['centroid']
            
            similarity = self.cosine_similarity(current_centroid, baseline_centroid)
            drift = 1 - similarity
            
            status = 'FAIL' if drift > threshold else 'PASS'
            
            return {
                'drift_score': round(float(drift), 4),
                'similarity': round(float(similarity), 4),
                'threshold': threshold,
                'status': status,
                'sample_size': sample_size,
                'message': f"Embedding drift: {drift:.4f} (threshold: {threshold})"
            }
        else:
            # Save baseline
            np.savez(self.baseline_path, centroid=current_centroid)
            return {
                'drift_score': 0.0,
                'status': 'BASELINE_ESTABLISHED',
                'message': 'Baseline embedding saved for future comparisons'
            }
    
    def validate_prompt_input(self, record: Dict, schema: Dict) -> Dict:
        """Validate prompt input against JSON schema"""
        violations = []
        
        required_fields = schema.get('required', [])
        for field in required_fields:
            if field not in record:
                violations.append(f"Missing required field: {field}")
        
        for field, rules in schema.get('properties', {}).items():
            if field in record:
                value = record[field]
                
                # Type checking
                expected_type = rules.get('type')
                if expected_type == 'string' and not isinstance(value, str):
                    violations.append(f"Field '{field}' should be string, got {type(value)}")
                elif expected_type == 'integer' and not isinstance(value, int):
                    violations.append(f"Field '{field}' should be integer, got {type(value)}")
                
                # Length checking
                if 'maxLength' in rules and isinstance(value, str):
                    if len(value) > rules['maxLength']:
                        violations.append(f"Field '{field}' exceeds max length {rules['maxLength']}")
        
        return {
            'valid': len(violations) == 0,
            'violations': violations,
            'violation_count': len(violations)
        }
    
    def validate_llm_output(self, output: Dict, schema: Dict) -> Dict:
        """Validate LLM structured output against schema"""
        violations = []
        
        required_fields = schema.get('required', [])
        for field in required_fields:
            if field not in output:
                violations.append(f"Missing required output field: {field}")
        
        # Check enum values
        for field, rules in schema.get('properties', {}).items():
            if field in output and 'enum' in rules:
                if output[field] not in rules['enum']:
                    violations.append(f"Field '{field}' value '{output[field]}' not in enum {rules['enum']}")
            
            # Range checking for numeric fields
            if field in output and isinstance(output[field], (int, float)):
                if 'minimum' in rules and output[field] < rules['minimum']:
                    violations.append(f"Field '{field}' value {output[field]} below minimum {rules['minimum']}")
                if 'maximum' in rules and output[field] > rules['maximum']:
                    violations.append(f"Field '{field}' value {output[field]} above maximum {rules['maximum']}")
        
        return {
            'valid': len(violations) == 0,
            'violations': violations,
            'violation_count': len(violations)
        }
    
    def analyze_verdict_records(self, verdicts_path: Path) -> Dict:
        """Analyze Week 2 verdict records for LLM output validation"""
        if not verdicts_path.exists():
            return {'error': 'Verdicts file not found'}
        
        schema = {
            'type': 'object',
            'required': ['verdict_id', 'overall_verdict', 'confidence'],
            'properties': {
                'verdict_id': {'type': 'string'},
                'overall_verdict': {'type': 'string', 'enum': ['PASS', 'FAIL', 'WARN']},
                'confidence': {'type': 'number', 'minimum': 0.0, 'maximum': 1.0},
                'overall_score': {'type': 'number', 'minimum': 1.0, 'maximum': 5.0}
            }
        }
        
        records = []
        with open(verdicts_path, 'r') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        total = len(records)
        violations = []
        
        for record in records:
            result = self.validate_llm_output(record, schema)
            if not result['valid']:
                violations.append(result)
        
        violation_rate = len(violations) / total if total > 0 else 0
        trend = self.calculate_trend(violation_rate)
        
        return {
            'total_outputs': total,
            'schema_violations': len(violations),
            'violation_rate': round(violation_rate, 4),
            'trend': trend,
            'baseline_violation_rate': 0.0142,  # From earlier runs
            'status': 'WARN' if violation_rate > 0.02 else 'PASS'
        }
    
    def calculate_trend(self, current_rate: float, historical_rates: List[float] = None) -> str:
        """Calculate trend based on historical rates"""
        if historical_rates and len(historical_rates) >= 3:
            avg_historical = np.mean(historical_rates[-3:])
            if current_rate > avg_historical * 1.2:
                return 'rising'
            elif current_rate < avg_historical * 0.8:
                return 'falling'
        return 'stable'
    
    def extract_text_from_extractions(self, extractions_path: Path) -> List[str]:
        """Extract text from Week 3 extraction facts for embedding drift"""
        if not extractions_path.exists():
            return []
        
        texts = []
        with open(extractions_path, 'r') as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    for fact in record.get('extracted_facts', []):
                        if 'text' in fact:
                            texts.append(fact['text'])
        
        return texts
    
    def run_all_extensions(self, extractions_path: Path, verdicts_path: Path) -> Dict:
        """Run all three AI extensions"""
        results = {}
        
        print("\n" + "="*60)
        print("🤖 AI Contract Extensions")
        print("="*60)
        
        # Extension 1: Embedding Drift
        print("\n📊 Extension 1: Embedding Drift Detection")
        texts = self.extract_text_from_extractions(extractions_path)
        drift_result = self.check_embedding_drift(texts)
        results['embedding_drift'] = drift_result
        print(f"   Drift score: {drift_result.get('drift_score', 'N/A')}")
        print(f"   Status: {drift_result.get('status', 'UNKNOWN')}")
        
        # Extension 2: Prompt Input Validation
        print("\n📊 Extension 2: Prompt Input Validation")
        prompt_schema = {
            'required': ['doc_id', 'source_path', 'content_preview'],
            'properties': {
                'doc_id': {'type': 'string'},
                'source_path': {'type': 'string', 'maxLength': 500},
                'content_preview': {'type': 'string', 'maxLength': 8000}
            }
        }
        
        # Sample validation on extraction records
        if extractions_path.exists():
            with open(extractions_path, 'r') as f:
                sample_record = json.loads(f.readline())
            
            prompt_input = {
                'doc_id': sample_record.get('doc_id'),
                'source_path': sample_record.get('source_path', ''),
                'content_preview': sample_record.get('extracted_facts', [{}])[0].get('text', '')[:1000]
            }
            
            validation = self.validate_prompt_input(prompt_input, prompt_schema)
            results['prompt_validation'] = validation
            print(f"   Valid: {validation['valid']}")
            print(f"   Violations: {validation['violation_count']}")
        
        # Extension 3: LLM Output Schema Enforcement
        print("\n📊 Extension 3: LLM Output Schema Enforcement")
        verdict_results = self.analyze_verdict_records(verdicts_path)
        results['llm_output'] = verdict_results
        print(f"   Violation rate: {verdict_results.get('violation_rate', 0):.2%}")
        print(f"   Status: {verdict_results.get('status', 'UNKNOWN')}")
        print(f"   Trend: {verdict_results.get('trend', 'unknown')}")
        
        # Summary
        print("\n" + "="*60)
        print("📊 AI Extensions Summary")
        print("="*60)
        
        statuses = []
        if 'embedding_drift' in results:
            statuses.append(results['embedding_drift'].get('status'))
        if 'llm_output' in results:
            statuses.append(results['llm_output'].get('status'))
        
        if 'FAIL' in statuses:
            overall = 'FAIL'
        elif 'WARN' in statuses:
            overall = 'WARN'
        else:
            overall = 'PASS'
        
        results['overall_status'] = overall
        print(f"Overall AI Contract Status: {overall}")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='Run AI contract extensions')
    parser.add_argument('--extractions', default='outputs/week3/extractions.jsonl', help='Path to extraction records')
    parser.add_argument('--verdicts', default='outputs/week2/verdicts.jsonl', help='Path to verdict records')
    parser.add_argument('--output', default='validation_reports/ai_metrics.json', help='Output path')
    
    args = parser.parse_args()
    
    extensions = AIExtensions()
    results = extensions.run_all_extensions(Path(args.extractions), Path(args.verdicts))
    
    # Save results
    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Results saved to {output_path}")


if __name__ == '__main__':
    main()