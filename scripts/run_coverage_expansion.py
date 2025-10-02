#!/usr/bin/env python3
"""
Coverage Expansion Pipeline Orchestrator

Executes Steps 0-5 of the coverage expansion pipeline end-to-end.
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

# Pipeline steps
STEPS = [
    {
        'name': 'Step 0: Preflight & Baseline',
        'script': 'scripts/coverage_expansion_step0_preflight.py',
        'description': 'Validate environment and establish baseline metrics'
    },
    {
        'name': 'Step 1: Sitemap-First Discovery',
        'script': 'scripts/coverage_expansion_step1_discovery.py',
        'description': 'Discover URLs from sitemaps and listings'
    },
    {
        'name': 'Step 2: Canonical Doc Creation',
        'script': 'scripts/coverage_expansion_step2_canonical.py',
        'description': 'Create canonical documents using Firecrawl'
    },
    {
        'name': 'Step 3: Micro-Enrichment',
        'script': 'scripts/coverage_expansion_step3_micro_enrich.py',
        'description': 'Enrich documents using OpenAI Batch API'
    },
    {
        'name': 'Step 4: QA & KPIs',
        'script': 'scripts/coverage_expansion_step4_qa_kpis.py',
        'description': 'Run quality checks and compute coverage metrics'
    },
    {
        'name': 'Step 5: Sales-Ready Pack',
        'script': 'scripts/coverage_expansion_step5_sales_pack.py',
        'description': 'Create sales-ready dataset and documentation'
    }
]

OUTPUT_DIR = "data/output/validation/latest"


def run_step(step_info):
    """Run a single pipeline step."""
    print(f"Starting {step_info['name']}...")
    print(f"Description: {step_info['description']}")
    print()
    
    start_time = time.time()
    
    try:
        # Run the step script
        result = subprocess.run([
            '.venv/bin/python', step_info['script']
        ], capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✓ {step_info['name']} completed successfully in {duration:.1f}s")
            return True, result.stdout, result.stderr, duration
        else:
            print(f"✗ {step_info['name']} failed with return code {result.returncode}")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False, result.stdout, result.stderr, duration
            
    except subprocess.TimeoutExpired:
        print(f"✗ {step_info['name']} timed out after 1 hour")
        return False, "", "Timeout", time.time() - start_time
    except Exception as e:
        print(f"✗ {step_info['name']} failed with exception: {e}")
        return False, "", str(e), time.time() - start_time


def main():
    print("=" * 80)
    print("ASEANFORGE COVERAGE EXPANSION PIPELINE")
    print("=" * 80)
    print()
    print("Target: Push corpus from 'working' to 'sellable' status")
    print("Goals:")
    print("  → Global doc completeness: ≥80%")
    print("  → Per-authority: ≥75% OR +30pp improvement")
    print("  → 90-day freshness: ≥85%")
    print("  → Net-new docs: ≥200 created")
    print()
    
    pipeline_start = time.time()
    results = []
    
    # Execute each step
    for i, step_info in enumerate(STEPS):
        print(f"[{i+1}/{len(STEPS)}] {step_info['name']}")
        print("-" * 60)
        
        success, stdout, stderr, duration = run_step(step_info)
        
        step_result = {
            'step_number': i + 1,
            'step_name': step_info['name'],
            'success': success,
            'duration_seconds': duration,
            'stdout': stdout,
            'stderr': stderr,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        results.append(step_result)
        
        if not success:
            print()
            print(f"Pipeline failed at {step_info['name']}")
            break
        
        print()
        print("=" * 60)
        print()
    
    # Calculate total duration
    total_duration = time.time() - pipeline_start
    
    # Generate final report
    successful_steps = sum(1 for r in results if r['success'])
    
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Steps completed: {successful_steps}/{len(STEPS)}")
    print(f"Total duration: {total_duration/60:.1f} minutes")
    print()
    
    for result in results:
        status = "✓ PASS" if result['success'] else "✗ FAIL"
        print(f"  {result['step_name']}: {status} ({result['duration_seconds']:.1f}s)")
    
    print()
    
    # Load final metrics if available
    qa_report_file = os.path.join(OUTPUT_DIR, "expansion_qa_kpis_report.json")
    if os.path.exists(qa_report_file):
        with open(qa_report_file, 'r') as f:
            qa_report = json.load(f)
        
        current_metrics = qa_report['current_metrics']
        baseline_metrics = qa_report['baseline_metrics']
        
        print("FINAL METRICS")
        print("-" * 40)
        print(f"Global doc completeness: {baseline_metrics['global_metrics']['doc_completeness_pct']:.1f}% → {current_metrics['global']['doc_completeness_pct']:.1f}%")
        print(f"90-day freshness: {baseline_metrics['freshness_metrics']['doc_completeness_90d_pct']:.1f}% → {current_metrics['freshness']['doc_completeness_90d_pct']:.1f}%")
        
        if qa_report.get('targets_met', False):
            print("🎉 All targets achieved!")
        else:
            print("⚠️  Some targets not met:")
            for failure in qa_report.get('failures', []):
                print(f"    - {failure}")
        print()
    
    # Check for snapshot
    snapshot_file = os.path.join(OUTPUT_DIR, "snapshot_path.txt")
    if os.path.exists(snapshot_file):
        with open(snapshot_file, 'r') as f:
            snapshot_path = f.read().strip()
        print(f"📦 Sales pack created: {os.path.basename(snapshot_path)}")
        print()
    
    # Write execution log
    execution_log = {
        'pipeline_name': 'coverage_expansion',
        'start_time': datetime.fromtimestamp(pipeline_start, timezone.utc).isoformat(),
        'end_time': datetime.now(timezone.utc).isoformat(),
        'total_duration_seconds': total_duration,
        'successful_steps': successful_steps,
        'total_steps': len(STEPS),
        'pipeline_success': successful_steps == len(STEPS),
        'step_results': results
    }
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "pipeline_execution_log.json"), 'w') as f:
        json.dump(execution_log, f, indent=2)
    
    # Exit with appropriate code
    if successful_steps == len(STEPS):
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        sys.exit(0)
    else:
        print("❌ PIPELINE FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
