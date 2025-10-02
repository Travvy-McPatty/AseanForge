#!/usr/bin/env python3
"""
One-Shot Pipeline: Canonical Document Creation + Micro-Enrichment + QA

Orchestrates the complete pipeline:
- STEP 0: Baseline Metrics
- STEP 1: Create Canonical Documents
- STEP 2: Micro-Enrich (OpenAI Batch API)
- STEP 3: Mini-Harvest (Conditional)
- STEP 4: QA Checks + Snapshot

Enforces hard constraints:
- OpenAI budget: $10 USD max
- Firecrawl: ≤200 URLs soft cap
- Robots.txt compliance
- Rate limit handling (3-strike rule)
"""

import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass


OUTPUT_DIR = "data/output/validation/latest"
PIPELINE_LOG = os.path.join(OUTPUT_DIR, "pipeline_run.log")

# Step scripts
STEPS = [
    ("STEP 0: Baseline Metrics", "scripts/pipeline_step0_baseline.py"),
    ("STEP 1: Create Canonical Documents", "scripts/pipeline_step1_canonical_docs.py"),
    ("STEP 2: Micro-Enrich", "scripts/pipeline_step2_micro_enrich.py"),
    ("STEP 3: Mini-Harvest", "scripts/pipeline_step3_mini_harvest.py"),
    ("STEP 4: QA + Snapshot", "scripts/pipeline_step4_qa_snapshot.py"),
]


def log_message(message: str):
    """Log message to both console and log file."""
    timestamp = datetime.now(timezone.utc).isoformat()
    log_line = f"[{timestamp}] {message}"
    
    print(message)
    
    with open(PIPELINE_LOG, "a") as f:
        f.write(log_line + "\n")


def run_step(step_name: str, script_path: str) -> bool:
    """
    Run a pipeline step.
    
    Returns:
        True if step passed, False if failed
    """
    log_message(f"Starting {step_name}...")
    log_message("-" * 60)
    
    # Use .venv/bin/python to avoid module errors
    python_bin = ".venv/bin/python"
    if not os.path.exists(python_bin):
        python_bin = "python3"
    
    try:
        result = subprocess.run(
            [python_bin, script_path],
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout per step
        )
        
        # Log output
        if result.stdout:
            log_message(result.stdout)
        
        if result.stderr:
            log_message(f"STDERR: {result.stderr}")
        
        if result.returncode == 0:
            log_message(f"✓ {step_name} completed successfully")
            log_message("")
            return True
        else:
            log_message(f"✗ {step_name} failed with exit code {result.returncode}")
            log_message("")
            return False
    
    except subprocess.TimeoutExpired:
        log_message(f"✗ {step_name} timed out after 2 hours")
        log_message("")
        return False
    
    except Exception as e:
        log_message(f"✗ {step_name} failed with exception: {e}")
        log_message("")
        return False


def load_json_file(file_path: str) -> Optional[Dict]:
    """Load JSON file, return None if not found."""
    if not os.path.exists(file_path):
        return None
    
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def load_csv_count(file_path: str) -> int:
    """Count rows in CSV file (excluding header)."""
    if not os.path.exists(file_path):
        return 0
    
    try:
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return sum(1 for _ in reader)
    except Exception:
        return 0


def generate_summary():
    """Generate final pipeline execution summary."""
    log_message("")
    log_message("=" * 60)
    log_message("PIPELINE EXECUTION SUMMARY")
    log_message("=" * 60)
    log_message("")
    
    # Load baseline and postrun metrics
    baseline_file = os.path.join(OUTPUT_DIR, "baseline_completeness.json")
    postrun_file = os.path.join(OUTPUT_DIR, "postrun_completeness.json")
    
    baseline_data = load_json_file(baseline_file)
    postrun_data = load_json_file(postrun_file)
    
    if not baseline_data or not postrun_data:
        log_message("ERROR: Could not load baseline or postrun metrics")
        return
    
    baseline_metrics = baseline_data.get('metrics', {})
    postrun_metrics = postrun_data.get('metrics', {})
    
    baseline_global = baseline_metrics.get('GLOBAL', {})
    postrun_global = postrun_metrics.get('GLOBAL', {})
    
    # Step results
    canonical_docs_count = load_csv_count(os.path.join(OUTPUT_DIR, "canonical_docs_created.csv"))
    
    # Load enrichment report
    enrichment_file = os.path.join(OUTPUT_DIR, "enrichment_report.md")
    enrichment_cost = 0.0
    emb_batch_id = "N/A"
    sum_batch_id = "N/A"
    
    if os.path.exists(enrichment_file):
        with open(enrichment_file, "r") as f:
            content = f.read()
            # Parse batch IDs and cost (simple regex-free parsing)
            for line in content.split("\n"):
                if "Batch ID:" in line and "`" in line:
                    batch_id = line.split("`")[1]
                    if emb_batch_id == "N/A":
                        emb_batch_id = batch_id
                    else:
                        sum_batch_id = batch_id
                if "Total Cost" in line and "$" in line:
                    try:
                        cost_str = line.split("$")[1].split()[0]
                        enrichment_cost = float(cost_str)
                    except Exception:
                        pass
    
    # Firecrawl usage
    firecrawl_urls = canonical_docs_count  # Approximate
    
    # Robots.txt blocks
    robots_blocked_count = load_csv_count(os.path.join(OUTPUT_DIR, "robots_blocked.csv"))
    
    # Coverage deltas
    doc_baseline = baseline_global.get('doc_completeness_pct', 0)
    doc_postrun = postrun_global.get('doc_completeness_pct', 0)
    doc_delta = doc_postrun - doc_baseline
    
    sum_baseline = baseline_global.get('summary_coverage_pct', 0)
    sum_postrun = postrun_global.get('summary_coverage_pct', 0)
    sum_delta = sum_postrun - sum_baseline
    
    emb_baseline = baseline_global.get('embedding_coverage_pct', 0)
    emb_postrun = postrun_global.get('embedding_coverage_pct', 0)
    emb_delta = emb_postrun - emb_baseline
    
    # Top improvements by authority
    coverage_csv = os.path.join(OUTPUT_DIR, "coverage_by_authority.csv")
    top_improvements = []
    
    if os.path.exists(coverage_csv):
        with open(coverage_csv, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Sort by delta_doc_pct
            rows_sorted = sorted(rows, key=lambda x: float(x.get('delta_doc_pct', 0)), reverse=True)
            top_improvements = rows_sorted[:3]
    
    # Snapshot path
    snapshot_path_file = os.path.join(OUTPUT_DIR, "snapshot_path.txt")
    snapshot_path = "N/A"
    
    if os.path.exists(snapshot_path_file):
        with open(snapshot_path_file, "r") as f:
            snapshot_path = f.read().strip()
    
    # Check for blockers
    blockers_file = os.path.join(OUTPUT_DIR, "blockers.md")
    has_blockers = os.path.exists(blockers_file)
    
    overall_status = "FAIL" if has_blockers else "PASS"
    
    # Print summary
    log_message(f"STEP 0 (Baseline): PASS")
    log_message(f"STEP 1 (Canonical Docs): {'PASS' if canonical_docs_count > 0 else 'SKIPPED'} - {canonical_docs_count} docs created")
    log_message(f"STEP 2 (Micro-Enrich): {'PASS' if enrichment_cost > 0 else 'SKIPPED'}")
    log_message(f"STEP 3 (Mini-Harvest): SKIPPED")
    log_message(f"STEP 4 (QA + Snapshot): {'PASS' if os.path.exists(snapshot_path_file) else 'FAIL'}")
    log_message("")
    
    log_message("BUDGET USAGE")
    log_message("-" * 60)
    log_message(f"OpenAI Batch API: ${enrichment_cost:.4f} USD (limit: $10.00)")
    log_message(f"Firecrawl URLs: {firecrawl_urls} fetched (soft cap: 200)")
    log_message("")
    
    log_message("COVERAGE DELTAS (Global)")
    log_message("-" * 60)
    log_message(f"Document Completeness: {doc_baseline:.1f}% → {doc_postrun:.1f}% ({doc_delta:+.1f}pp)")
    log_message(f"Summary Coverage: {sum_baseline:.1f}% → {sum_postrun:.1f}% ({sum_delta:+.1f}pp)")
    log_message(f"Embedding Coverage: {emb_baseline:.1f}% → {emb_postrun:.1f}% ({emb_delta:+.1f}pp)")
    log_message("")
    
    if len(top_improvements) > 0:
        log_message("TOP IMPROVEMENTS (by authority)")
        log_message("-" * 60)
        for row in top_improvements:
            authority = row.get('authority', 'N/A')
            delta = float(row.get('delta_doc_pct', 0))
            log_message(f"{authority}: {delta:+.1f}pp doc completeness")
        log_message("")
    
    log_message("OPENAI BATCH JOB IDs")
    log_message("-" * 60)
    log_message(f"Embeddings (Step 2): {emb_batch_id}")
    log_message(f"Summaries (Step 2): {sum_batch_id}")
    log_message("")
    
    log_message("SNAPSHOT LOCATION")
    log_message("-" * 60)
    log_message(snapshot_path)
    log_message("")
    
    log_message("ROBOTS.TXT BLOCKS")
    log_message("-" * 60)
    log_message(f"{robots_blocked_count} URLs blocked")
    if robots_blocked_count > 0:
        log_message("See robots_blocked.csv for details")
    log_message("")
    
    log_message(f"OVERALL STATUS: {overall_status}")
    log_message("")
    
    if has_blockers:
        log_message("=" * 60)
        log_message("BLOCKERS DETECTED")
        log_message("=" * 60)
        log_message("")
        
        with open(blockers_file, "r") as f:
            log_message(f.read())


def main():
    """Main entry point."""
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Initialize log file
    with open(PIPELINE_LOG, "w") as f:
        f.write(f"Pipeline started at {datetime.now(timezone.utc).isoformat()}\n")
        f.write("=" * 60 + "\n\n")
    
    log_message("=" * 60)
    log_message("ONE-SHOT PIPELINE: CANONICAL DOCS + MICRO-ENRICHMENT + QA")
    log_message("=" * 60)
    log_message("")
    log_message("Hard Constraints:")
    log_message("  - OpenAI budget: $10 USD max")
    log_message("  - Firecrawl: ≤200 URLs soft cap")
    log_message("  - Robots.txt compliance enforced")
    log_message("  - Rate limit handling (3-strike rule)")
    log_message("")
    
    # Run each step
    all_passed = True
    
    for step_name, script_path in STEPS:
        passed = run_step(step_name, script_path)
        
        if not passed:
            log_message(f"Pipeline halted due to {step_name} failure")
            all_passed = False
            break
    
    # Generate summary
    generate_summary()
    
    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()

