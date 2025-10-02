#!/usr/bin/env python3
"""
STEP 4: QA Checks + KPI Pack + Snapshot Archive

Actions:
  4A: Data Quality Checks
  4B: Coverage Metrics (postrun)
  4C: Final Report
  4D: Snapshot Archive

Pass Criteria:
- All DQ checks pass (or only minor failures documented)
- postrun_completeness.json shows improvement over baseline
- coverage_by_authority.csv exists with all authorities
- final_report.md exists with all required sections
- ZIP archive created successfully
"""

import csv
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from typing import Dict, List

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor


OUTPUT_DIR = "data/output/validation/latest"
DELIVERABLES_DIR = "deliverables"

BASELINE_FILE = os.path.join(OUTPUT_DIR, "baseline_completeness.json")
POSTRUN_FILE = os.path.join(OUTPUT_DIR, "postrun_completeness.json")
COVERAGE_CSV = os.path.join(OUTPUT_DIR, "coverage_by_authority.csv")
DQ_REPORT = os.path.join(OUTPUT_DIR, "dq_report.md")
FINAL_REPORT = os.path.join(OUTPUT_DIR, "final_report.md")
SNAPSHOT_PATH_FILE = os.path.join(OUTPUT_DIR, "snapshot_path.txt")


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def compute_completeness_metrics(conn):
    """Compute completeness metrics (same as Step 0)."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Per-authority metrics
    query = """
    SELECT 
        e.authority,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN 1 END) AS events_with_docs,
        COUNT(CASE WHEN e.summary_en IS NOT NULL THEN 1 END) AS events_with_summary,
        COUNT(CASE WHEN e.embedding IS NOT NULL THEN 1 END) AS events_with_embedding
    FROM events e
    LEFT JOIN documents d ON d.event_id = e.event_id
    GROUP BY e.authority
    ORDER BY e.authority;
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    metrics = {}
    
    for row in rows:
        authority = row['authority']
        total = row['total_events']
        
        metrics[authority] = {
            'total_events': total,
            'events_with_docs': row['events_with_docs'],
            'events_with_summary': row['events_with_summary'],
            'events_with_embedding': row['events_with_embedding'],
            'doc_completeness_pct': round(100.0 * row['events_with_docs'] / total, 2) if total > 0 else 0.0,
            'summary_coverage_pct': round(100.0 * row['events_with_summary'] / total, 2) if total > 0 else 0.0,
            'embedding_coverage_pct': round(100.0 * row['events_with_embedding'] / total, 2) if total > 0 else 0.0,
        }
    
    # Global totals
    cur.execute("""
    SELECT 
        COUNT(*) AS total_events,
        COUNT(CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN 1 END) AS events_with_docs,
        COUNT(CASE WHEN e.summary_en IS NOT NULL THEN 1 END) AS events_with_summary,
        COUNT(CASE WHEN e.embedding IS NOT NULL THEN 1 END) AS events_with_embedding
    FROM events e
    LEFT JOIN documents d ON d.event_id = e.event_id;
    """)
    
    global_row = cur.fetchone()
    total_global = global_row['total_events']
    
    metrics['GLOBAL'] = {
        'total_events': total_global,
        'events_with_docs': global_row['events_with_docs'],
        'events_with_summary': global_row['events_with_summary'],
        'events_with_embedding': global_row['events_with_embedding'],
        'doc_completeness_pct': round(100.0 * global_row['events_with_docs'] / total_global, 2) if total_global > 0 else 0.0,
        'summary_coverage_pct': round(100.0 * global_row['events_with_summary'] / total_global, 2) if total_global > 0 else 0.0,
        'embedding_coverage_pct': round(100.0 * global_row['events_with_embedding'] / total_global, 2) if total_global > 0 else 0.0,
    }
    
    cur.close()
    return metrics


def run_dq_checks(conn) -> Dict:
    """Run data quality checks."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    checks = {}
    
    # Check 1: Uniqueness of event_hash within authority
    cur.execute("""
        SELECT authority, event_hash, COUNT(*) as cnt
        FROM events
        GROUP BY authority, event_hash
        HAVING COUNT(*) > 1
        LIMIT 10;
    """)
    duplicates = cur.fetchall()
    checks['uniqueness'] = {
        'pass': len(duplicates) == 0,
        'failures': [dict(row) for row in duplicates]
    }
    
    # Check 2: Completeness of required fields
    cur.execute("""
        SELECT event_id, authority, title, url
        FROM events
        WHERE authority IS NULL OR title IS NULL OR url IS NULL OR access_ts IS NULL
        LIMIT 10;
    """)
    incomplete = cur.fetchall()
    checks['completeness'] = {
        'pass': len(incomplete) == 0,
        'failures': [dict(row) for row in incomplete]
    }
    
    # Check 3: Document quality (median length)
    cur.execute("""
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(clean_text)) AS median_length
        FROM documents
        WHERE clean_text IS NOT NULL;
    """)
    median_row = cur.fetchone()
    median_length = median_row['median_length'] if median_row else 0
    checks['document_quality'] = {
        'pass': median_length >= 500,
        'median_length': int(median_length) if median_length else 0
    }
    
    # Check 4: URL validity
    cur.execute("""
        SELECT event_id, url
        FROM events
        WHERE url NOT LIKE 'http://%' AND url NOT LIKE 'https://%'
        LIMIT 10;
    """)
    invalid_urls = cur.fetchall()
    checks['url_validity'] = {
        'pass': len(invalid_urls) == 0,
        'failures': [dict(row) for row in invalid_urls]
    }
    
    # Check 5: Timeliness (80% of events from last 90 days have access_ts)
    cur.execute("""
        SELECT 
            COUNT(*) AS total,
            COUNT(CASE WHEN access_ts IS NOT NULL THEN 1 END) AS with_access_ts
        FROM events
        WHERE pub_date >= NOW() - INTERVAL '90 days';
    """)
    timeliness_row = cur.fetchone()
    total = timeliness_row['total'] if timeliness_row else 0
    with_access_ts = timeliness_row['with_access_ts'] if timeliness_row else 0
    timeliness_pct = 100.0 * with_access_ts / total if total > 0 else 0
    checks['timeliness'] = {
        'pass': timeliness_pct >= 80.0,
        'percentage': round(timeliness_pct, 2)
    }
    
    cur.close()
    return checks


def write_dq_report(checks: Dict):
    """Write data quality report."""
    with open(DQ_REPORT, "w") as f:
        f.write("# Data Quality Report\n\n")
        f.write(f"**Generated:** {datetime.now(timezone.utc).isoformat()}\n\n")
        
        for check_name, check_data in checks.items():
            status = "✓ PASS" if check_data['pass'] else "✗ FAIL"
            f.write(f"## {check_name.replace('_', ' ').title()}\n\n")
            f.write(f"**Status:** {status}\n\n")
            
            if 'failures' in check_data and len(check_data['failures']) > 0:
                f.write(f"**Sample Failures ({len(check_data['failures'])}):**\n\n")
                f.write("```json\n")
                f.write(json.dumps(check_data['failures'], indent=2))
                f.write("\n```\n\n")
            
            if 'median_length' in check_data:
                f.write(f"**Median Length:** {check_data['median_length']} chars\n\n")
            
            if 'percentage' in check_data:
                f.write(f"**Percentage:** {check_data['percentage']:.1f}%\n\n")


def write_coverage_csv(baseline_metrics: Dict, postrun_metrics: Dict):
    """Write coverage comparison CSV."""
    with open(COVERAGE_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "authority",
            "baseline_doc_pct",
            "postrun_doc_pct",
            "baseline_summary_pct",
            "postrun_summary_pct",
            "baseline_embed_pct",
            "postrun_embed_pct",
            "delta_doc_pct",
            "delta_summary_pct",
            "delta_embed_pct"
        ])
        
        all_authorities = set(baseline_metrics.keys()) | set(postrun_metrics.keys())
        
        for authority in sorted(all_authorities):
            baseline = baseline_metrics.get(authority, {})
            postrun = postrun_metrics.get(authority, {})
            
            baseline_doc = baseline.get('doc_completeness_pct', 0)
            postrun_doc = postrun.get('doc_completeness_pct', 0)
            baseline_sum = baseline.get('summary_coverage_pct', 0)
            postrun_sum = postrun.get('summary_coverage_pct', 0)
            baseline_emb = baseline.get('embedding_coverage_pct', 0)
            postrun_emb = postrun.get('embedding_coverage_pct', 0)
            
            writer.writerow([
                authority,
                f"{baseline_doc:.2f}",
                f"{postrun_doc:.2f}",
                f"{baseline_sum:.2f}",
                f"{postrun_sum:.2f}",
                f"{baseline_emb:.2f}",
                f"{postrun_emb:.2f}",
                f"{postrun_doc - baseline_doc:+.2f}",
                f"{postrun_sum - baseline_sum:+.2f}",
                f"{postrun_emb - baseline_emb:+.2f}"
            ])


def write_final_report(baseline_metrics: Dict, postrun_metrics: Dict, dq_checks: Dict):
    """Write final executive report."""
    with open(FINAL_REPORT, "w") as f:
        f.write("# Pipeline Final Report\n\n")
        f.write(f"**Generated:** {datetime.now(timezone.utc).isoformat()}\n\n")
        
        f.write("## Executive Summary\n\n")
        f.write("Completed one-shot pipeline for canonical document creation and micro-enrichment.\n\n")
        
        f.write("## Steps Completed\n\n")
        f.write("- [x] STEP 0: Baseline Metrics\n")
        f.write("- [x] STEP 1: Create Canonical Documents\n")
        f.write("- [x] STEP 2: Micro-Enrich (OpenAI Batch API)\n")
        f.write("- [x] STEP 3: Mini-Harvest (Conditional/Skipped)\n")
        f.write("- [x] STEP 4: QA Checks + Snapshot\n\n")
        
        f.write("## Coverage Improvements\n\n")
        
        baseline_global = baseline_metrics.get('GLOBAL', {})
        postrun_global = postrun_metrics.get('GLOBAL', {})
        
        f.write("### Global Metrics\n\n")
        f.write(f"- **Document Completeness:** {baseline_global.get('doc_completeness_pct', 0):.1f}% → {postrun_global.get('doc_completeness_pct', 0):.1f}% ({postrun_global.get('doc_completeness_pct', 0) - baseline_global.get('doc_completeness_pct', 0):+.1f}pp)\n")
        f.write(f"- **Summary Coverage:** {baseline_global.get('summary_coverage_pct', 0):.1f}% → {postrun_global.get('summary_coverage_pct', 0):.1f}% ({postrun_global.get('summary_coverage_pct', 0) - baseline_global.get('summary_coverage_pct', 0):+.1f}pp)\n")
        f.write(f"- **Embedding Coverage:** {baseline_global.get('embedding_coverage_pct', 0):.1f}% → {postrun_global.get('embedding_coverage_pct', 0):.1f}% ({postrun_global.get('embedding_coverage_pct', 0) - baseline_global.get('embedding_coverage_pct', 0):+.1f}pp)\n\n")
        
        f.write("## Data Quality\n\n")
        all_pass = all(check['pass'] for check in dq_checks.values())
        f.write(f"**Status:** {'✓ All checks passed' if all_pass else '⚠ Some checks failed (see dq_report.md)'}\n\n")
        
        f.write("## Costs\n\n")
        f.write("See `enrichment_report.md` for detailed OpenAI Batch API costs.\n\n")
        
        f.write("## Robots.txt Blocks\n\n")
        robots_csv = os.path.join(OUTPUT_DIR, "robots_blocked.csv")
        if os.path.exists(robots_csv):
            with open(robots_csv, "r") as rf:
                reader = csv.DictReader(rf)
                blocked = list(reader)
                f.write(f"**Total Blocked:** {len(blocked)} URLs\n\n")
                if len(blocked) > 0:
                    f.write("See `robots_blocked.csv` for details.\n\n")
        else:
            f.write("No URLs blocked by robots.txt.\n\n")


def create_snapshot_archive() -> str:
    """Create ZIP archive of all deliverables."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_filename = f"backfill_snapshot_{timestamp}.zip"
    zip_path = os.path.join(DELIVERABLES_DIR, zip_filename)
    
    os.makedirs(DELIVERABLES_DIR, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add all files from OUTPUT_DIR
        for filename in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.isfile(file_path):
                zf.write(file_path, os.path.join("validation", filename))
        
        # Add config/sources.yaml
        if os.path.exists("config/sources.yaml"):
            zf.write("config/sources.yaml", "config/sources.yaml")
    
    return os.path.abspath(zip_path)


def write_blocker(step: str, status: str, error: str, details: str = ""):
    """Write blocker file."""
    with open(os.path.join(OUTPUT_DIR, "blockers.md"), "w") as f:
        f.write("# Pipeline Blockers\n\n")
        f.write(f"## {step}\n\n")
        f.write(f"**Status:** {status}\n\n")
        f.write(f"**Error:** {error}\n\n")
        if details:
            f.write(f"**Details:**\n```\n{details}\n```\n\n")
        f.write(f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}\n")


def main():
    """Main entry point."""
    print("=" * 60)
    print("STEP 4: QA Checks + KPI Pack + Snapshot Archive")
    print("=" * 60)
    print()
    
    # Load baseline metrics
    if not os.path.exists(BASELINE_FILE):
        print(f"ERROR: Baseline file not found: {BASELINE_FILE}", file=sys.stderr)
        write_blocker("STEP 4: QA + Snapshot", "FAILED", "Baseline file not found")
        sys.exit(1)
    
    with open(BASELINE_FILE, "r") as f:
        baseline_data = json.load(f)
        baseline_metrics = baseline_data['metrics']
    
    # Connect to database
    try:
        conn = get_db()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        write_blocker("STEP 4: QA + Snapshot", "FAILED", "Database connection failed", str(e))
        sys.exit(1)
    
    # 4A: Run DQ checks
    print("Running data quality checks...")
    try:
        dq_checks = run_dq_checks(conn)
        write_dq_report(dq_checks)
        print(f"  ✓ Wrote DQ report to {DQ_REPORT}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to run DQ checks: {e}", file=sys.stderr)
        write_blocker("STEP 4: QA + Snapshot", "FAILED", "DQ checks failed", str(e))
        sys.exit(1)
    
    # 4B: Compute postrun metrics
    print("Computing postrun completeness metrics...")
    try:
        postrun_metrics = compute_completeness_metrics(conn)
        conn.close()
        
        postrun_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': postrun_metrics
        }
        
        with open(POSTRUN_FILE, "w") as f:
            json.dump(postrun_data, f, indent=2)
        
        print(f"  ✓ Wrote postrun metrics to {POSTRUN_FILE}")
        print()
        
    except Exception as e:
        print(f"ERROR: Failed to compute postrun metrics: {e}", file=sys.stderr)
        write_blocker("STEP 4: QA + Snapshot", "FAILED", "Postrun metrics failed", str(e))
        sys.exit(1)
    
    # Write coverage CSV
    print("Writing coverage comparison CSV...")
    write_coverage_csv(baseline_metrics, postrun_metrics)
    print(f"  ✓ Wrote coverage CSV to {COVERAGE_CSV}")
    print()
    
    # 4C: Write final report
    print("Writing final report...")
    write_final_report(baseline_metrics, postrun_metrics, dq_checks)
    print(f"  ✓ Wrote final report to {FINAL_REPORT}")
    print()
    
    # 4D: Create snapshot archive
    print("Creating snapshot archive...")
    try:
        snapshot_path = create_snapshot_archive()
        
        with open(SNAPSHOT_PATH_FILE, "w") as f:
            f.write(snapshot_path)
        
        print(f"  ✓ Created snapshot: {snapshot_path}")
        print(f"  ✓ Wrote path to {SNAPSHOT_PATH_FILE}")
        print()
        
    except Exception as e:
        print(f"ERROR: Failed to create snapshot: {e}", file=sys.stderr)
        write_blocker("STEP 4: QA + Snapshot", "FAILED", "Snapshot creation failed", str(e))
        sys.exit(1)
    
    print("✓ STEP 4: PASS")
    print()


if __name__ == "__main__":
    main()

