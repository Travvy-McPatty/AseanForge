#!/usr/bin/env python3
"""
STEP 0: Baseline Metrics (Pre-flight Check)

Query database for per-authority completeness metrics:
- Total events count
- Events with non-empty documents.clean_text (length ≥ 400 chars)
- Events with non-null events.summary_en
- Events with non-null events.embedding (vector dimension 1536)

Output: baseline_completeness.json
"""

import json
import os
import sys
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor


OUTPUT_DIR = "data/output/validation/latest"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "baseline_completeness.json")


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def compute_completeness_metrics(conn):
    """
    Compute completeness metrics per authority and global totals.
    
    Returns:
        dict: Metrics by authority plus 'GLOBAL' totals
    """
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


def main():
    """Main entry point."""
    print("=" * 60)
    print("STEP 0: Baseline Metrics (Pre-flight Check)")
    print("=" * 60)
    print()
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Connect to database
    try:
        conn = get_db()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        
        # Write blocker
        with open(os.path.join(OUTPUT_DIR, "blockers.md"), "w") as f:
            f.write("# Pipeline Blockers\n\n")
            f.write("## STEP 0: Baseline Metrics\n\n")
            f.write(f"**Status:** FAILED\n\n")
            f.write(f"**Error:** Failed to connect to database\n\n")
            f.write(f"**Details:**\n```\n{e}\n```\n\n")
            f.write(f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}\n")
        
        sys.exit(1)
    
    # Compute metrics
    try:
        print("Computing completeness metrics...")
        metrics = compute_completeness_metrics(conn)
        conn.close()
        
        print(f"  ✓ Computed metrics for {len(metrics) - 1} authorities + global totals")
        print()
        
    except Exception as e:
        print(f"ERROR: Failed to compute metrics: {e}", file=sys.stderr)
        
        # Write blocker
        with open(os.path.join(OUTPUT_DIR, "blockers.md"), "w") as f:
            f.write("# Pipeline Blockers\n\n")
            f.write("## STEP 0: Baseline Metrics\n\n")
            f.write(f"**Status:** FAILED\n\n")
            f.write(f"**Error:** Failed to compute metrics\n\n")
            f.write(f"**Details:**\n```\n{e}\n```\n\n")
            f.write(f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}\n")
        
        sys.exit(1)
    
    # Write output
    output_data = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'metrics': metrics
    }
    
    try:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output_data, f, indent=2)
        
        print(f"✓ Wrote baseline metrics to {OUTPUT_FILE}")
        print()
        
    except Exception as e:
        print(f"ERROR: Failed to write output file: {e}", file=sys.stderr)
        
        # Write blocker
        with open(os.path.join(OUTPUT_DIR, "blockers.md"), "w") as f:
            f.write("# Pipeline Blockers\n\n")
            f.write("## STEP 0: Baseline Metrics\n\n")
            f.write(f"**Status:** FAILED\n\n")
            f.write(f"**Error:** Failed to write output file\n\n")
            f.write(f"**Details:**\n```\n{e}\n```\n\n")
            f.write(f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}\n")
        
        sys.exit(1)
    
    # Display summary
    print("BASELINE METRICS SUMMARY")
    print("-" * 60)
    
    global_metrics = metrics.get('GLOBAL', {})
    print(f"Total Events: {global_metrics.get('total_events', 0)}")
    print(f"Document Completeness: {global_metrics.get('doc_completeness_pct', 0):.1f}%")
    print(f"Summary Coverage: {global_metrics.get('summary_coverage_pct', 0):.1f}%")
    print(f"Embedding Coverage: {global_metrics.get('embedding_coverage_pct', 0):.1f}%")
    print()
    
    print("Top 5 Authorities by Event Count:")
    auth_list = [(k, v['total_events']) for k, v in metrics.items() if k != 'GLOBAL']
    auth_list.sort(key=lambda x: x[1], reverse=True)
    
    for auth, count in auth_list[:5]:
        doc_pct = metrics[auth]['doc_completeness_pct']
        print(f"  {auth}: {count} events ({doc_pct:.1f}% doc completeness)")
    
    print()
    print("✓ STEP 0: PASS")
    print()


if __name__ == "__main__":
    main()

