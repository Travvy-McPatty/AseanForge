#!/usr/bin/env python3
"""
Coverage Expansion Step 0: Preflight & Baseline

Purpose: Establish baseline metrics and validate environment before expansion
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor

OUTPUT_DIR = "data/output/validation/latest"
BASELINE_FILE = os.path.join(OUTPUT_DIR, "expansion_baseline.json")


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def compute_baseline_metrics():
    """Compute comprehensive baseline metrics."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Global metrics
    cur.execute("""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) as events_with_docs,
            COUNT(DISTINCT CASE WHEN e.summary_en IS NOT NULL THEN e.event_id END) as events_with_summary,
            COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN e.event_id END) as events_with_embedding,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) / COUNT(*), 2) as doc_completeness_pct,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.summary_en IS NOT NULL THEN e.event_id END) / COUNT(*), 2) as summary_coverage_pct,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN e.event_id END) / COUNT(*), 2) as embedding_coverage_pct
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
    """)
    global_metrics = dict(cur.fetchone())
    
    # Per-authority metrics
    cur.execute("""
        SELECT 
            e.authority,
            COUNT(*) as total_events,
            COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) as events_with_docs,
            COUNT(DISTINCT CASE WHEN e.summary_en IS NOT NULL THEN e.event_id END) as events_with_summary,
            COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN e.event_id END) as events_with_embedding,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) / COUNT(*), 2) as doc_completeness_pct,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.summary_en IS NOT NULL THEN e.event_id END) / COUNT(*), 2) as summary_coverage_pct,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN e.event_id END) / COUNT(*), 2) as embedding_coverage_pct
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        GROUP BY e.authority
        ORDER BY e.authority
    """)
    authority_metrics = {row['authority']: dict(row) for row in cur.fetchall()}
    
    # Freshness metrics (7, 30, 90 days)
    now = datetime.now(timezone.utc)
    freshness_windows = [7, 30, 90]
    freshness_metrics = {}
    
    for days in freshness_windows:
        since_date = now - timedelta(days=days)
        cur.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) as events_with_docs,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400 THEN e.event_id END) / COUNT(*), 2) as doc_completeness_pct
            FROM events e
            LEFT JOIN documents d ON d.event_id = e.event_id
            WHERE e.pub_date >= %s
        """, (since_date,))
        freshness_metrics[f"{days}d"] = dict(cur.fetchone())
    
    # Identify laggards (doc completeness < 75%)
    laggards = []
    for authority, metrics in authority_metrics.items():
        if metrics['doc_completeness_pct'] < 75.0:
            laggards.append({
                'authority': authority,
                'doc_completeness_pct': metrics['doc_completeness_pct'],
                'total_events': metrics['total_events'],
                'events_with_docs': metrics['events_with_docs']
            })
    
    # Sort laggards by total events (prioritize high-volume authorities)
    laggards.sort(key=lambda x: x['total_events'], reverse=True)
    
    cur.close()
    conn.close()
    
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'global': global_metrics,
        'by_authority': authority_metrics,
        'freshness': freshness_metrics,
        'laggards': laggards
    }


def validate_environment():
    """Validate environment and SDK configurations."""
    issues = []
    
    # Check required environment variables
    required_vars = [
        'NEON_DATABASE_URL',
        'FIRECRAWL_API_KEY',
        'OPENAI_API_KEY',
        'SUMMARY_MODEL',
        'EMBED_MODEL',
        'ROBOTS_UA'
    ]
    
    for var in required_vars:
        if not os.getenv(var):
            issues.append(f"Missing environment variable: {var}")
    
    # Test database connection
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
    except Exception as e:
        issues.append(f"Database connection failed: {e}")
    
    # Test Firecrawl SDK
    try:
        from firecrawl import FirecrawlApp
        fc = FirecrawlApp(api_key=os.getenv('FIRECRAWL_API_KEY'))
        # Test signature (should have wait_for parameter)
        import inspect
        sig = inspect.signature(fc.scrape)
        if 'wait_for' not in sig.parameters:
            issues.append("Firecrawl SDK appears to be v1 (need v2 with wait_for parameter)")
    except Exception as e:
        issues.append(f"Firecrawl SDK validation failed: {e}")
    
    # Test OpenAI SDK
    try:
        import openai
        client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        # Test that we can access the client
        if not hasattr(client, 'batches'):
            issues.append("OpenAI SDK missing batch API support")
    except Exception as e:
        issues.append(f"OpenAI SDK validation failed: {e}")
    
    return issues


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 0: Preflight & Baseline")
    print("=" * 60)
    print()
    
    # Validate environment
    print("Validating environment...")
    issues = validate_environment()
    
    if issues:
        print("✗ Environment validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        print()
        sys.exit(1)
    
    print("  ✓ Environment validation passed")
    print()
    
    # Compute baseline metrics
    print("Computing baseline metrics...")
    try:
        baseline = compute_baseline_metrics()
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Write baseline file (convert Decimal to float for JSON serialization)
        def decimal_to_float(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: decimal_to_float(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [decimal_to_float(v) for v in obj]
            return obj

        baseline_serializable = decimal_to_float(baseline)

        with open(BASELINE_FILE, 'w') as f:
            json.dump(baseline_serializable, f, indent=2)
        
        print(f"  ✓ Baseline metrics saved to {BASELINE_FILE}")
        print()

        # Sanity checks: zero-doc and short-doc counts by authority, FK & duplicates
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            WITH per_event AS (
              SELECT e.event_id, e.authority,
                     COALESCE(MAX(LENGTH(d.clean_text)), 0) AS max_len
              FROM events e
              LEFT JOIN documents d ON d.event_id = e.event_id
              GROUP BY e.event_id, e.authority
            )
            SELECT authority,
                   COUNT(*) FILTER (WHERE max_len = 0) AS zero_doc_events,
                   COUNT(*) FILTER (WHERE max_len > 0 AND max_len < 400) AS short_doc_events
            FROM per_event
            GROUP BY authority
            ORDER BY authority
        """)
        rows = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*)
            FROM documents d
            LEFT JOIN events e ON e.event_id = d.event_id
            WHERE e.event_id IS NULL
        """)
        orphan_docs = cur.fetchone()[0]

        cur.execute("""
            SELECT (COUNT(*) - COUNT(DISTINCT source_url))
            FROM documents
            WHERE source_url IS NOT NULL AND source_url <> ''
        """)
        duplicate_source_urls = cur.fetchone()[0]

        # Diagnosis note
        diagnosis = (
            "Likely cause of no improvement: previous Step 2 targeted events with max_doc_length < 1000, "
            "which often already had qualifying docs (>=400 chars). Switching to zero-doc/short-doc (<400) focus."
        )

        # Write sanity findings
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        sanity_path = os.path.join(OUTPUT_DIR, "sanity_findings.md")
        with open(sanity_path, 'w') as fmd:
            fmd.write("# Sanity Findings\n\n")
            fmd.write("## Zero-doc and Short-doc Events by Authority\n\n")
            fmd.write("authority | zero_doc_events | short_doc_events\n")
            fmd.write("---|---:|---:\n")
            for r in rows:
                fmd.write(f"{r[0]} | {r[1]} | {r[2]}\n")
            fmd.write("\n## Integrity Checks\n\n")
            fmd.write(f"Orphan documents (no matching event): {orphan_docs}\n\n")
            fmd.write(f"Duplicate source_url entries: {duplicate_source_urls}\n\n")
            fmd.write("## Diagnosis\n\n")
            fmd.write(diagnosis)

        cur.close()
        conn.close()
        print(f"  ✓ Sanity findings saved to {sanity_path}")
        print()

    except Exception as e:
        print(f"✗ Failed to compute baseline metrics: {e}")
        sys.exit(1)

    # Display summary
    global_metrics = baseline['global']
    laggards = baseline['laggards']
    freshness_90d = baseline['freshness']['90d']
    
    print("BASELINE SUMMARY")
    print("-" * 40)
    print(f"Total Events: {global_metrics['total_events']}")
    print(f"Global Doc Completeness: {global_metrics['doc_completeness_pct']:.1f}%")
    print(f"Summary Coverage: {global_metrics['summary_coverage_pct']:.1f}%")
    print(f"Embedding Coverage: {global_metrics['embedding_coverage_pct']:.1f}%")
    print(f"90-day Doc Completeness: {freshness_90d['doc_completeness_pct']:.1f}%")
    print()
    
    print(f"LAGGARDS (doc completeness < 75%): {len(laggards)}")
    print("-" * 40)
    for laggard in laggards[:8]:  # Show top 8
        print(f"  {laggard['authority']}: {laggard['doc_completeness_pct']:.1f}% ({laggard['events_with_docs']}/{laggard['total_events']} events)")
    print()
    
    # Check targets
    targets_met = []
    targets_needed = []
    
    if global_metrics['doc_completeness_pct'] >= 80.0:
        targets_met.append("Global doc completeness ≥80%")
    else:
        targets_needed.append(f"Global doc completeness: {global_metrics['doc_completeness_pct']:.1f}% → ≥80%")
    
    if freshness_90d['doc_completeness_pct'] >= 85.0:
        targets_met.append("90-day freshness ≥85%")
    else:
        targets_needed.append(f"90-day freshness: {freshness_90d['doc_completeness_pct']:.1f}% → ≥85%")
    
    if targets_met:
        print("TARGETS ALREADY MET:")
        for target in targets_met:
            print(f"  ✓ {target}")
        print()
    
    if targets_needed:
        print("TARGETS TO ACHIEVE:")
        for target in targets_needed:
            print(f"  → {target}")
        print()
    
    print("✓ STEP 0: PASS")
    print()


if __name__ == "__main__":
    main()
