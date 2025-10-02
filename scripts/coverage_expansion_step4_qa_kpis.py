#!/usr/bin/env python3
"""
Coverage Expansion Step 4: QA & KPIs

Purpose: Run quality assurance checks and compute coverage/freshness KPIs
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

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


def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(v) for v in obj]
    return obj


def run_qa_checks():
    """Run data quality checks."""
    conn = get_db()
    cur = conn.cursor()

    qa_results = {}

    # 1. Uniqueness checks
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT event_hash) FROM events")
    total_events, unique_hashes = cur.fetchone()
    qa_results['uniqueness'] = {
        'total_events': total_events,
        'unique_hashes': unique_hashes,
        'duplicates': total_events - unique_hashes,
        'pass': total_events == unique_hashes
    }

    # 2. URL validity
    cur.execute("SELECT COUNT(*) FROM events WHERE url IS NULL OR url = ''")
    invalid_urls = cur.fetchone()[0]
    qa_results['url_validity'] = {
        'invalid_urls': invalid_urls,
        'pass': invalid_urls == 0
    }

    # 3. Document quality
    cur.execute("""
        SELECT
            COUNT(*) as total_docs,
            COUNT(*) FILTER (WHERE LENGTH(clean_text) >= 1000) as good_docs,
            AVG(LENGTH(clean_text)) as avg_length,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(clean_text)) as median_length
        FROM documents
        WHERE clean_text IS NOT NULL
    """)
    result = cur.fetchone()
    qa_results['document_quality'] = {
        'total_docs': result[0],
        'docs_over_1000_chars': result[1],
        'avg_length': float(result[2]) if result[2] else 0,
        'median_length': float(result[3]) if result[3] else 0,
        'pass': result[1] / max(1, result[0]) >= 0.8  # 80% of docs should be >1000 chars
    }

    # 4. Timeliness
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    cur.execute("""
        SELECT COUNT(*)
        FROM events
        WHERE pub_date >= %s
    """, (cutoff_date,))
    recent_events = cur.fetchone()[0]
    qa_results['timeliness'] = {
        'events_last_90_days': recent_events,
        'pass': recent_events >= 10  # Should have at least 10 recent events
    }

    cur.close()
    conn.close()

    # Overall QA pass
    qa_results['overall_pass'] = all(
        check['pass'] for check in qa_results.values()
        if isinstance(check, dict) and 'pass' in check
    )

    return qa_results


def compute_coverage_metrics():
    """Compute coverage and freshness metrics."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Global metrics
    cur.execute("""
        SELECT
            COUNT(*) as total_events,
            COUNT(*) FILTER (WHERE d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400) as events_with_docs,
            COUNT(*) FILTER (WHERE e.summary_en IS NOT NULL) as events_with_summaries,
            COUNT(*) FILTER (WHERE e.embedding IS NOT NULL) as events_with_embeddings
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
    """)

    global_result = cur.fetchone()
    global_metrics = {
        'total_events': global_result['total_events'],
        'events_with_docs': global_result['events_with_docs'],
        'events_with_summaries': global_result['events_with_summaries'],
        'events_with_embeddings': global_result['events_with_embeddings'],
        'doc_completeness_pct': (global_result['events_with_docs'] / global_result['total_events']) * 100,
        'summary_coverage_pct': (global_result['events_with_summaries'] / global_result['total_events']) * 100,
        'embedding_coverage_pct': (global_result['events_with_embeddings'] / global_result['total_events']) * 100
    }

    # Per-authority metrics
    cur.execute("""
        SELECT
            e.authority,
            COUNT(*) as total_events,
            COUNT(*) FILTER (WHERE d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400) as events_with_docs,
            COUNT(*) FILTER (WHERE e.summary_en IS NOT NULL) as events_with_summaries,
            COUNT(*) FILTER (WHERE e.embedding IS NOT NULL) as events_with_embeddings
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        GROUP BY e.authority
        ORDER BY e.authority
    """)

    authority_results = cur.fetchall()
    authority_metrics = {}

    for row in authority_results:
        authority = row['authority']
        authority_metrics[authority] = {
            'total_events': row['total_events'],
            'events_with_docs': row['events_with_docs'],
            'events_with_summaries': row['events_with_summaries'],
            'events_with_embeddings': row['events_with_embeddings'],
            'doc_completeness_pct': (row['events_with_docs'] / row['total_events']) * 100,
            'summary_coverage_pct': (row['events_with_summaries'] / row['total_events']) * 100,
            'embedding_coverage_pct': (row['events_with_embeddings'] / row['total_events']) * 100
        }

    # Freshness metrics (last 90 days)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    cur.execute("""
        SELECT
            COUNT(*) as total_events,
            COUNT(*) FILTER (WHERE d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 400) as events_with_docs
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        WHERE e.pub_date >= %s
    """, (cutoff_date,))

    freshness_result = cur.fetchone()
    freshness_metrics = {
        'total_events_90d': freshness_result['total_events'],
        'events_with_docs_90d': freshness_result['events_with_docs'],
        'doc_completeness_90d_pct': (freshness_result['events_with_docs'] / max(1, freshness_result['total_events'])) * 100
    }

    cur.close()
    conn.close()

    return {
        'global': decimal_to_float(global_metrics),
        'by_authority': decimal_to_float(authority_metrics),
        'freshness': decimal_to_float(freshness_metrics)
    }


def load_baseline():
    """Load baseline metrics."""
    if not os.path.exists(BASELINE_FILE):
        raise RuntimeError(f"Baseline file not found: {BASELINE_FILE}")

    with open(BASELINE_FILE, 'r') as f:
        return json.load(f)


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 4: QA & KPIs")
    print("=" * 60)
    print()

    # Load baseline
    try:
        baseline = load_baseline()
        print("✓ Loaded baseline metrics")
    except Exception as e:
        print(f"✗ Failed to load baseline: {e}")
        sys.exit(1)

    # Run QA checks
    print("Running data quality checks...")
    qa_results = run_qa_checks()

    print(f"  ✓ Uniqueness: {'PASS' if qa_results['uniqueness']['pass'] else 'FAIL'}")
    print(f"  ✓ URL validity: {'PASS' if qa_results['url_validity']['pass'] else 'FAIL'}")
    print(f"  ✓ Document quality: {'PASS' if qa_results['document_quality']['pass'] else 'FAIL'}")
    print(f"  ✓ Timeliness: {'PASS' if qa_results['timeliness']['pass'] else 'FAIL'}")
    print()

    # Compute current metrics
    print("Computing coverage metrics...")
    current_metrics = compute_coverage_metrics()
    print("  ✓ Coverage metrics computed")
    print()

    # Compare with baseline
    baseline_global = baseline['global']
    current_global = current_metrics['global']

    print("COVERAGE COMPARISON")
    print("-" * 40)
    baseline_fresh_90 = baseline['freshness']['90d']['doc_completeness_pct']
    current_fresh_90 = current_metrics['freshness']['doc_completeness_90d_pct']

    print(f"Global doc completeness: {baseline_global['doc_completeness_pct']:.1f}% → {current_global['doc_completeness_pct']:.1f}%")
    print(f"90-day freshness: {baseline_fresh_90:.1f}% → {current_fresh_90:.1f}%")
    print()

    # Check targets
    targets_met = True
    failures = []

    # Global target: ≥80%
    if current_global['doc_completeness_pct'] < 80.0:
        targets_met = False
        failures.append(f"Global doc completeness {current_global['doc_completeness_pct']:.1f}% (need ≥80%)")

    # Freshness target: ≥85%
    if current_metrics['freshness']['doc_completeness_90d_pct'] < 85.0:
        targets_met = False
        failures.append(f"90-day freshness {current_metrics['freshness']['doc_completeness_90d_pct']:.1f}% (need ≥85%)")

    # Per-authority targets
    laggards_improved = 0
    for authority in baseline['laggards']:
        auth_name = authority['authority']
        baseline_pct = authority['doc_completeness_pct']
        current_pct = current_metrics['by_authority'].get(auth_name, {}).get('doc_completeness_pct', 0)

        improvement = current_pct - baseline_pct
        if current_pct >= 75.0 or improvement >= 30.0:
            laggards_improved += 1

    if laggards_improved < 4:
        targets_met = False
        failures.append(f"Only {laggards_improved} laggards improved (need ≥4)")

    # QA checks
    if not qa_results['overall_pass']:
        targets_met = False
        failures.append("QA checks failed")

    # Write final report
    final_report = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'baseline_metrics': baseline,
        'current_metrics': current_metrics,
        'qa_results': qa_results,
        'targets_met': targets_met,
        'failures': failures,
        'improvements': {
            'global_doc_completeness_change': current_global['doc_completeness_pct'] - baseline_global['doc_completeness_pct'],
            'freshness_change': current_metrics['freshness']['doc_completeness_90d_pct'] - baseline_fresh_90,
            'laggards_improved': laggards_improved
        }
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "expansion_qa_kpis_report.json"), 'w') as f:
        json.dump(final_report, f, indent=2)

    # Write coverage CSV
    with open(os.path.join(OUTPUT_DIR, "expansion_coverage_by_authority.csv"), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['authority', 'baseline_doc_pct', 'current_doc_pct', 'improvement'])

        for authority in baseline['laggards']:
            auth_name = authority['authority']
            baseline_pct = authority['doc_completeness_pct']
            current_pct = current_metrics['by_authority'].get(auth_name, {}).get('doc_completeness_pct', 0)
            improvement = current_pct - baseline_pct
    # Also write MVP outputs
    # postrun_completeness.json
    postrun = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'baseline': {
            'global_doc_completeness_pct': baseline_global['doc_completeness_pct'],
            'freshness_90d_doc_completeness_pct': baseline_fresh_90
        },
        'current': {
            'global_doc_completeness_pct': current_global['doc_completeness_pct'],
            'freshness_90d_doc_completeness_pct': current_metrics['freshness']['doc_completeness_90d_pct']
        },
        'delta': {
            'global_doc_completeness_pp': current_global['doc_completeness_pct'] - baseline_global['doc_completeness_pct'],
            'freshness_90d_doc_completeness_pp': current_metrics['freshness']['doc_completeness_90d_pct'] - baseline_fresh_90
        }
    }
    with open(os.path.join(OUTPUT_DIR, 'postrun_completeness.json'), 'w') as f:
        json.dump(postrun, f, indent=2)

    # coverage_by_authority.csv (MVP name)
    with open(os.path.join(OUTPUT_DIR, 'coverage_by_authority.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['authority', 'baseline_doc_pct', 'current_doc_pct', 'improvement'])
        for authority in baseline['laggards']:
            auth_name = authority['authority']
            baseline_pct = authority['doc_completeness_pct']
            current_pct = current_metrics['by_authority'].get(auth_name, {}).get('doc_completeness_pct', 0)
            writer.writerow([auth_name, f"{baseline_pct:.1f}%", f"{current_pct:.1f}%", f"{(current_pct - baseline_pct):+.1f}pp"])

    # qa_results.json (MVP name)
    with open(os.path.join(OUTPUT_DIR, 'qa_results.json'), 'w') as f:
        json.dump(qa_results, f, indent=2)



    if targets_met:
        print("✓ STEP 4: PASS")
    else:
        print("! STEP 4: COMPLETE (MVP) – targets not met; see qa_results.json and postrun_completeness.json")

    print()


if __name__ == "__main__":
    main()
