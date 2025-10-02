#!/usr/bin/env python3
"""
STEP 2: Micro-Enrich Newly Documented Events (OpenAI Batch API)

Target: Events that received new/updated documents.clean_text in Step 1
Actions:
  2A: Generate embeddings using OpenAI Batch API
  2B: Generate summaries using OpenAI Batch API

Pass Criteria:
- Embeddings present for ≥95% of Step 1 cohort events
- Summaries present for ≥90% of Step 1 cohort events
- Zero database merge errors
- Cumulative OpenAI spend ≤ $10 USD
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor

# Import batch enrichment modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app.enrich_batch import builders, submit, poll, merge


OUTPUT_DIR = "data/output/validation/latest"
ENRICHMENT_REPORT = os.path.join(OUTPUT_DIR, "enrichment_report.md")
CANONICAL_DOCS_CSV = os.path.join(OUTPUT_DIR, "canonical_docs_created.csv")

# Budget limit
MAX_BUDGET_USD = 10.0


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def get_step1_event_ids() -> List[str]:
    """
    Get event IDs from Step 1 canonical docs CSV.
    """
    if not os.path.exists(CANONICAL_DOCS_CSV):
        return []
    
    event_ids = []
    
    with open(CANONICAL_DOCS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_ids.append(row['event_id'])
    
    return event_ids


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


def write_enrichment_report(
    emb_batch_id: str,
    emb_meta: Dict,
    emb_result: Dict,
    sum_batch_id: str,
    sum_meta: Dict,
    sum_result: Dict,
    total_cost: float
):
    """Write enrichment report."""
    with open(ENRICHMENT_REPORT, "w") as f:
        f.write("# Enrichment Report\n\n")
        f.write(f"**Generated:** {datetime.now(timezone.utc).isoformat()}\n\n")
        
        f.write("## Embeddings Batch\n\n")
        f.write(f"- **Batch ID:** `{emb_batch_id}`\n")
        f.write(f"- **Status:** {emb_result.get('status', 'unknown')}\n")
        f.write(f"- **Input Count:** {emb_meta.get('request_count', 0)}\n")
        f.write(f"- **Successful:** {emb_result.get('request_counts', {}).get('completed', 0)}\n")
        f.write(f"- **Failed:** {emb_result.get('request_counts', {}).get('failed', 0)}\n")
        f.write(f"- **Token Usage:** {emb_meta.get('estimated_tokens', 0):,}\n")
        f.write(f"- **Estimated Cost:** ${emb_meta.get('projected_cost_usd', 0):.4f}\n\n")

        f.write("## Summaries Batch\n\n")
        f.write(f"- **Batch ID:** `{sum_batch_id}`\n")
        f.write(f"- **Status:** {sum_result.get('status', 'unknown')}\n")
        f.write(f"- **Input Count:** {sum_meta.get('request_count', 0)}\n")
        f.write(f"- **Successful:** {sum_result.get('request_counts', {}).get('completed', 0)}\n")
        f.write(f"- **Failed:** {sum_result.get('request_counts', {}).get('failed', 0)}\n")
        f.write(f"- **Input Token Usage:** {sum_meta.get('estimated_input_tokens', 0):,}\n")
        f.write(f"- **Output Token Usage:** {sum_meta.get('estimated_output_tokens', 0):,}\n")
        f.write(f"- **Estimated Cost:** ${sum_meta.get('projected_cost_usd', 0):.4f}\n\n")
        
        f.write("## Total Cost\n\n")
        f.write(f"**${total_cost:.4f} USD** (limit: ${MAX_BUDGET_USD:.2f})\n\n")


def main():
    """Main entry point."""
    print("=" * 60)
    print("STEP 2: Micro-Enrich Newly Documented Events")
    print("=" * 60)
    print()
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("data/batch", exist_ok=True)
    
    # Get Step 1 event IDs
    step1_event_ids = get_step1_event_ids()
    
    if len(step1_event_ids) == 0:
        print("No events from Step 1. Skipping STEP 2.")
        print("✓ STEP 2: PASS (no work needed)")
        
        # Write empty report
        with open(ENRICHMENT_REPORT, "w") as f:
            f.write("# Enrichment Report\n\n")
            f.write("No events from Step 1 to enrich.\n")
        
        sys.exit(0)
    
    print(f"Found {len(step1_event_ids)} events from Step 1")
    print()
    
    # Connect to database to get events needing enrichment
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get events with documents but missing embeddings/summaries
        placeholders = ','.join(['%s'] * len(step1_event_ids))
        
        cur.execute(f"""
            SELECT 
                e.event_id,
                e.authority,
                d.clean_text,
                e.embedding IS NULL AS needs_embedding,
                e.summary_en IS NULL AS needs_summary
            FROM events e
            JOIN documents d ON d.event_id = e.event_id
            WHERE 
                e.event_id::text IN ({placeholders})
                AND d.clean_text IS NOT NULL
                AND LENGTH(d.clean_text) >= 400
        """, step1_event_ids)
        
        events = cur.fetchall()
        cur.close()
        conn.close()
        
        print(f"  ✓ Found {len(events)} events with documents needing enrichment")
        print()
        
    except Exception as e:
        print(f"ERROR: Failed to query events: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Database query failed", str(e))
        sys.exit(1)
    
    if len(events) == 0:
        print("No events need enrichment. Skipping STEP 2.")
        print("✓ STEP 2: PASS (no work needed)")
        sys.exit(0)
    
    # Build embedding requests
    print("Building embedding requests...")
    try:
        emb_meta = builders.build_embedding_requests(
            since_date=None,
            limit=None,
            output_path="data/batch/step2_embeddings.requests.jsonl",
            authorities=None
        )
        print(f"  ✓ Built {emb_meta['request_count']} embedding requests")
        print(f"    Estimated cost: ${emb_meta.get('projected_cost_usd', 0):.4f}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to build embedding requests: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to build embedding requests", str(e))
        sys.exit(1)

    # Build summary requests
    print("Building summary requests...")
    try:
        sum_meta = builders.build_summary_requests(
            since_date=None,
            limit=None,
            output_path="data/batch/step2_summaries.requests.jsonl",
            authorities=None
        )
        print(f"  ✓ Built {sum_meta['request_count']} summary requests")
        print(f"    Estimated cost: ${sum_meta.get('projected_cost_usd', 0):.4f}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to build summary requests: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to build summary requests", str(e))
        sys.exit(1)

    # Check budget
    total_estimated_cost = emb_meta.get('projected_cost_usd', 0) + sum_meta.get('projected_cost_usd', 0)
    
    print(f"Total estimated cost: ${total_estimated_cost:.4f}")
    print(f"Budget limit: ${MAX_BUDGET_USD:.2f}")
    print()
    
    if total_estimated_cost > MAX_BUDGET_USD:
        print(f"ERROR: Estimated cost exceeds budget limit", file=sys.stderr)
        write_blocker(
            "STEP 2: Micro-Enrich",
            "FAILED",
            "Budget exceeded",
            f"Estimated: ${total_estimated_cost:.4f}, Limit: ${MAX_BUDGET_USD:.2f}"
        )
        sys.exit(1)
    
    print("✓ Budget check passed")
    print()
    
    # Submit embeddings batch
    print("Submitting embeddings batch...")
    try:
        emb_batch_id = submit.submit_batch(emb_meta['file_path'], "embeddings")
        print(f"  ✓ Batch ID: {emb_batch_id}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to submit embeddings batch: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to submit embeddings batch", str(e))
        sys.exit(1)
    
    # Submit summaries batch
    print("Submitting summaries batch...")
    try:
        sum_batch_id = submit.submit_batch(sum_meta['file_path'], "summaries")
        print(f"  ✓ Batch ID: {sum_batch_id}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to submit summaries batch: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to submit summaries batch", str(e))
        sys.exit(1)
    
    # Poll embeddings batch
    print("Polling embeddings batch (this may take a while)...")
    try:
        emb_result = poll.poll_batch(emb_batch_id, poll_interval_seconds=60, timeout_hours=26)
        
        if emb_result['status'] != 'completed':
            print(f"ERROR: Embeddings batch did not complete: {emb_result['status']}", file=sys.stderr)
            write_blocker(
                "STEP 2: Micro-Enrich",
                "FAILED",
                f"Embeddings batch {emb_result['status']}",
                json.dumps(emb_result, indent=2)
            )
            sys.exit(1)
        
        print(f"  ✓ Embeddings batch completed")
        print()
    except Exception as e:
        print(f"ERROR: Failed to poll embeddings batch: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to poll embeddings batch", str(e))
        sys.exit(1)
    
    # Poll summaries batch
    print("Polling summaries batch (this may take a while)...")
    try:
        sum_result = poll.poll_batch(sum_batch_id, poll_interval_seconds=60, timeout_hours=26)
        
        if sum_result['status'] != 'completed':
            print(f"ERROR: Summaries batch did not complete: {sum_result['status']}", file=sys.stderr)
            write_blocker(
                "STEP 2: Micro-Enrich",
                "FAILED",
                f"Summaries batch {sum_result['status']}",
                json.dumps(sum_result, indent=2)
            )
            sys.exit(1)
        
        print(f"  ✓ Summaries batch completed")
        print()
    except Exception as e:
        print(f"ERROR: Failed to poll summaries batch: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to poll summaries batch", str(e))
        sys.exit(1)
    
    # Merge embeddings
    print("Merging embeddings into database...")
    try:
        emb_merge_stats = merge.merge_embeddings(emb_result['output_file_path'])
        print(f"  ✓ Upserted: {emb_merge_stats['upserted_count']}")
        print(f"    Skipped: {emb_merge_stats['skipped_count']}")
        print(f"    Errors: {emb_merge_stats['error_count']}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to merge embeddings: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to merge embeddings", str(e))
        sys.exit(1)
    
    # Merge summaries
    print("Merging summaries into database...")
    try:
        sum_merge_stats = merge.merge_summaries(sum_result['output_file_path'])
        print(f"  ✓ Upserted: {sum_merge_stats['upserted_count']}")
        print(f"    Skipped: {sum_merge_stats['skipped_count']}")
        print(f"    Errors: {sum_merge_stats['error_count']}")
        print()
    except Exception as e:
        print(f"ERROR: Failed to merge summaries: {e}", file=sys.stderr)
        write_blocker("STEP 2: Micro-Enrich", "FAILED", "Failed to merge summaries", str(e))
        sys.exit(1)
    
    # Write enrichment report
    write_enrichment_report(
        emb_batch_id, emb_meta, emb_result,
        sum_batch_id, sum_meta, sum_result,
        total_estimated_cost
    )
    
    print(f"✓ Wrote enrichment report to {ENRICHMENT_REPORT}")
    print()
    
    # Check pass criteria
    # Calculate coverage based on events that NEEDED enrichment
    # (not all Step 1 events, since some may already have been enriched)
    emb_needed = emb_meta.get('request_count', 0) // 3  # Rough estimate: ~3 chunks per doc
    sum_needed = sum_meta.get('request_count', 0)

    # Calculate actual coverage from database
    conn = get_db()
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(step1_event_ids))
    cur.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE e.embedding IS NOT NULL) as with_emb,
            COUNT(*) FILTER (WHERE e.summary_en IS NOT NULL) as with_sum,
            COUNT(*) as total
        FROM events e
        WHERE e.event_id::text IN ({placeholders})
    """, step1_event_ids)
    row = cur.fetchone()
    cur.close()
    conn.close()

    cohort_size = len(step1_event_ids)
    emb_coverage_pct = 100.0 * row[0] / cohort_size if cohort_size > 0 else 0
    sum_coverage_pct = 100.0 * row[1] / cohort_size if cohort_size > 0 else 0

    pass_criteria_met = True
    failures = []

    if emb_coverage_pct < 95.0:
        pass_criteria_met = False
        failures.append(f"Embedding coverage {emb_coverage_pct:.1f}% (need ≥95%)")

    if sum_coverage_pct < 90.0:
        pass_criteria_met = False
        failures.append(f"Summary coverage {sum_coverage_pct:.1f}% (need ≥90%)")
    
    if emb_merge_stats['error_count'] > 0 or sum_merge_stats['error_count'] > 0:
        pass_criteria_met = False
        failures.append(f"Database merge errors: {emb_merge_stats['error_count'] + sum_merge_stats['error_count']}")
    
    if total_estimated_cost > MAX_BUDGET_USD:
        pass_criteria_met = False
        failures.append(f"Cost ${total_estimated_cost:.4f} exceeds ${MAX_BUDGET_USD:.2f}")
    
    print("=" * 60)
    print("STEP 2 RESULTS")
    print("=" * 60)
    print(f"Cohort size: {cohort_size}")
    print(f"Embedding coverage: {emb_coverage_pct:.1f}%")
    print(f"Summary coverage: {sum_coverage_pct:.1f}%")
    print(f"Total cost: ${total_estimated_cost:.4f}")
    print()
    
    if not pass_criteria_met:
        print("✗ STEP 2: FAIL")
        print()
        print("Failures:")
        for failure in failures:
            print(f"  - {failure}")
        print()
        
        write_blocker(
            "STEP 2: Micro-Enrich",
            "FAILED",
            "Pass criteria not met",
            "\n".join(failures)
        )
        sys.exit(1)
    
    print("✓ STEP 2: PASS")
    print()


if __name__ == "__main__":
    main()

