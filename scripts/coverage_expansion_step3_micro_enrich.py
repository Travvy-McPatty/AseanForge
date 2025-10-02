#!/usr/bin/env python3
"""
Coverage Expansion Step 3: Micro-Enrichment

Purpose: Enrich newly created documents using OpenAI Batch API
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

# Import enrichment modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app.enrich_batch import builders, submit, poll, merge

OUTPUT_DIR = "data/output/validation/latest"
CANONICAL_CSV = os.path.join(OUTPUT_DIR, "mvp_canonical_docs.csv")

# Budget limits (MVP)
MAX_USD = float(os.getenv('OPENAI_ENRICH_MAX_USD', '10'))


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def load_step_c_event_ids() -> list:
    """Load event IDs from Step C output CSV."""
    if not os.path.exists(CANONICAL_CSV):
        return []
    ids = []
    import csv
    with open(CANONICAL_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('event_id'):
                ids.append(row['event_id'])
    # De-duplicate
    return sorted(list(set(ids)))


def get_events_needing_enrichment(event_ids: list) -> list:
    """Get events (restricted to Step C cohort) that need embeddings or summaries."""
    if not event_ids:
        return []
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    placeholders = ','.join(['%s'] * len(event_ids))
    cur.execute(f"""
        SELECT
            e.event_id,
            e.authority,
            e.title,
            e.url,
            CASE WHEN e.embedding IS NULL THEN 1 ELSE 0 END as needs_embedding,
            CASE WHEN e.summary_en IS NULL THEN 1 ELSE 0 END as needs_summary
        FROM events e
        INNER JOIN documents d ON d.event_id = e.event_id
        WHERE e.event_id IN ({placeholders})
          AND d.clean_text IS NOT NULL
          AND LENGTH(d.clean_text) >= 400
          AND (e.embedding IS NULL OR e.summary_en IS NULL)
        ORDER BY e.authority, e.pub_date DESC
    """, event_ids)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(row) for row in results]


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 3: Micro-Enrichment")
    print("=" * 60)
    print()
    
    # Load Step C cohort from CSV
    print("Loading Step C cohort from mvp_canonical_docs.csv...")
    cohort_event_ids = load_step_c_event_ids()

    # Get events needing enrichment, restricted to cohort
    events = get_events_needing_enrichment(cohort_event_ids)

    events_needing_embeddings = [e for e in events if e['needs_embedding']]
    events_needing_summaries = [e for e in events if e['needs_summary']]

    print(f"  ✓ Cohort size: {len(cohort_event_ids)} events")
    print(f"  ✓ Embeddings needed: {len(events_needing_embeddings)} events")
    print(f"  ✓ Summaries needed: {len(events_needing_summaries)} events")
    print()

    if len(events) == 0:
        print("No events need enrichment. Skipping Step 3.")
        print("✓ STEP 3: PASS (no work needed)")
        sys.exit(0)
    
    total_projected_cost = 0
    batch_ids = []
    
    # Build and submit embedding requests
    if events_needing_embeddings:
        print(f"Building embedding requests for {len(events_needing_embeddings)} events...")
        
        event_ids = [e['event_id'] for e in events_needing_embeddings]
        emb_result = builders.build_embedding_requests(event_ids)
        
        if emb_result['request_count'] > 0:
            projected_cost = emb_result.get('projected_cost_usd', 0)
            total_projected_cost += projected_cost
            
            print(f"  ✓ Built {emb_result['request_count']} embedding requests")
            print(f"  ✓ Projected cost: ${projected_cost:.4f}")
            
            if total_projected_cost > MAX_USD:
                print(f"  ✗ Projected cost ${total_projected_cost:.4f} exceeds budget ${MAX_USD}")
                sys.exit(1)
            
            # Submit batch
            print("  Submitting embedding batch...")
            batch_id = submit.submit_batch(emb_result['file_path'], "embeddings")
            batch_ids.append(('embeddings', batch_id))
            print(f"  ✓ Submitted batch: {batch_id}")
        else:
            print("  ✓ No embedding requests to submit")
    
    # Build and submit summary requests
    if events_needing_summaries:
        print(f"Building summary requests for {len(events_needing_summaries)} events...")
        
        event_ids = [e['event_id'] for e in events_needing_summaries]
        sum_result = builders.build_summary_requests(event_ids)
        
        if sum_result['request_count'] > 0:
            projected_cost = sum_result.get('projected_cost_usd', 0)
            total_projected_cost += projected_cost
            
            print(f"  ✓ Built {sum_result['request_count']} summary requests")
            print(f"  ✓ Projected cost: ${projected_cost:.4f}")
            
            if total_projected_cost > MAX_USD:
                print(f"  ✗ Projected cost ${total_projected_cost:.4f} exceeds budget ${MAX_USD}")
                sys.exit(1)
            
            # Submit batch
            print("  Submitting summary batch...")
            batch_id = submit.submit_batch(sum_result['file_path'], "summaries")
            batch_ids.append(('summaries', batch_id))
            print(f"  ✓ Submitted batch: {batch_id}")
        else:
            print("  ✓ No summary requests to submit")
    
    print()
    print(f"Total projected cost: ${total_projected_cost:.4f} (budget: ${MAX_USD})")
    print()
    
    if not batch_ids:
        print("No batches submitted. Skipping polling.")
        print("✓ STEP 3: PASS (no work needed)")
        sys.exit(0)
    
    # Poll for completion
    print("Polling for batch completion...")
    completed_batches = []
    
    for batch_type, batch_id in batch_ids:
        print(f"  Polling {batch_type} batch: {batch_id}")
        
        # Poll with timeout
        status = poll.poll_batch_completion(batch_id, max_wait_minutes=30)
        
        if status == 'completed':
            print(f"  ✓ {batch_type} batch completed")
            completed_batches.append((batch_type, batch_id))
        else:
            print(f"  ✗ {batch_type} batch failed or timed out: {status}")
    
    print()
    
    # Merge results
    merge_errors = 0
    
    for batch_type, batch_id in completed_batches:
        print(f"Merging {batch_type} results from batch {batch_id}...")
        
        try:
            if batch_type == 'embeddings':
                merge.merge_embedding_results(batch_id)
            else:  # summaries
                merge.merge_summary_results(batch_id)
            
            print(f"  ✓ Merged {batch_type} results")
            
        except Exception as e:
            print(f"  ✗ Error merging {batch_type}: {e}")
            merge_errors += 1
    
    print()
    
    # Calculate final coverage
    print("Calculating final coverage...")
    
    conn = get_db()
    cur = conn.cursor()
    
    # Get coverage for the events we tried to enrich
    event_ids = [e['event_id'] for e in events]
    placeholders = ','.join(['%s'] * len(event_ids))
    
    cur.execute(f"""
        SELECT 
            COUNT(*) FILTER (WHERE embedding IS NOT NULL) as with_emb,
            COUNT(*) FILTER (WHERE summary_en IS NOT NULL) as with_sum,
            COUNT(*) as total
        FROM events
        WHERE event_id IN ({placeholders})
    """, event_ids)
    
    result = cur.fetchone()
    embedding_coverage = (result[0] / result[2]) * 100 if result[2] > 0 else 0
    summary_coverage = (result[1] / result[2]) * 100 if result[2] > 0 else 0
    
    cur.close()
    conn.close()
    
    print(f"  ✓ Embedding coverage: {embedding_coverage:.1f}% ({result[0]}/{result[2]})")
    print(f"  ✓ Summary coverage: {summary_coverage:.1f}% ({result[1]}/{result[2]})")
    print()
    
    # Write enrichment report (Markdown)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, "mvp_enrichment_report.md")
    with open(report_path, 'w') as f:
        f.write("# MVP Enrichment Report\n\n")
        f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write(f"Cohort size: {len(cohort_event_ids)} events\n\n")
        f.write("## Coverage\n")
        f.write(f"- Embedding coverage: {embedding_coverage:.1f}% ({result[0]}/{result[2]})\n")
        f.write(f"- Summary coverage: {summary_coverage:.1f}% ({result[1]}/{result[2]})\n\n")
        f.write("## Batches\n")
        if batch_ids:
            for btype, bid in batch_ids:
                f.write(f"- {btype}: {bid}\n")
        else:
            f.write("- No batches submitted\n")
        f.write(f"\nTotal projected cost: ${total_projected_cost:.4f} (budget: ${MAX_USD:.2f})\n")
        f.write(f"\nMerge errors: {merge_errors}\n")

    print(f"\n✓ STEP 3: PASS (MVP) – report: {report_path}\n")


if __name__ == "__main__":
    main()
