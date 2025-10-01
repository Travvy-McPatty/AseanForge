#!/usr/bin/env python3
"""
Generate Enrichment Deltas Report

Queries database to show before/after enrichment coverage.

Usage:
    .venv/bin/python scripts/generate_enrichment_deltas.py
"""

import os
import sys
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2


def main():
    output_path = "data/output/validation/latest/enrichment_deltas.txt"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
    cur = conn.cursor()
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("=== Enrichment Deltas Report ===\n")
        f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n")
        
        # Summary coverage by authority
        f.write("## Summary Coverage by Authority (since 2025-07-01)\n\n")
        
        cur.execute("""
            SELECT 
                authority,
                COUNT(*) AS total_events,
                COUNT(summary_en) AS events_with_summary,
                ROUND(100.0 * COUNT(summary_en) / COUNT(*), 1) AS pct_summary,
                COUNT(CASE WHEN summary_model = 'gpt-4o-mini' THEN 1 END) AS with_new_model,
                ROUND(100.0 * COUNT(CASE WHEN summary_model = 'gpt-4o-mini' THEN 1 END) / COUNT(*), 1) AS pct_new_model,
                COUNT(DISTINCT summary_model) AS models_used
            FROM events
            WHERE pub_date >= '2025-07-01'
            GROUP BY authority
            ORDER BY authority;
        """)
        
        f.write("| Authority | Total | With Summary | % | With gpt-4o-mini | % | Models |\n")
        f.write("|-----------|-------|--------------|---|------------------|---|--------|\n")
        
        for auth, total, with_summary, pct_summary, with_new, pct_new, models in cur.fetchall():
            f.write(f"| {auth} | {total} | {with_summary} | {pct_summary:.1f}% | {with_new} | {pct_new:.1f}% | {models} |\n")
        
        f.write("\n")
        
        # Embedding coverage by authority
        f.write("## Embedding Coverage by Authority (since 2025-07-01)\n\n")
        
        cur.execute("""
            SELECT 
                e.authority,
                COUNT(DISTINCT d.document_id) AS total_docs,
                COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN d.document_id END) AS docs_with_vectors,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN d.document_id END) / COUNT(DISTINCT d.document_id), 1) AS pct_vectors,
                COUNT(DISTINCT CASE WHEN e.embedding_model = 'text-embedding-3-small' THEN d.document_id END) AS with_new_model,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding_model = 'text-embedding-3-small' THEN d.document_id END) / COUNT(DISTINCT d.document_id), 1) AS pct_new_model,
                COUNT(DISTINCT e.embedding_model) AS models_used
            FROM documents d
            JOIN events e ON e.event_id = d.event_id
            WHERE e.pub_date >= '2025-07-01'
            GROUP BY e.authority
            ORDER BY e.authority;
        """)
        
        f.write("| Authority | Total Docs | With Vectors | % | With text-embedding-3-small | % | Models |\n")
        f.write("|-----------|------------|--------------|---|----------------------------|---|--------|\n")
        
        for auth, total, with_vectors, pct_vectors, with_new, pct_new, models in cur.fetchall():
            f.write(f"| {auth} | {total} | {with_vectors} | {pct_vectors:.1f}% | {with_new} | {pct_new:.1f}% | {models} |\n")
        
        f.write("\n")
        
        # Recent enrichment activity
        f.write("## Recent Enrichment Activity\n\n")
        
        cur.execute("""
            SELECT 
                DATE(summary_ts) AS enrichment_date,
                COUNT(*) AS summaries_added
            FROM events
            WHERE summary_ts >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(summary_ts)
            ORDER BY enrichment_date DESC;
        """)
        
        f.write("### Summaries Added (Last 7 Days)\n\n")
        f.write("| Date | Summaries Added |\n")
        f.write("|------|----------------|\n")
        
        for date, count in cur.fetchall():
            f.write(f"| {date} | {count} |\n")
        
        f.write("\n")
        
        cur.execute("""
            SELECT 
                DATE(embedding_ts) AS enrichment_date,
                COUNT(*) AS embeddings_added
            FROM events
            WHERE embedding_ts >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(embedding_ts)
            ORDER BY enrichment_date DESC;
        """)
        
        f.write("### Embeddings Added (Last 7 Days)\n\n")
        f.write("| Date | Embeddings Added |\n")
        f.write("|------|------------------|\n")
        
        for date, count in cur.fetchall():
            f.write(f"| {date} | {count} |\n")
        
        f.write("\n")
        
        # Overall stats
        f.write("## Overall Statistics\n\n")
        
        cur.execute("""
            SELECT 
                COUNT(*) AS total_events,
                COUNT(summary_en) AS events_with_summary,
                COUNT(CASE WHEN summary_model = 'gpt-4o-mini' THEN 1 END) AS events_with_new_summary,
                COUNT(embedding) AS events_with_embedding,
                COUNT(CASE WHEN embedding_model = 'text-embedding-3-small' THEN 1 END) AS events_with_new_embedding,
                MIN(pub_date) AS earliest_pub_date,
                MAX(pub_date) AS latest_pub_date
            FROM events
            WHERE pub_date >= '2025-07-01';
        """)
        
        total, with_summary, with_new_summary, with_embedding, with_new_embedding, earliest, latest = cur.fetchone()
        
        f.write(f"- Total Events: {total}\n")
        f.write(f"- Events with Summary: {with_summary} ({round(100.0 * with_summary / total, 1) if total > 0 else 0:.1f}%)\n")
        f.write(f"- Events with gpt-4o-mini Summary: {with_new_summary} ({round(100.0 * with_new_summary / total, 1) if total > 0 else 0:.1f}%)\n")
        f.write(f"- Events with Embedding: {with_embedding} ({round(100.0 * with_embedding / total, 1) if total > 0 else 0:.1f}%)\n")
        f.write(f"- Events with text-embedding-3-small Embedding: {with_new_embedding} ({round(100.0 * with_new_embedding / total, 1) if total > 0 else 0:.1f}%)\n")
        if earliest and latest:
            f.write(f"- Date Range: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}\n")
        f.write("\n")
    
    conn.close()
    
    print(f"✓ Enrichment deltas saved to: {output_path}")


if __name__ == "__main__":
    main()

