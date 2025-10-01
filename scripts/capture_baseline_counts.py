#!/usr/bin/env python3
"""
Capture Baseline Database Counts

Queries events and documents tables to capture pre-enrichment baseline.

Usage:
    .venv/bin/python scripts/capture_baseline_counts.py [--authorities MAS,IMDA] [--since 2025-08-01]
"""

import argparse
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
    parser = argparse.ArgumentParser(description="Capture baseline database counts")
    parser.add_argument("--authorities", type=str, help="Comma-separated list of authorities (default: all)")
    parser.add_argument("--since", type=str, help="YYYY-MM-DD (default: all dates)")
    parser.add_argument("--output", type=str, default="data/output/validation/latest/db_counts_before.txt",
                        help="Output file path")
    args = parser.parse_args()
    
    output_dir = os.path.dirname(args.output)
    os.makedirs(output_dir, exist_ok=True)
    
    # Parse authorities
    authorities = None
    if args.authorities:
        authorities = [a.strip().upper() for a in args.authorities.split(",")]
    
    # Connect to database
    conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
    cur = conn.cursor()
    
    # Build WHERE clause
    where_clauses = []
    params = []
    
    if authorities:
        placeholders = ",".join(["%s"] * len(authorities))
        where_clauses.append(f"authority IN ({placeholders})")
        params.extend(authorities)
    
    if args.since:
        where_clauses.append("pub_date >= %s")
        params.append(args.since)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Open output file
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(f"=== Baseline Database Counts ===\n")
        f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
        if authorities:
            f.write(f"Authorities: {', '.join(authorities)}\n")
        if args.since:
            f.write(f"Since: {args.since}\n")
        f.write("\n")
        
        # Query 1: Summary coverage by authority
        f.write("## Summary Coverage by Authority\n\n")
        
        cur.execute(f"""
            SELECT 
                authority,
                COUNT(*) AS total_events,
                COUNT(summary_en) AS events_with_summary,
                ROUND(100.0 * COUNT(summary_en) / NULLIF(COUNT(*), 0), 1) AS pct_summary,
                COUNT(DISTINCT summary_model) AS models_used
            FROM events
            WHERE {where_sql}
            GROUP BY authority
            ORDER BY authority;
        """, params)
        
        rows = cur.fetchall()
        
        f.write("| Authority | Total Events | With Summary | % Summary | Models Used |\n")
        f.write("|-----------|--------------|--------------|-----------|-------------|\n")
        
        total_events = 0
        total_with_summary = 0
        
        for auth, total, with_summary, pct, models in rows:
            f.write(f"| {auth} | {total:,} | {with_summary:,} | {pct or 0:.1f}% | {models or 0} |\n")
            total_events += total
            total_with_summary += with_summary or 0
        
        overall_pct = round(100.0 * total_with_summary / total_events, 1) if total_events > 0 else 0
        f.write(f"| **TOTAL** | **{total_events:,}** | **{total_with_summary:,}** | **{overall_pct:.1f}%** | - |\n")
        f.write("\n")
        
        # Query 2: Embedding coverage by authority
        f.write("## Embedding Coverage by Authority\n\n")
        
        cur.execute(f"""
            SELECT 
                e.authority,
                COUNT(DISTINCT d.document_id) AS total_docs,
                COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN d.document_id END) AS docs_with_vectors,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding IS NOT NULL THEN d.document_id END) / NULLIF(COUNT(DISTINCT d.document_id), 0), 1) AS pct_vectors,
                COUNT(DISTINCT e.embedding_model) AS models_used
            FROM documents d
            JOIN events e ON e.event_id = d.event_id
            WHERE {where_sql}
            GROUP BY e.authority
            ORDER BY e.authority;
        """, params)
        
        rows = cur.fetchall()
        
        f.write("| Authority | Total Docs | With Vectors | % Vectors | Models Used |\n")
        f.write("|-----------|------------|--------------|-----------|-------------|\n")
        
        total_docs = 0
        total_with_vectors = 0
        
        for auth, total, with_vectors, pct, models in rows:
            f.write(f"| {auth} | {total:,} | {with_vectors:,} | {pct or 0:.1f}% | {models or 0} |\n")
            total_docs += total
            total_with_vectors += with_vectors or 0
        
        overall_pct = round(100.0 * total_with_vectors / total_docs, 1) if total_docs > 0 else 0
        f.write(f"| **TOTAL** | **{total_docs:,}** | **{total_with_vectors:,}** | **{overall_pct:.1f}%** | - |\n")
        f.write("\n")
        
        # Query 3: Overall stats
        f.write("## Overall Statistics\n\n")
        
        cur.execute(f"""
            SELECT 
                COUNT(*) AS total_events,
                COUNT(summary_en) AS events_with_summary,
                COUNT(embedding) AS events_with_embedding,
                MIN(pub_date) AS earliest_pub_date,
                MAX(pub_date) AS latest_pub_date
            FROM events
            WHERE {where_sql};
        """, params)
        
        total, with_summary, with_embedding, earliest, latest = cur.fetchone()
        
        f.write(f"- Total Events: {total:,}\n")
        f.write(f"- Events with Summary: {with_summary:,} ({round(100.0 * with_summary / total, 1) if total > 0 else 0:.1f}%)\n")
        f.write(f"- Events with Embedding: {with_embedding:,} ({round(100.0 * with_embedding / total, 1) if total > 0 else 0:.1f}%)\n")
        if earliest and latest:
            f.write(f"- Date Range: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}\n")
        f.write("\n")
    
    conn.close()
    
    print(f"✓ Baseline counts saved to: {args.output}")
    
    # Print summary to stdout
    print()
    print("Summary:")
    print(f"  Total Events: {total_events:,}")
    print(f"  Events with Summary: {total_with_summary:,} ({overall_pct:.1f}%)")
    print(f"  Total Documents: {total_docs:,}")
    print(f"  Documents with Vectors: {total_with_vectors:,}")


if __name__ == "__main__":
    main()

