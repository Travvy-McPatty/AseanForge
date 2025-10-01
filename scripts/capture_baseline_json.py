#!/usr/bin/env python3
"""
Capture Baseline Metrics for Coverage Gap Closure Pipeline

Queries database for all 13 authorities and captures:
- Total events count
- Events with non-NULL summary_en
- Events with non-NULL summary_model
- Documents with non-NULL embedding (vector)
- Documents with non-NULL embedding_model
- Events with zero associated documents

Saves to baseline_counts.json with per-authority breakdown and global totals.

Usage:
    .venv/bin/python scripts/capture_baseline_json.py
"""

import os
import sys
import json
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2


AUTHORITIES = [
    "ASEAN", "BI", "BOT", "BSP", "DICT", 
    "IMDA", "MAS", "MCMC", "MIC", "OJK", 
    "PDPC", "SBV", "SC"
]


def main():
    output_path = "data/output/validation/latest/baseline_counts.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
        cur = conn.cursor()
        
        # Verify schema elements
        print("Verifying database schema...")
        
        # Check unique index
        cur.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'events' 
              AND indexname = 'events_unique_hash';
        """)
        if not cur.fetchone():
            print("❌ FAIL: Unique index 'events_unique_hash' not found")
            sys.exit(1)
        print("  ✓ Unique index 'events_unique_hash' exists")
        
        # Check enrichment columns
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'events' 
              AND column_name IN ('summary_model', 'summary_ts', 'summary_version', 
                                  'embedding_model', 'embedding_ts', 'embedding_version');
        """)
        enrichment_cols = [row[0] for row in cur.fetchall()]
        if len(enrichment_cols) != 6:
            print(f"❌ FAIL: Expected 6 enrichment columns, found {len(enrichment_cols)}")
            sys.exit(1)
        print("  ✓ All 6 enrichment columns exist")
        
        # Capture metrics
        print("\nCapturing baseline metrics...")
        
        baseline = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "authorities": {},
            "global": {}
        }
        
        # Per-authority metrics
        for authority in AUTHORITIES:
            cur.execute("""
                SELECT 
                    COUNT(*) AS total_events,
                    COUNT(summary_en) AS events_with_summary,
                    COUNT(summary_model) AS events_with_summary_model,
                    COUNT(embedding) AS events_with_embedding,
                    COUNT(embedding_model) AS events_with_embedding_model
                FROM events
                WHERE authority = %s;
            """, (authority,))
            
            total, with_summary, with_summary_model, with_embedding, with_embedding_model = cur.fetchone()
            
            # Count documents
            cur.execute("""
                SELECT COUNT(DISTINCT d.document_id)
                FROM documents d
                JOIN events e ON e.event_id = d.event_id
                WHERE e.authority = %s;
            """, (authority,))
            total_docs = cur.fetchone()[0]
            
            # Count documents with embeddings
            cur.execute("""
                SELECT COUNT(DISTINCT d.document_id)
                FROM documents d
                JOIN events e ON e.event_id = d.event_id
                WHERE e.authority = %s
                  AND e.embedding IS NOT NULL;
            """, (authority,))
            docs_with_embedding = cur.fetchone()[0]
            
            # Count documents with embedding_model
            cur.execute("""
                SELECT COUNT(DISTINCT d.document_id)
                FROM documents d
                JOIN events e ON e.event_id = d.event_id
                WHERE e.authority = %s
                  AND e.embedding_model IS NOT NULL;
            """, (authority,))
            docs_with_embedding_model = cur.fetchone()[0]
            
            # Count events with zero documents
            cur.execute("""
                SELECT COUNT(*)
                FROM events e
                WHERE e.authority = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM documents d WHERE d.event_id = e.event_id
                  );
            """, (authority,))
            events_without_docs = cur.fetchone()[0]
            
            baseline["authorities"][authority] = {
                "total_events": total,
                "events_with_summary": with_summary,
                "events_with_summary_model": with_summary_model,
                "events_with_embedding": with_embedding,
                "events_with_embedding_model": with_embedding_model,
                "total_documents": total_docs,
                "docs_with_embedding": docs_with_embedding,
                "docs_with_embedding_model": docs_with_embedding_model,
                "events_without_docs": events_without_docs,
                "summary_coverage_pct": round(100.0 * with_summary / total, 2) if total > 0 else 0,
                "summary_model_coverage_pct": round(100.0 * with_summary_model / total, 2) if total > 0 else 0,
                "doc_coverage_pct": round(100.0 * (total - events_without_docs) / total, 2) if total > 0 else 0,
                "embedding_model_coverage_pct": round(100.0 * docs_with_embedding_model / total_docs, 2) if total_docs > 0 else 0
            }
        
        # Global totals
        cur.execute("""
            SELECT 
                COUNT(*) AS total_events,
                COUNT(summary_en) AS events_with_summary,
                COUNT(summary_model) AS events_with_summary_model,
                COUNT(embedding) AS events_with_embedding,
                COUNT(embedding_model) AS events_with_embedding_model
            FROM events;
        """)
        
        total, with_summary, with_summary_model, with_embedding, with_embedding_model = cur.fetchone()
        
        cur.execute("SELECT COUNT(*) FROM documents;")
        total_docs = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM documents d JOIN events e ON e.event_id = d.event_id WHERE e.embedding IS NOT NULL;")
        docs_with_embedding = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM documents d JOIN events e ON e.event_id = d.event_id WHERE e.embedding_model IS NOT NULL;")
        docs_with_embedding_model = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*)
            FROM events e
            WHERE NOT EXISTS (
                SELECT 1 FROM documents d WHERE d.event_id = e.event_id
            );
        """)
        events_without_docs = cur.fetchone()[0]
        
        baseline["global"] = {
            "total_events": total,
            "events_with_summary": with_summary,
            "events_with_summary_model": with_summary_model,
            "events_with_embedding": with_embedding,
            "events_with_embedding_model": with_embedding_model,
            "total_documents": total_docs,
            "docs_with_embedding": docs_with_embedding,
            "docs_with_embedding_model": docs_with_embedding_model,
            "events_without_docs": events_without_docs,
            "summary_coverage_pct": round(100.0 * with_summary / total, 2) if total > 0 else 0,
            "summary_model_coverage_pct": round(100.0 * with_summary_model / total, 2) if total > 0 else 0,
            "doc_coverage_pct": round(100.0 * (total - events_without_docs) / total, 2) if total > 0 else 0,
            "embedding_model_coverage_pct": round(100.0 * docs_with_embedding_model / total_docs, 2) if total_docs > 0 else 0
        }
        
        # Save to JSON
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(baseline, f, indent=2)
        
        conn.close()
        
        print(f"\n✓ Baseline counts saved to: {output_path}")
        print(f"\nGlobal Summary:")
        print(f"  Total Events: {baseline['global']['total_events']}")
        print(f"  Events with Summary: {baseline['global']['events_with_summary']} ({baseline['global']['summary_coverage_pct']}%)")
        print(f"  Events with Summary Model: {baseline['global']['events_with_summary_model']} ({baseline['global']['summary_model_coverage_pct']}%)")
        print(f"  Total Documents: {baseline['global']['total_documents']}")
        print(f"  Docs with Embedding Model: {baseline['global']['docs_with_embedding_model']} ({baseline['global']['embedding_model_coverage_pct']}%)")
        print(f"  Events without Docs: {baseline['global']['events_without_docs']}")
        
        return 0
        
    except Exception as e:
        print(f"❌ FAIL: {e}")
        
        # Create blockers.md
        blockers_path = "data/output/validation/latest/blockers.md"
        os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
        
        with open(blockers_path, "w", encoding="utf-8") as f:
            f.write("# Step 0: Baseline Metrics - FAILED\n\n")
            f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(f"**Error**: {str(e)}\n\n")
            f.write("## Details\n\n")
            f.write("Database connectivity or schema verification failed.\n")
        
        print(f"\n❌ Blockers documented in: {blockers_path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

