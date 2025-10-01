#!/usr/bin/env python3
"""
Backfill Model Tracking on Legacy Rows

Marks legacy rows that already have summaries/embeddings but lack model tracking.

Usage:
    .venv/bin/python scripts/backfill_model_tracking.py
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
    try:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
        cur = conn.cursor()
        
        print("=== Step 1: Backfill Model Tracking ===\n")
        
        # A. Summary Model Backfill
        print("A. Summary Model Backfill")
        
        # Count rows needing backfill
        cur.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE summary_en IS NOT NULL 
              AND summary_model IS NULL;
        """)
        summary_backfill_count = cur.fetchone()[0]
        print(f"  Found {summary_backfill_count} events needing summary model backfill")
        
        # Perform backfill
        cur.execute("""
            UPDATE events 
            SET summary_model = 'legacy-prebatch',
                summary_version = 'v0',
                summary_ts = NOW()
            WHERE summary_en IS NOT NULL 
              AND summary_model IS NULL;
        """)
        conn.commit()
        print(f"  ✓ Updated {summary_backfill_count} events with summary_model='legacy-prebatch'")
        
        # B. Embedding Model Backfill
        print("\nB. Embedding Model Backfill")
        
        # Count rows needing backfill
        cur.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE embedding IS NOT NULL 
              AND embedding_model IS NULL;
        """)
        embedding_backfill_count = cur.fetchone()[0]
        print(f"  Found {embedding_backfill_count} events needing embedding model backfill")
        
        # Perform backfill
        cur.execute("""
            UPDATE events 
            SET embedding_model = 'legacy-prebatch',
                embedding_version = 'v0',
                embedding_ts = NOW()
            WHERE embedding IS NOT NULL 
              AND embedding_model IS NULL;
        """)
        conn.commit()
        print(f"  ✓ Updated {embedding_backfill_count} events with embedding_model='legacy-prebatch'")
        
        # Verify coverage
        print("\n=== Verification ===\n")
        
        # Overall summary coverage
        cur.execute("""
            SELECT 
                COUNT(*) AS total_with_summary,
                COUNT(summary_model) AS total_with_model,
                ROUND(100.0 * COUNT(summary_model) / COUNT(*), 2) AS pct
            FROM events
            WHERE summary_en IS NOT NULL;
        """)
        total_summary, total_model, pct = cur.fetchone()
        print(f"Overall Summary Coverage: {total_model}/{total_summary} ({pct}%)")
        
        overall_summary_pass = pct >= 98.0
        print(f"  {'✓ PASS' if overall_summary_pass else '❌ FAIL'}: Overall summary coverage ≥98%")
        
        # Per-authority summary coverage
        print("\nPer-Authority Summary Coverage:")
        
        cur.execute("""
            SELECT 
                authority,
                COUNT(*) AS total_with_summary,
                COUNT(summary_model) AS total_with_model,
                ROUND(100.0 * COUNT(summary_model) / COUNT(*), 2) AS pct
            FROM events
            WHERE summary_en IS NOT NULL
            GROUP BY authority
            ORDER BY authority;
        """)
        
        per_auth_summary_pass = True
        failing_authorities = []
        
        for auth, total_summary, total_model, pct in cur.fetchall():
            status = "✓" if pct >= 90.0 else "❌"
            print(f"  {status} {auth}: {total_model}/{total_summary} ({pct}%)")
            if pct < 90.0:
                per_auth_summary_pass = False
                failing_authorities.append((auth, pct))
        
        print(f"\n  {'✓ PASS' if per_auth_summary_pass else '❌ FAIL'}: All authorities ≥90% summary coverage")
        
        # Overall embedding coverage
        cur.execute("""
            SELECT 
                COUNT(DISTINCT d.document_id) AS total_with_embedding,
                COUNT(DISTINCT CASE WHEN e.embedding_model IS NOT NULL THEN d.document_id END) AS total_with_model,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.embedding_model IS NOT NULL THEN d.document_id END) / COUNT(DISTINCT d.document_id), 2) AS pct
            FROM documents d
            JOIN events e ON e.event_id = d.event_id
            WHERE e.embedding IS NOT NULL;
        """)
        total_embedding, total_model, pct = cur.fetchone()
        print(f"\nOverall Embedding Coverage: {total_model}/{total_embedding} ({pct}%)")
        
        overall_embedding_pass = pct >= 98.0
        print(f"  {'✓ PASS' if overall_embedding_pass else '❌ FAIL'}: Overall embedding coverage ≥98%")
        
        # Final verdict
        all_pass = overall_summary_pass and per_auth_summary_pass and overall_embedding_pass
        
        print(f"\n{'='*50}")
        print(f"Step 1 Result: {'✓ PASS' if all_pass else '❌ FAIL'}")
        print(f"{'='*50}\n")
        
        conn.close()
        
        if not all_pass:
            # Create blockers.md
            blockers_path = "data/output/validation/latest/blockers.md"
            os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
            
            with open(blockers_path, "w", encoding="utf-8") as f:
                f.write("# Step 1: Backfill Model Tracking - FAILED\n\n")
                f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
                
                if not overall_summary_pass:
                    f.write(f"## Overall Summary Coverage FAILED\n\n")
                    f.write(f"- Expected: ≥98%\n")
                    f.write(f"- Actual: {pct}%\n\n")
                
                if not per_auth_summary_pass:
                    f.write(f"## Per-Authority Summary Coverage FAILED\n\n")
                    f.write(f"Authorities below 90% threshold:\n\n")
                    for auth, pct in failing_authorities:
                        f.write(f"- {auth}: {pct}%\n")
                    f.write("\n")
                
                if not overall_embedding_pass:
                    f.write(f"## Overall Embedding Coverage FAILED\n\n")
                    f.write(f"- Expected: ≥98%\n")
                    f.write(f"- Actual: {pct}%\n\n")
            
            print(f"❌ Blockers documented in: {blockers_path}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ FAIL: {e}")
        
        # Create blockers.md
        blockers_path = "data/output/validation/latest/blockers.md"
        os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
        
        with open(blockers_path, "w", encoding="utf-8") as f:
            f.write("# Step 1: Backfill Model Tracking - FAILED\n\n")
            f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(f"**Error**: {str(e)}\n\n")
        
        print(f"\n❌ Blockers documented in: {blockers_path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

