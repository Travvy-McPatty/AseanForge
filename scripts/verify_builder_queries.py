#!/usr/bin/env python3
"""
Verify Enrichment Builder Queries

Runs dry-run builds and compares SQL counts vs JSONL line counts.

Usage:
    .venv/bin/python scripts/verify_builder_queries.py
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2


def count_jsonl_lines(filepath):
    """Count lines in JSONL file."""
    if not os.path.exists(filepath):
        return 0
    with open(filepath, 'r') as f:
        return sum(1 for _ in f)


def main():
    try:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
        cur = conn.cursor()
        
        print("=== Step 2: Verify Enrichment Builder Queries ===\n")
        
        # Get model names from env
        summary_model = os.getenv("SUMMARY_MODEL", "gpt-4o-mini")
        embed_model = os.getenv("EMBED_MODEL", "text-embedding-3-small")
        
        print(f"Summary Model: {summary_model}")
        print(f"Embed Model: {embed_model}\n")
        
        # A. Embeddings Builder
        print("A. Embeddings Builder")
        
        # SQL count
        cur.execute("""
            SELECT COUNT(*)
            FROM documents d
            JOIN events e ON e.event_id = d.event_id
            WHERE (e.embedding IS NULL OR e.embedding_model != %s OR e.embedding_model IS NULL)
              AND d.clean_text IS NOT NULL
              AND LENGTH(d.clean_text) > 100
              AND e.pub_date >= '2025-07-01';
        """, (embed_model,))
        sql_count_embeddings = cur.fetchone()[0]
        print(f"  SQL count: {sql_count_embeddings}")
        
        # Run dry-run build
        print("  Running dry-run build...")
        result = subprocess.run([
            ".venv/bin/python", "-m", "app.enrich_batch.cli", "build",
            "--kind", "embeddings",
            "--since", "2025-07-01",
            "--out", "data/batch/dryrun_embeddings.jsonl"
        ], capture_output=True, text=True, cwd="/Users/travispaterson/Documents/augment-projects/AseanForge")
        
        if result.returncode != 0:
            print(f"  ❌ Build failed: {result.stderr}")
            raise Exception(f"Embeddings build failed: {result.stderr}")
        
        # Count JSONL lines
        jsonl_count_embeddings = count_jsonl_lines("data/batch/dryrun_embeddings.jsonl")
        print(f"  JSONL count: {jsonl_count_embeddings}")
        
        # Calculate difference
        if sql_count_embeddings > 0:
            diff_pct_embeddings = abs(jsonl_count_embeddings - sql_count_embeddings) / sql_count_embeddings
        else:
            diff_pct_embeddings = 0.0
        
        print(f"  Difference: {abs(jsonl_count_embeddings - sql_count_embeddings)} ({diff_pct_embeddings*100:.2f}%)")
        
        embeddings_pass = diff_pct_embeddings <= 0.01
        print(f"  {'✓ PASS' if embeddings_pass else '❌ FAIL'}: Difference ≤1%")
        
        # B. Summaries Builder
        print("\nB. Summaries Builder")

        # SQL count (matching builder logic: needs clean_text or title with ≥50 chars)
        cur.execute("""
            SELECT COUNT(*)
            FROM events e
            LEFT JOIN documents d ON d.event_id = e.event_id
            WHERE (e.summary_en IS NULL OR e.summary_model != %s OR e.summary_model IS NULL)
              AND e.pub_date >= '2025-07-01'
              AND (
                  (d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= 50)
                  OR (d.clean_text IS NULL AND e.title IS NOT NULL AND LENGTH(e.title) >= 50)
              );
        """, (summary_model,))
        sql_count_summaries = cur.fetchone()[0]
        print(f"  SQL count: {sql_count_summaries}")
        
        # Run dry-run build
        print("  Running dry-run build...")
        result = subprocess.run([
            ".venv/bin/python", "-m", "app.enrich_batch.cli", "build",
            "--kind", "summaries",
            "--since", "2025-07-01",
            "--out", "data/batch/dryrun_summaries.jsonl"
        ], capture_output=True, text=True, cwd="/Users/travispaterson/Documents/augment-projects/AseanForge")
        
        if result.returncode != 0:
            print(f"  ❌ Build failed: {result.stderr}")
            raise Exception(f"Summaries build failed: {result.stderr}")
        
        # Count JSONL lines
        jsonl_count_summaries = count_jsonl_lines("data/batch/dryrun_summaries.jsonl")
        print(f"  JSONL count: {jsonl_count_summaries}")
        
        # Calculate difference
        if sql_count_summaries > 0:
            diff_pct_summaries = abs(jsonl_count_summaries - sql_count_summaries) / sql_count_summaries
        else:
            diff_pct_summaries = 0.0
        
        print(f"  Difference: {abs(jsonl_count_summaries - sql_count_summaries)} ({diff_pct_summaries*100:.2f}%)")
        
        summaries_pass = diff_pct_summaries <= 0.01
        print(f"  {'✓ PASS' if summaries_pass else '❌ FAIL'}: Difference ≤1%")
        
        # Create build plan
        build_plan = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "embeddings": {
                "sql_count": sql_count_embeddings,
                "jsonl_count": jsonl_count_embeddings,
                "difference": jsonl_count_embeddings - sql_count_embeddings,
                "difference_pct": round(diff_pct_embeddings * 100, 2),
                "pass": embeddings_pass
            },
            "summaries": {
                "sql_count": sql_count_summaries,
                "jsonl_count": jsonl_count_summaries,
                "difference": jsonl_count_summaries - sql_count_summaries,
                "difference_pct": round(diff_pct_summaries * 100, 2),
                "pass": summaries_pass
            }
        }
        
        build_plan_path = "data/output/validation/latest/build_plan.json"
        os.makedirs(os.path.dirname(build_plan_path), exist_ok=True)
        with open(build_plan_path, "w") as f:
            json.dump(build_plan, f, indent=2)
        
        print(f"\n✓ Build plan saved to: {build_plan_path}")
        
        # Final verdict
        all_pass = embeddings_pass and summaries_pass
        
        print(f"\n{'='*50}")
        print(f"Step 2 Result: {'✓ PASS' if all_pass else '❌ FAIL'}")
        print(f"{'='*50}\n")
        
        conn.close()
        
        if not all_pass:
            # Create blockers.md
            blockers_path = "data/output/validation/latest/blockers.md"
            os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
            
            with open(blockers_path, "w", encoding="utf-8") as f:
                f.write("# Step 2: Builder Patch Validation - FAILED\n\n")
                f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
                
                if not embeddings_pass:
                    f.write(f"## Embeddings Builder FAILED\n\n")
                    f.write(f"- SQL count: {sql_count_embeddings}\n")
                    f.write(f"- JSONL count: {jsonl_count_embeddings}\n")
                    f.write(f"- Difference: {abs(jsonl_count_embeddings - sql_count_embeddings)} ({diff_pct_embeddings*100:.2f}%)\n")
                    f.write(f"- Expected: ≤1%\n\n")
                
                if not summaries_pass:
                    f.write(f"## Summaries Builder FAILED\n\n")
                    f.write(f"- SQL count: {sql_count_summaries}\n")
                    f.write(f"- JSONL count: {jsonl_count_summaries}\n")
                    f.write(f"- Difference: {abs(jsonl_count_summaries - sql_count_summaries)} ({diff_pct_summaries*100:.2f}%)\n")
                    f.write(f"- Expected: ≤1%\n\n")
            
            print(f"❌ Blockers documented in: {blockers_path}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ FAIL: {e}")
        
        # Create blockers.md
        blockers_path = "data/output/validation/latest/blockers.md"
        os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
        
        with open(blockers_path, "w", encoding="utf-8") as f:
            f.write("# Step 2: Builder Patch Validation - FAILED\n\n")
            f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(f"**Error**: {str(e)}\n\n")
        
        print(f"\n❌ Blockers documented in: {blockers_path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

