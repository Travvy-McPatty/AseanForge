#!/usr/bin/env python3
"""
Batch Results Merger

Parse results JSONL and upsert to database with idempotency.
"""

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


def get_db():
    """Get database connection."""
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL not set")
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def merge_embeddings(results_jsonl_path: str) -> Dict:
    """
    Merge embedding results into database.
    
    Args:
        results_jsonl_path: Path to results JSONL file
    
    Returns:
        Stats dict with upserted_count, skipped_count, error_count
    """
    if not os.path.exists(results_jsonl_path):
        raise FileNotFoundError(f"Results file not found: {results_jsonl_path}")
    
    embed_model = os.getenv("EMBED_MODEL", "text-embedding-3-small")
    embed_version = f"{embed_model}-v1"
    
    conn = get_db()
    cur = conn.cursor()
    
    upserted_count = 0
    skipped_count = 0
    error_count = 0
    
    print(f"Merging embeddings from {results_jsonl_path}...")
    
    with open(results_jsonl_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            try:
                result = json.loads(line)
                
                # Parse custom_id: "emb:<document_id>:<chunk_idx>"
                custom_id = result.get("custom_id", "")
                parts = custom_id.split(":")
                
                if len(parts) != 3 or parts[0] != "emb":
                    print(f"  WARNING: Invalid custom_id format: {custom_id}")
                    error_count += 1
                    continue
                
                document_id = parts[1]
                chunk_idx = int(parts[2])
                
                # Extract embedding vector
                response_body = result.get("response", {}).get("body", {})
                data = response_body.get("data", [])
                
                if not data or len(data) == 0:
                    print(f"  WARNING: No embedding data for {custom_id}")
                    error_count += 1
                    continue
                
                embedding = data[0].get("embedding", [])
                
                if not embedding:
                    print(f"  WARNING: Empty embedding for {custom_id}")
                    error_count += 1
                    continue
                
                # For now, we'll use the first chunk's embedding (chunk_idx == 0)
                # In a more sophisticated implementation, we could average all chunks
                if chunk_idx != 0:
                    skipped_count += 1
                    continue
                
                # Convert to PostgreSQL vector format
                embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"
                
                # Upsert to events table (join via documents.event_id)
                cur.execute("""
                    UPDATE events SET
                        embedding = %s::vector,
                        embedding_model = %s,
                        embedding_ts = NOW(),
                        embedding_version = %s
                    WHERE event_id = (
                        SELECT event_id FROM documents WHERE document_id = %s::uuid
                    )
                    AND (embedding_model IS NULL OR embedding_model != %s)
                """, (embedding_str, embed_model, embed_version, document_id, embed_model))
                
                if cur.rowcount > 0:
                    upserted_count += 1
                else:
                    skipped_count += 1
                
                if (line_num % 100) == 0:
                    print(f"  Processed {line_num} results... (upserted: {upserted_count}, skipped: {skipped_count})")
            
            except Exception as e:
                print(f"  ERROR processing line {line_num}: {e}")
                error_count += 1
    
    cur.close()
    conn.close()
    
    stats = {
        "upserted_count": upserted_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "total_processed": upserted_count + skipped_count + error_count
    }
    
    print(f"\nEmbedding merge complete:")
    print(f"  Upserted: {upserted_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    
    return stats


def merge_summaries(results_jsonl_path: str) -> Dict:
    """
    Merge summary results into database.
    
    Args:
        results_jsonl_path: Path to results JSONL file
    
    Returns:
        Stats dict with upserted_count, skipped_count, error_count
    """
    if not os.path.exists(results_jsonl_path):
        raise FileNotFoundError(f"Results file not found: {results_jsonl_path}")
    
    summary_model = os.getenv("SUMMARY_MODEL", "gpt-4o-mini")
    summary_version = f"{summary_model}-v1"
    
    conn = get_db()
    cur = conn.cursor()
    
    upserted_count = 0
    skipped_count = 0
    error_count = 0
    
    print(f"Merging summaries from {results_jsonl_path}...")
    
    with open(results_jsonl_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            try:
                result = json.loads(line)
                
                # Parse custom_id: "sum:<event_id>"
                custom_id = result.get("custom_id", "")
                parts = custom_id.split(":")
                
                if len(parts) != 2 or parts[0] != "sum":
                    print(f"  WARNING: Invalid custom_id format: {custom_id}")
                    error_count += 1
                    continue
                
                event_id = parts[1]
                
                # Extract summary text
                response_body = result.get("response", {}).get("body", {})
                choices = response_body.get("choices", [])
                
                if not choices or len(choices) == 0:
                    print(f"  WARNING: No choices for {custom_id}")
                    error_count += 1
                    continue
                
                message = choices[0].get("message", {})
                summary_text = message.get("content", "").strip()
                
                if not summary_text:
                    print(f"  WARNING: Empty summary for {custom_id}")
                    error_count += 1
                    continue
                
                # Upsert to events table
                cur.execute("""
                    UPDATE events SET
                        summary_en = %s,
                        summary_model = %s,
                        summary_ts = NOW(),
                        summary_version = %s
                    WHERE event_id = %s::uuid
                    AND (summary_model IS NULL OR summary_model != %s)
                """, (summary_text, summary_model, summary_version, event_id, summary_model))
                
                if cur.rowcount > 0:
                    upserted_count += 1
                else:
                    skipped_count += 1
                
                if (line_num % 100) == 0:
                    print(f"  Processed {line_num} results... (upserted: {upserted_count}, skipped: {skipped_count})")
            
            except Exception as e:
                print(f"  ERROR processing line {line_num}: {e}")
                error_count += 1
    
    cur.close()
    conn.close()
    
    stats = {
        "upserted_count": upserted_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "total_processed": upserted_count + skipped_count + error_count
    }
    
    print(f"\nSummary merge complete:")
    print(f"  Upserted: {upserted_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    
    return stats

