#!/usr/bin/env python3
"""
Batch Request Builders

Build JSONL request files for OpenAI Batch API (embeddings and summaries).
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
import tiktoken


def get_db():
    """Get database connection."""
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL not set")
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def estimate_tokens(text: str, model: str = "gpt-4o-mini") -> int:
    """Estimate token count for text using tiktoken."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
    
    return len(encoding.encode(text))


def chunk_text(text: str, max_tokens: int = 1500, overlap_pct: float = 0.10) -> List[str]:
    """
    Chunk text into segments with overlap.
    
    Args:
        text: Text to chunk
        max_tokens: Maximum tokens per chunk
        overlap_pct: Overlap percentage (0.10 = 10%)
    
    Returns:
        List of text chunks
    """
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(text)
    
    if len(tokens) <= max_tokens:
        return [text]
    
    chunks = []
    overlap_tokens = int(max_tokens * overlap_pct)
    step = max_tokens - overlap_tokens
    
    for i in range(0, len(tokens), step):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = encoding.decode(chunk_tokens)
        chunks.append(chunk_text)
        
        if i + max_tokens >= len(tokens):
            break
    
    return chunks


def build_embedding_requests(
    since_date: Optional[str] = None,
    limit: Optional[int] = None,
    output_path: str = "data/batch/embeddings.requests.jsonl",
    authorities: Optional[List[str]] = None
) -> Dict:
    """
    Build JSONL request file for embeddings.

    Args:
        since_date: Filter events since this date (YYYY-MM-DD)
        limit: Maximum number of documents to process
        output_path: Path to write JSONL file
        authorities: Filter by authority codes (e.g., ['MAS', 'IMDA'])

    Returns:
        Metadata dict with file_path, request_count, estimated_tokens, projected_cost_usd
    """
    embed_model = os.getenv("EMBED_MODEL", "text-embedding-3-small")
    max_requests = int(os.getenv("BATCH_MAX_REQUESTS", "20000"))
    max_file_mb = int(os.getenv("BATCH_MAX_FILE_MB", "100"))
    
    conn = get_db()
    cur = conn.cursor()
    
    # Query documents needing embeddings
    query = """
        SELECT d.document_id, d.clean_text, e.event_id, e.embedding_model, e.authority
        FROM documents d
        JOIN events e ON e.event_id = d.event_id
        WHERE (e.embedding IS NULL OR e.embedding_model != %s OR e.embedding_model IS NULL)
          AND d.clean_text IS NOT NULL
          AND LENGTH(d.clean_text) > 100
    """

    params = [embed_model]

    if since_date:
        query += " AND e.pub_date >= %s"
        params.append(since_date)

    if authorities:
        placeholders = ",".join(["%s"] * len(authorities))
        query += f" AND e.authority IN ({placeholders})"
        params.extend(authorities)
    
    query += " ORDER BY e.pub_date DESC"
    
    if limit:
        query += f" LIMIT {int(limit)}"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    print(f"Found {len(rows)} documents needing embeddings")
    
    # Build JSONL requests
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    request_count = 0
    total_tokens = 0
    file_size_bytes = 0
    
    with open(output_path, "w", encoding="utf-8") as f:
        for doc_id, clean_text, event_id, current_model, authority in rows:
            if not clean_text:
                continue
            
            # Chunk text
            chunks = chunk_text(clean_text, max_tokens=1500, overlap_pct=0.10)
            
            for chunk_idx, chunk in enumerate(chunks):
                # Estimate tokens
                chunk_tokens = estimate_tokens(chunk, "text-embedding-3-small")
                total_tokens += chunk_tokens
                
                # Build request
                request = {
                    "custom_id": f"emb:{doc_id}:{chunk_idx}",
                    "method": "POST",
                    "url": "/v1/embeddings",
                    "body": {
                        "model": embed_model,
                        "input": chunk
                    }
                }
                
                line = json.dumps(request) + "\n"
                f.write(line)
                
                file_size_bytes += len(line.encode("utf-8"))
                request_count += 1
                
                # Check limits
                if request_count >= max_requests:
                    print(f"Reached max requests limit: {max_requests}")
                    break
                
                if file_size_bytes >= max_file_mb * 1024 * 1024:
                    print(f"Reached max file size limit: {max_file_mb} MB")
                    break
            
            if request_count >= max_requests or file_size_bytes >= max_file_mb * 1024 * 1024:
                break
    
    cur.close()
    conn.close()
    
    # Calculate cost (text-embedding-3-small: $0.00002 per 1K tokens, 50% batch discount)
    cost_per_1k_tokens = 0.00002
    batch_discount = 0.50
    projected_cost = (total_tokens / 1000.0) * cost_per_1k_tokens * batch_discount
    
    metadata = {
        "file_path": output_path,
        "request_count": request_count,
        "estimated_tokens": total_tokens,
        "projected_cost_usd": round(projected_cost, 4),
        "model": embed_model,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    print(f"Built {request_count} embedding requests")
    print(f"Estimated tokens: {total_tokens:,}")
    print(f"Projected cost: ${projected_cost:.4f} (with 50% batch discount)")
    
    return metadata


def build_summary_requests(
    since_date: Optional[str] = None,
    limit: Optional[int] = None,
    output_path: str = "data/batch/summaries.requests.jsonl",
    authorities: Optional[List[str]] = None
) -> Dict:
    """
    Build JSONL request file for summaries.

    Args:
        since_date: Filter events since this date (YYYY-MM-DD)
        limit: Maximum number of events to process
        output_path: Path to write JSONL file
        authorities: Filter by authority codes (e.g., ['MAS', 'IMDA'])

    Returns:
        Metadata dict with file_path, request_count, estimated_tokens, projected_cost_usd
    """
    summary_model = os.getenv("SUMMARY_MODEL", "gpt-4o-mini")
    max_requests = int(os.getenv("BATCH_MAX_REQUESTS", "20000"))
    max_file_mb = int(os.getenv("BATCH_MAX_FILE_MB", "100"))
    
    conn = get_db()
    cur = conn.cursor()
    
    # Query events needing summaries
    query = """
        SELECT e.event_id, e.title, d.clean_text, e.summary_model, e.authority
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        WHERE (e.summary_en IS NULL OR e.summary_model != %s OR e.summary_model IS NULL)
    """

    params = [summary_model]

    if since_date:
        query += " AND e.pub_date >= %s"
        params.append(since_date)

    if authorities:
        placeholders = ",".join(["%s"] * len(authorities))
        query += f" AND e.authority IN ({placeholders})"
        params.extend(authorities)
    
    query += " ORDER BY e.pub_date DESC"
    
    if limit:
        query += f" LIMIT {int(limit)}"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    print(f"Found {len(rows)} events needing summaries")
    
    # Build JSONL requests
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    request_count = 0
    total_input_tokens = 0
    total_output_tokens = 0
    file_size_bytes = 0
    
    with open(output_path, "w", encoding="utf-8") as f:
        for event_id, title, clean_text, current_model, authority in rows:
            # Use first 2000 chars of clean_text or title
            text = (clean_text or title or "")[:2000]
            
            if not text or len(text) < 50:
                continue
            
            # Estimate tokens
            input_tokens = estimate_tokens(text, summary_model)
            output_tokens = 180  # max_tokens setting
            
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            
            # Build request
            request = {
                "custom_id": f"sum:{event_id}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": summary_model,
                    "temperature": 0,
                    "max_tokens": 180,
                    "messages": [
                        {"role": "system", "content": "Summarize in 2 sentences. English only."},
                        {"role": "user", "content": text}
                    ]
                }
            }
            
            line = json.dumps(request) + "\n"
            f.write(line)
            
            file_size_bytes += len(line.encode("utf-8"))
            request_count += 1
            
            # Check limits
            if request_count >= max_requests:
                print(f"Reached max requests limit: {max_requests}")
                break
            
            if file_size_bytes >= max_file_mb * 1024 * 1024:
                print(f"Reached max file size limit: {max_file_mb} MB")
                break
    
    cur.close()
    conn.close()
    
    # Calculate cost (gpt-4o-mini: $0.150 input, $0.600 output per 1M tokens, 50% batch discount)
    cost_per_1m_input = 0.150
    cost_per_1m_output = 0.600
    batch_discount = 0.50
    
    input_cost = (total_input_tokens / 1_000_000.0) * cost_per_1m_input * batch_discount
    output_cost = (total_output_tokens / 1_000_000.0) * cost_per_1m_output * batch_discount
    projected_cost = input_cost + output_cost
    
    metadata = {
        "file_path": output_path,
        "request_count": request_count,
        "estimated_input_tokens": total_input_tokens,
        "estimated_output_tokens": total_output_tokens,
        "projected_cost_usd": round(projected_cost, 4),
        "model": summary_model,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    print(f"Built {request_count} summary requests")
    print(f"Estimated input tokens: {total_input_tokens:,}")
    print(f"Estimated output tokens: {total_output_tokens:,}")
    print(f"Projected cost: ${projected_cost:.4f} (with 50% batch discount)")
    
    return metadata

