#!/usr/bin/env python3
"""
Batch Submission

Upload JSONL to OpenAI Files API and create Batch jobs.
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

from openai import OpenAI


def submit_batch(input_file_path: str, kind: str) -> str:
    """
    Submit batch job to OpenAI.
    
    Args:
        input_file_path: Path to JSONL request file
        kind: 'embeddings' or 'summaries'
    
    Returns:
        batch_id
    """
    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"Input file not found: {input_file_path}")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    
    client = OpenAI(api_key=api_key)
    
    # Determine endpoint
    if kind == "embeddings":
        endpoint = "/v1/embeddings"
    elif kind == "summaries":
        endpoint = "/v1/chat/completions"
    else:
        raise ValueError(f"Invalid kind: {kind}. Must be 'embeddings' or 'summaries'")
    
    completion_window = os.getenv("BATCH_COMPLETION_WINDOW", "24h")
    
    print(f"Uploading {input_file_path} to OpenAI Files API...")
    
    # Upload file
    with open(input_file_path, "rb") as f:
        file_obj = client.files.create(
            file=f,
            purpose="batch"
        )
    
    file_id = file_obj.id
    print(f"  File uploaded: {file_id}")
    
    # Create batch
    print(f"Creating batch job (endpoint={endpoint}, completion_window={completion_window})...")
    
    timestamp = datetime.now(timezone.utc).isoformat()
    
    batch = client.batches.create(
        input_file_id=file_id,
        endpoint=endpoint,
        completion_window=completion_window,
        metadata={
            "kind": kind,
            "timestamp": timestamp,
            "source": "aseanforge_enrich"
        }
    )
    
    batch_id = batch.id
    print(f"  Batch created: {batch_id}")
    print(f"  Status: {batch.status}")
    
    # Save batch metadata
    os.makedirs("data/batch", exist_ok=True)
    
    metadata = {
        "batch_id": batch_id,
        "input_file_id": file_id,
        "kind": kind,
        "endpoint": endpoint,
        "completion_window": completion_window,
        "created_at": timestamp,
        "status": batch.status,
        "input_file_path": input_file_path
    }
    
    metadata_path = f"data/batch/{kind}_{batch_id}.batch.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"  Metadata saved: {metadata_path}")
    
    return batch_id


def get_batch_status(batch_id: str) -> Dict:
    """
    Get current status of a batch job.
    
    Args:
        batch_id: Batch ID
    
    Returns:
        Status dict
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    
    client = OpenAI(api_key=api_key)
    
    batch = client.batches.retrieve(batch_id)
    
    return {
        "batch_id": batch.id,
        "status": batch.status,
        "created_at": batch.created_at,
        "completed_at": getattr(batch, "completed_at", None),
        "failed_at": getattr(batch, "failed_at", None),
        "expired_at": getattr(batch, "expired_at", None),
        "request_counts": {
            "total": getattr(batch.request_counts, "total", 0),
            "completed": getattr(batch.request_counts, "completed", 0),
            "failed": getattr(batch.request_counts, "failed", 0)
        },
        "output_file_id": getattr(batch, "output_file_id", None),
        "error_file_id": getattr(batch, "error_file_id", None)
    }

