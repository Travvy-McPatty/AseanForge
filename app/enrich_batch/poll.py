#!/usr/bin/env python3
"""
Batch Polling

Poll batch status until completion or failure.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

from openai import OpenAI


def poll_batch(
    batch_id: str,
    poll_interval_seconds: int = 60,
    timeout_hours: int = 26
) -> Dict:
    """
    Poll batch status until completion or failure.
    
    Args:
        batch_id: Batch ID to poll
        poll_interval_seconds: Seconds between polls (default: 60)
        timeout_hours: Maximum hours to wait (default: 26 for 24h window + buffer)
    
    Returns:
        Result dict with status, output_file_path, error_file_path, request_counts
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    
    client = OpenAI(api_key=api_key)
    
    start_time = time.time()
    timeout_seconds = timeout_hours * 3600
    
    print(f"Polling batch {batch_id} (interval={poll_interval_seconds}s, timeout={timeout_hours}h)...")
    
    iteration = 0
    
    while True:
        iteration += 1
        elapsed = time.time() - start_time
        
        if elapsed > timeout_seconds:
            error_msg = f"Timeout after {timeout_hours} hours"
            print(f"  ERROR: {error_msg}")
            
            # Write failure report
            os.makedirs("data/output/validation/latest", exist_ok=True)
            with open(f"data/output/validation/latest/batch_{batch_id}_timeout.txt", "w") as f:
                f.write(f"Batch ID: {batch_id}\n")
                f.write(f"Status: TIMEOUT\n")
                f.write(f"Elapsed: {elapsed / 3600:.2f} hours\n")
                f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
            
            return {
                "status": "timeout",
                "batch_id": batch_id,
                "elapsed_seconds": elapsed,
                "error": error_msg
            }
        
        # Retrieve batch status
        try:
            batch = client.batches.retrieve(batch_id)
        except Exception as e:
            print(f"  ERROR retrieving batch: {e}")
            time.sleep(poll_interval_seconds)
            continue
        
        status = batch.status
        request_counts = {
            "total": getattr(batch.request_counts, "total", 0),
            "completed": getattr(batch.request_counts, "completed", 0),
            "failed": getattr(batch.request_counts, "failed", 0)
        }
        
        print(f"  [{iteration}] Status: {status} | Completed: {request_counts['completed']}/{request_counts['total']} | Elapsed: {elapsed / 60:.1f}m")
        
        if status == "completed":
            print(f"  ✓ Batch completed successfully")
            
            # Download output file
            output_file_id = batch.output_file_id
            error_file_id = getattr(batch, "error_file_id", None)
            
            output_path = None
            error_path = None
            
            if output_file_id:
                output_path = f"data/batch/{batch_id}.results.jsonl"
                print(f"  Downloading output file {output_file_id} to {output_path}...")
                
                content = client.files.content(output_file_id)
                with open(output_path, "wb") as f:
                    f.write(content.read())
                
                print(f"    ✓ Downloaded {os.path.getsize(output_path):,} bytes")
            
            if error_file_id:
                error_path = f"data/batch/{batch_id}.errors.jsonl"
                print(f"  Downloading error file {error_file_id} to {error_path}...")
                
                content = client.files.content(error_file_id)
                with open(error_path, "wb") as f:
                    f.write(content.read())
                
                print(f"    ✓ Downloaded {os.path.getsize(error_path):,} bytes")
            
            return {
                "status": "completed",
                "batch_id": batch_id,
                "output_file_path": output_path,
                "error_file_path": error_path,
                "request_counts": request_counts,
                "elapsed_seconds": elapsed
            }
        
        elif status in ("failed", "expired", "cancelled"):
            error_msg = f"Batch {status}"
            print(f"  ✗ {error_msg}")
            
            # Write failure report
            os.makedirs("data/output/validation/latest", exist_ok=True)
            with open(f"data/output/validation/latest/batch_{batch_id}_failure.txt", "w") as f:
                f.write(f"Batch ID: {batch_id}\n")
                f.write(f"Status: {status.upper()}\n")
                f.write(f"Request counts: {json.dumps(request_counts, indent=2)}\n")
                f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
                
                # Try to get error details
                if hasattr(batch, "errors"):
                    f.write(f"\nErrors:\n{json.dumps(batch.errors, indent=2)}\n")
            
            return {
                "status": status,
                "batch_id": batch_id,
                "request_counts": request_counts,
                "elapsed_seconds": elapsed,
                "error": error_msg
            }
        
        elif status in ("validating", "in_progress", "finalizing"):
            # Continue polling
            time.sleep(poll_interval_seconds)
        
        else:
            print(f"  WARNING: Unknown status '{status}', continuing to poll...")
            time.sleep(poll_interval_seconds)


def cancel_batch(batch_id: str) -> bool:
    """
    Cancel a running batch job.
    
    Args:
        batch_id: Batch ID to cancel
    
    Returns:
        True if cancelled successfully
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    
    client = OpenAI(api_key=api_key)
    
    try:
        batch = client.batches.cancel(batch_id)
        print(f"Batch {batch_id} cancelled. Status: {batch.status}")
        return True
    except Exception as e:
        print(f"ERROR cancelling batch: {e}")
        return False

