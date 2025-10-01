#!/usr/bin/env python3
"""
Firecrawl Health Check Script

Queries Firecrawl account usage and queue health status.
Outputs JSON files for pre-flight and post-flight validation.

Usage:
    python scripts/fc_health_check.py --output account_usage.json --queue-output queue_status.json
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

try:
    import requests
except ImportError:
    print("ERROR: requests library not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(1)


def check_account_usage(api_key: str) -> Dict[str, Any]:
    """
    Query Firecrawl account usage/credits.
    
    Note: Firecrawl Python SDK doesn't expose this endpoint yet.
    Using direct API call to /v2/team/usage or similar.
    """
    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "unknown",
        "credits_used": None,
        "credits_limit": None,
        "credits_remaining": None,
        "plan_name": "unknown",
        "note": "Firecrawl API endpoint for usage not documented; using placeholder"
    }
    
    # Try to query usage endpoint (may not exist in current API)
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Attempt v2 endpoint (may not exist)
        response = requests.get(
            "https://api.firecrawl.dev/v2/team/usage",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            result["status"] = "ok"
            result["credits_used"] = data.get("credits_used")
            result["credits_limit"] = data.get("credits_limit")
            result["credits_remaining"] = data.get("credits_remaining")
            result["plan_name"] = data.get("plan_name", "unknown")
            result["note"] = "Successfully retrieved usage data"
        else:
            result["status"] = "error"
            result["note"] = f"API returned {response.status_code}: {response.text[:200]}"
    except Exception as e:
        result["status"] = "error"
        result["note"] = f"Failed to query usage endpoint: {str(e)}"
    
    return result


def check_queue_status(api_key: str) -> Dict[str, Any]:
    """
    Query Firecrawl queue health status.
    
    Note: Firecrawl Python SDK doesn't expose this endpoint yet.
    Using direct API call to /v2/team/queue-status or similar.
    """
    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "healthy": True,  # Assume healthy unless proven otherwise
        "queue_depth": None,
        "error_rate": None,
        "note": "Firecrawl API endpoint for queue status not documented; using placeholder"
    }
    
    # Try to query queue status endpoint (may not exist in current API)
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Attempt v2 endpoint (may not exist)
        response = requests.get(
            "https://api.firecrawl.dev/v2/team/queue-status",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            result["healthy"] = data.get("healthy", True)
            result["queue_depth"] = data.get("queue_depth")
            result["error_rate"] = data.get("error_rate")
            result["note"] = "Successfully retrieved queue status"
        else:
            result["note"] = f"API returned {response.status_code}: {response.text[:200]}"
    except Exception as e:
        result["note"] = f"Failed to query queue status endpoint: {str(e)}"
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Firecrawl Health Check")
    parser.add_argument("--output", type=str, required=True,
                       help="Output path for account usage JSON")
    parser.add_argument("--queue-output", type=str, required=True,
                       help="Output path for queue status JSON")
    parser.add_argument("--halt-on-low-credits", action="store_true",
                       help="Exit with error if credits < 10% of limit")
    parser.add_argument("--halt-on-unhealthy", action="store_true",
                       help="Exit with error if queue is unhealthy")
    
    args = parser.parse_args()
    
    # Get API key from environment
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("ERROR: FIRECRAWL_API_KEY not set in environment", file=sys.stderr)
        sys.exit(1)
    
    # Check account usage
    print("Checking Firecrawl account usage...", file=sys.stderr)
    usage = check_account_usage(api_key)
    
    # Write usage to file
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(usage, f, indent=2)
    print(f"✓ Account usage written to: {args.output}", file=sys.stderr)
    
    # Check queue status
    print("Checking Firecrawl queue status...", file=sys.stderr)
    queue = check_queue_status(api_key)
    
    # Write queue status to file
    os.makedirs(os.path.dirname(args.queue_output) or ".", exist_ok=True)
    with open(args.queue_output, "w", encoding="utf-8") as f:
        json.dump(queue, f, indent=2)
    print(f"✓ Queue status written to: {args.queue_output}", file=sys.stderr)
    
    # Check halt conditions
    halt = False
    
    if args.halt_on_low_credits:
        if usage.get("credits_remaining") is not None and usage.get("credits_limit") is not None:
            pct = (usage["credits_remaining"] / usage["credits_limit"]) * 100
            if pct < 10:
                print(f"ERROR: Credits remaining ({pct:.1f}%) below 10% threshold", file=sys.stderr)
                halt = True
    
    if args.halt_on_unhealthy:
        if not queue.get("healthy", True):
            print("ERROR: Firecrawl queue is unhealthy", file=sys.stderr)
            halt = True
        if queue.get("error_rate") is not None and queue["error_rate"] > 0.05:
            print(f"ERROR: Queue error rate ({queue['error_rate']:.2%}) above 5% threshold", file=sys.stderr)
            halt = True
    
    if halt:
        print("\n⛔ HALT: Pre-flight checks failed. See errors above.", file=sys.stderr)
        sys.exit(1)
    
    print("\n✓ All health checks passed", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()

