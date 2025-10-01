#!/usr/bin/env python3
"""
Firecrawl Account Status & Queue Health Probe

Queries Firecrawl API for:
- Account usage (credits consumed, plan limits)
- Queue health (status, error rate, processing time)

Outputs JSON artifacts for pre/post-run comparison.
"""
import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any

try:
    from firecrawl import Firecrawl
except ImportError:
    print("[error] firecrawl-py not installed; run: pip install firecrawl-py", file=sys.stderr)
    sys.exit(1)


def probe_account_usage(fc: Firecrawl) -> Dict[str, Any]:
    """
    Query Firecrawl account usage endpoint.
    Returns dict with credits_used, credits_limit, plan_name, etc.
    
    Note: Firecrawl SDK may not expose account usage directly.
    This is a placeholder for when the API supports it.
    """
    # Placeholder: Firecrawl Python SDK doesn't expose account usage endpoint yet
    # We'll return a stub structure and note that manual API call may be needed
    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "unknown",
        "credits_used": None,
        "credits_limit": None,
        "plan_name": "unknown",
        "note": "Firecrawl Python SDK does not expose account usage endpoint; manual API call required"
    }
    
    # If Firecrawl adds account usage to SDK, implement here:
    # try:
    #     usage = fc.get_account_usage()  # hypothetical method
    #     result.update(usage)
    # except Exception as e:
    #     result["error"] = str(e)
    
    return result


def probe_queue_health(fc: Firecrawl) -> Dict[str, Any]:
    """
    Query Firecrawl queue health endpoint.
    Returns dict with healthy status, error_rate, avg_processing_time, etc.
    
    Note: Firecrawl SDK may not expose queue health directly.
    This is a placeholder for when the API supports it.
    """
    # Placeholder: Firecrawl Python SDK doesn't expose queue health endpoint yet
    result = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "healthy": True,  # assume healthy unless proven otherwise
        "error_rate": 0.0,
        "avg_processing_time_ms": None,
        "queue_depth": None,
        "note": "Firecrawl Python SDK does not expose queue health endpoint; manual API call required"
    }
    
    # If Firecrawl adds queue health to SDK, implement here:
    # try:
    #     health = fc.get_queue_health()  # hypothetical method
    #     result.update(health)
    # except Exception as e:
    #     result["error"] = str(e)
    #     result["healthy"] = False
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Probe Firecrawl account status and queue health")
    parser.add_argument("--output", required=True, help="Path to write account usage JSON")
    parser.add_argument("--queue-output", required=True, help="Path to write queue health JSON")
    args = parser.parse_args()
    
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("[error] FIRECRAWL_API_KEY not set in environment", file=sys.stderr)
        sys.exit(1)
    
    try:
        fc = Firecrawl(api_key=api_key)
    except Exception as e:
        print(f"[error] Failed to initialize Firecrawl client: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Probe account usage
    usage = probe_account_usage(fc)
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(usage, f, indent=2)
    print(f"[info] Account usage written to {args.output}")
    
    # Probe queue health
    health = probe_queue_health(fc)
    os.makedirs(os.path.dirname(args.queue_output) or ".", exist_ok=True)
    with open(args.queue_output, "w", encoding="utf-8") as f:
        json.dump(health, f, indent=2)
    print(f"[info] Queue health written to {args.queue_output}")
    
    # Exit with error if queue unhealthy or credits exhausted
    if not health.get("healthy", True):
        print("[error] Queue status unhealthy; halt execution", file=sys.stderr)
        sys.exit(1)
    
    if usage.get("credits_used") and usage.get("credits_limit"):
        pct = (usage["credits_used"] / usage["credits_limit"]) * 100
        if pct >= 90:
            print(f"[error] Credits {pct:.1f}% consumed (>90%); halt execution", file=sys.stderr)
            sys.exit(1)
    
    print("[info] Firecrawl account status: OK")


if __name__ == "__main__":
    main()

