#!/usr/bin/env python3
"""
Canonical Seed URL Validator

Validates all seed URLs from config/sources.yaml by:
1. Performing HTTP GET with redirect follow (max 5 redirects)
2. Recording: authority, original_url, http_status, final_url, redirect_chain, timestamp
3. Writing results to CSV

Only seeds returning HTTP 200 (or 3xx → 200) should be used for ingestion.
"""
import argparse
import csv
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

try:
    import yaml
except ImportError:
    print("[error] PyYAML not installed; run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def load_sources_config(path: str) -> List[Dict[str, Any]]:
    """Load sources from YAML config."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    entries: List[Dict[str, Any]] = []
    for section, items in (data or {}).items():
        for it in items or []:
            it = dict(it)
            it["section"] = section
            entries.append(it)
    
    return entries


def validate_url(url: str, timeout: int = 20, max_redirects: int = 5) -> Tuple[int, str, List[str]]:
    """
    Validate a URL by performing HTTP GET with redirect follow.
    
    Returns:
        (http_status, final_url, redirect_chain)
    """
    redirect_chain = []
    current_url = url
    
    for i in range(max_redirects + 1):
        try:
            req = Request(current_url, headers={"User-Agent": "Mozilla/5.0 (compatible; AseanForge/1.0)"})
            with urlopen(req, timeout=timeout) as resp:
                status = resp.status
                final_url = resp.geturl()
                
                if final_url != current_url:
                    redirect_chain.append(f"{current_url} -> {final_url}")
                    current_url = final_url
                else:
                    return (status, final_url, redirect_chain)
                
                # If we got a 200, we're done
                if status == 200:
                    return (status, final_url, redirect_chain)
        
        except HTTPError as e:
            return (e.code, current_url, redirect_chain)
        
        except URLError as e:
            return (0, current_url, redirect_chain)  # 0 indicates network error
        
        except Exception as e:
            return (-1, current_url, redirect_chain)  # -1 indicates unknown error
    
    # Max redirects exceeded
    return (310, current_url, redirect_chain)  # 310 = too many redirects


def authority_from_url(url: str) -> str:
    """Extract authority label from URL domain."""
    try:
        domain = urlparse(url).netloc.lower()
        
        # Map domains to authority labels
        if "mas.gov.sg" in domain:
            return "MAS"
        elif "imda.gov.sg" in domain:
            return "IMDA"
        elif "pdpc.gov.sg" in domain:
            return "PDPC"
        elif "sc.com.my" in domain:
            return "SC"
        elif "bnm.gov.my" in domain:
            return "BNM"
        elif "mcmc.gov.my" in domain:
            return "MCMC"
        elif "ojk.go.id" in domain:
            return "OJK"
        elif "bi.go.id" in domain:
            return "BI"
        elif "kominfo.go.id" in domain:
            return "KOMINFO"
        elif "bot.or.th" in domain:
            return "BOT"
        elif "bsp.gov.ph" in domain:
            return "BSP"
        elif "dict.gov.ph" in domain:
            return "DICT"
        elif "sbv.gov.vn" in domain:
            return "SBV"
        elif "mic.gov.vn" in domain:
            return "MIC"
        elif "asean.org" in domain:
            return "ASEAN"
        else:
            return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def main():
    parser = argparse.ArgumentParser(description="Validate canonical seed URLs from config")
    parser.add_argument("--config", default="config/sources.yaml", help="Path to sources.yaml")
    parser.add_argument("--output", required=True, help="Path to write validation results CSV")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"[error] Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    
    entries = load_sources_config(args.config)
    print(f"[info] Loaded {len(entries)} sources from {args.config}")
    
    # Prepare output
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    
    results = []
    for entry in entries:
        url = entry.get("url")
        name = entry.get("name", "")
        
        if not url:
            continue
        
        authority = authority_from_url(url)
        print(f"[info] Validating {authority}: {url}")
        
        status, final_url, redirect_chain = validate_url(url, timeout=args.timeout)
        
        results.append({
            "authority": authority,
            "name": name,
            "original_url": url,
            "http_status": status,
            "final_url": final_url,
            "redirect_chain": " | ".join(redirect_chain) if redirect_chain else "",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
        
        # Status indicator
        if status == 200:
            print(f"  ✓ HTTP {status}")
        elif 300 <= status < 400:
            print(f"  → HTTP {status} (redirect)")
        else:
            print(f"  ✗ HTTP {status}")
    
    # Write CSV
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "authority", "name", "original_url", "http_status", "final_url", "redirect_chain", "timestamp"
        ])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"[info] Validation results written to {args.output}")
    
    # Summary
    total = len(results)
    ok_count = sum(1 for r in results if r["http_status"] == 200)
    redirect_count = sum(1 for r in results if 300 <= r["http_status"] < 400)
    error_count = sum(1 for r in results if r["http_status"] >= 400 or r["http_status"] <= 0)
    
    print(f"\n[summary] Total: {total} | OK (200): {ok_count} | Redirects (3xx): {redirect_count} | Errors: {error_count}")
    
    if ok_count < total * 0.5:
        print(f"[warn] Less than 50% of seeds returned HTTP 200; review results before proceeding", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

