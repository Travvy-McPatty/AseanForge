#!/usr/bin/env python3
"""
URL Discovery Script

Discovers press/news/regulatory URLs from authority websites without ingesting.
Uses Firecrawl crawl mode to discover links, then validates with HTTP HEAD.

Outputs CSV with columns: authority|category|url|http_status|discovered_at
"""
import argparse
import csv
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Set
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

try:
    import yaml
except ImportError:
    print("[error] PyYAML not installed; run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

try:
    from firecrawl import Firecrawl
except ImportError:
    print("[error] firecrawl-py not installed; run: pip install firecrawl-py", file=sys.stderr)
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


def authority_from_url(url: str) -> str:
    """Extract authority label from URL domain."""
    try:
        domain = urlparse(url).netloc.lower()
        
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


def validate_url_head(url: str, timeout: int = 10) -> int:
    """Validate URL with HTTP HEAD request. Returns status code."""
    try:
        req = Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0 (compatible; AseanForge/1.0)"})
        with urlopen(req, timeout=timeout) as resp:
            return resp.status
    except HTTPError as e:
        return e.code
    except (URLError, Exception):
        return 0


def discover_urls_from_seed(fc: Firecrawl, seed_url: str, limit: int, max_depth: int) -> List[str]:
    """
    Use Firecrawl crawl to discover URLs from a seed.
    Returns list of discovered URLs.
    """
    discovered = []
    
    try:
        # Firecrawl v2 crawl
        docs = fc.crawl(
            url=seed_url,
            limit=limit,
            pageOptions={
                "waitFor": 2000,
                "timeout": 60000,
                "includeHtml": False,
                "onlyMainContent": True
            },
            proxy="auto",
            poll_interval=1,
            timeout=120,
            maxAge=172800000
        )
        
        # Extract URLs from crawl results
        items = []
        if isinstance(docs, dict) and "data" in docs:
            items = docs.get("data") or []
        elif hasattr(docs, "data"):
            items = getattr(docs, "data") or []
        elif isinstance(docs, list):
            items = docs
        
        for item in items:
            if isinstance(item, dict):
                meta = item.get("metadata") or {}
                url = (
                    meta.get("sourceURL") or 
                    meta.get("ogUrl") or 
                    meta.get("url") or 
                    item.get("url")
                )
                if url and url.startswith("http"):
                    discovered.append(url)
            elif isinstance(item, str) and item.startswith("http"):
                discovered.append(item)
        
        # Polite delay
        time.sleep(1.2)
    
    except TypeError:
        # Fallback to legacy signature
        try:
            docs = fc.crawl(seed_url, limit=limit)
            if isinstance(docs, dict) and "data" in docs:
                items = docs.get("data") or []
            elif hasattr(docs, "data"):
                items = getattr(docs, "data") or []
            else:
                items = []
            
            for item in items:
                if isinstance(item, dict):
                    url = item.get("url")
                    if url and url.startswith("http"):
                        discovered.append(url)
        except Exception:
            pass
    
    except Exception as e:
        print(f"  [warn] Crawl failed for {seed_url}: {e}")
    
    return discovered


def main():
    parser = argparse.ArgumentParser(description="Discover URLs from authority websites")
    parser.add_argument("--config", default="config/sources.yaml", help="Path to sources.yaml")
    parser.add_argument("--categories", default="press,news,regulations,sitemaps", help="Comma-separated categories to discover")
    parser.add_argument("--limit-per-category", type=int, default=200, help="Max URLs to discover per category")
    parser.add_argument("--max-depth", type=int, default=1, help="Max crawl depth")
    parser.add_argument("--dry-run", action="store_true", help="Discover only; do not validate URLs")
    parser.add_argument("--output", required=True, help="Path to write discovered URLs CSV")
    args = parser.parse_args()
    
    # Load Firecrawl API key
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("[error] FIRECRAWL_API_KEY not set in environment", file=sys.stderr)
        sys.exit(1)
    
    # Load source filter
    source_filter = os.getenv("SOURCE_FILTER", "")
    if source_filter:
        filter_list = [s.strip().upper() for s in source_filter.split(",") if s.strip()]
        print(f"[info] Filtering sources: {', '.join(filter_list)}")
    else:
        filter_list = []
    
    # Initialize Firecrawl
    try:
        fc = Firecrawl(api_key=api_key)
    except Exception as e:
        print(f"[error] Failed to initialize Firecrawl: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Load sources
    if not os.path.exists(args.config):
        print(f"[error] Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    
    entries = load_sources_config(args.config)
    print(f"[info] Loaded {len(entries)} sources from {args.config}")
    
    # Filter by SOURCE_FILTER if set
    if filter_list:
        filtered = []
        for entry in entries:
            url = entry.get("url", "")
            name = entry.get("name", "")
            auth = authority_from_url(url)
            if auth in filter_list or any(f in name.upper() or f in url.upper() for f in filter_list):
                filtered.append(entry)
        entries = filtered
        print(f"[info] Filtered to {len(entries)} sources")
    
    # Discover URLs
    discovered_urls: Set[str] = set()
    results = []
    
    for entry in entries:
        seed_url = entry.get("url")
        name = entry.get("name", "")
        
        if not seed_url:
            continue
        
        authority = authority_from_url(seed_url)
        category = entry.get("category", "unknown")
        
        print(f"[info] Discovering from {authority}: {seed_url}")
        
        urls = discover_urls_from_seed(fc, seed_url, args.limit_per_category, args.max_depth)
        print(f"  Found {len(urls)} URLs")
        
        for url in urls:
            if url in discovered_urls:
                continue
            
            discovered_urls.add(url)
            
            # Validate with HEAD if not dry-run
            status = 0
            if not args.dry_run:
                status = validate_url_head(url)
            
            results.append({
                "authority": authority,
                "category": category,
                "url": url,
                "http_status": status,
                "discovered_at": datetime.utcnow().isoformat() + "Z"
            })
    
    # Write CSV
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["authority", "category", "url", "http_status", "discovered_at"])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n[info] Discovered {len(results)} URLs written to {args.output}")
    
    if not args.dry_run:
        ok_count = sum(1 for r in results if r["http_status"] == 200)
        print(f"[info] Validated: {ok_count}/{len(results)} returned HTTP 200")


if __name__ == "__main__":
    main()

