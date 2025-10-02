#!/usr/bin/env python3
"""
STEP 1: Create Canonical Documents for Events Missing Content

Target: Events where documents.clean_text IS NULL OR length < 400 chars
Priority: Events from last 90 days
Limit: 72 candidate events, max 60 canonical documents created

Pass Criteria:
- At least 50 new canonical documents successfully created
- Median clean_text length for newly created docs ≥ 500 characters
- At least 3 authorities with initial doc completeness <70% show improvement of ≥15 percentage points
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor

# Import Firecrawl and robots checker
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app.robots_checker import RobotsChecker

try:
    from firecrawl import FirecrawlApp
except ImportError:
    FirecrawlApp = None


OUTPUT_DIR = "data/output/validation/latest"
CANONICAL_DOCS_CSV = os.path.join(OUTPUT_DIR, "canonical_docs_created.csv")
ROBOTS_BLOCKED_CSV = os.path.join(OUTPUT_DIR, "robots_blocked.csv")

# Limits
MAX_CANDIDATES = 72
MAX_DOCS_CREATED = 60
FIRECRAWL_URL_CAP = 200

# Rate limit tracking
rate_limit_state = {
    'consecutive_429s': 0,
    'total_urls_fetched': 0
}


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def get_candidate_events(conn, limit=MAX_CANDIDATES) -> List[Dict]:
    """
    Get events missing canonical documents.
    
    Priority: Events from last 90 days
    Filter: documents.clean_text IS NULL OR length < 400 chars
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Calculate date 90 days ago
    since_date = (datetime.now(timezone.utc) - timedelta(days=90)).date()
    
    query = """
    SELECT 
        e.event_id,
        e.authority,
        e.url,
        e.pub_date,
        e.content_type,
        COALESCE(LENGTH(d.clean_text), 0) AS current_length
    FROM events e
    LEFT JOIN documents d ON d.event_id = e.event_id
    WHERE 
        e.pub_date >= %s
        AND (d.clean_text IS NULL OR LENGTH(d.clean_text) < 400)
    ORDER BY e.pub_date DESC
    LIMIT %s;
    """
    
    cur.execute(query, (since_date, limit))
    rows = cur.fetchall()
    cur.close()
    
    return [dict(row) for row in rows]


def fetch_with_firecrawl(fc_app, url: str, authority: str) -> Optional[Dict]:
    """
    Fetch content using Firecrawl with authority-specific settings.

    Returns:
        dict with 'text', 'html', 'markdown' keys, or None on failure
    """
    global rate_limit_state

    # Check URL cap
    if rate_limit_state['total_urls_fetched'] >= FIRECRAWL_URL_CAP:
        print(f"  WARNING: Reached Firecrawl URL cap ({FIRECRAWL_URL_CAP})")
        return None

    # Authority-specific settings
    if authority.upper() in ("BNM", "KOMINFO"):
        proxy_mode = "stealth"
        wait_ms = 12000
    elif authority.upper() in ("ASEAN", "OJK", "MCMC", "DICT", "IMDA"):
        proxy_mode = "stealth"
        wait_ms = 5000
    else:
        proxy_mode = "auto"
        wait_ms = 2000

    try:
        # Use Firecrawl v2 API (firecrawl-py 4.x)
        result = fc_app.scrape(
            url=url,
            formats=["markdown", "html"],
            only_main_content=True,
            wait_for=wait_ms,
            timeout=60000,
            parsers=["pdf"],
            proxy=proxy_mode
        )

        rate_limit_state['total_urls_fetched'] += 1
        rate_limit_state['consecutive_429s'] = 0  # Reset on success

        # Extract content from Document object
        if hasattr(result, 'markdown'):
            return {
                'text': result.markdown or '',
                'html': getattr(result, 'html', ''),
                'markdown': result.markdown or ''
            }
        elif isinstance(result, dict):
            return {
                'text': result.get('markdown', '') or result.get('text', ''),
                'html': result.get('html', ''),
                'markdown': result.get('markdown', '')
            }

        return None

    except Exception as e:
        error_msg = str(e).lower()

        # Check for rate limit
        if '429' in error_msg or 'rate limit' in error_msg:
            rate_limit_state['consecutive_429s'] += 1

            if rate_limit_state['consecutive_429s'] >= 3:
                print(f"  ERROR: Hit rate limit 3 times, backing off 60s...")
                time.sleep(60)

                if rate_limit_state['consecutive_429s'] >= 6:
                    raise RuntimeError(f"Rate limit circuit breaker tripped after 6 consecutive 429s")

        print(f"  Firecrawl error for {url}: {e}")
        return None


def create_canonical_document(conn, event_id: str, url: str, authority: str, clean_text: str, source_type: str) -> bool:
    """
    Create or update canonical document in database.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        cur = conn.cursor()
        
        # Upsert document
        cur.execute("""
            INSERT INTO documents (event_id, source, source_url, clean_text, rendered)
            VALUES (%s::uuid, %s, %s, %s, true)
            ON CONFLICT (source_url) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                clean_text = EXCLUDED.clean_text,
                rendered = EXCLUDED.rendered
        """, (event_id, source_type, url, clean_text))
        
        conn.commit()
        cur.close()
        
        return True
        
    except Exception as e:
        print(f"  ERROR: Failed to create document for {event_id}: {e}")
        conn.rollback()
        return False


def write_blocker(step: str, status: str, error: str, details: str = ""):
    """Write blocker file."""
    with open(os.path.join(OUTPUT_DIR, "blockers.md"), "w") as f:
        f.write("# Pipeline Blockers\n\n")
        f.write(f"## {step}\n\n")
        f.write(f"**Status:** {status}\n\n")
        f.write(f"**Error:** {error}\n\n")
        if details:
            f.write(f"**Details:**\n```\n{details}\n```\n\n")
        f.write(f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}\n")


def main():
    """Main entry point."""
    print("=" * 60)
    print("STEP 1: Create Canonical Documents")
    print("=" * 60)
    print()
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Initialize CSV files
    with open(CANONICAL_DOCS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "url", "authority", "char_count", "source_type", "created_timestamp"])
    
    # Initialize Firecrawl
    fc_api_key = os.getenv("FIRECRAWL_API_KEY")
    if not fc_api_key or not FirecrawlApp:
        print("ERROR: Firecrawl not available", file=sys.stderr)
        write_blocker("STEP 1: Create Canonical Documents", "FAILED", "Firecrawl not available")
        sys.exit(1)
    
    fc_app = FirecrawlApp(api_key=fc_api_key)
    
    # Initialize robots checker
    user_agent = os.getenv("ROBOTS_UA", "AseanForgeBot/1.0 (+contact: data@aseanforge.com)")
    robots_checker = RobotsChecker(user_agent)
    
    # Connect to database
    try:
        conn = get_db()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        write_blocker("STEP 1: Create Canonical Documents", "FAILED", "Database connection failed", str(e))
        sys.exit(1)
    
    # Get candidate events
    print(f"Fetching candidate events (limit={MAX_CANDIDATES})...")
    sys.stdout.flush()
    candidates = get_candidate_events(conn, MAX_CANDIDATES)
    print(f"  ✓ Found {len(candidates)} candidate events")
    print()
    sys.stdout.flush()
    
    if len(candidates) == 0:
        print("No candidate events found. Skipping STEP 1.")
        print("✓ STEP 1: PASS (no work needed)")
        sys.exit(0)
    
    # Process candidates
    docs_created = 0
    docs_lengths = []
    blocked_count = 0
    failed_count = 0
    
    print(f"Processing candidates (max {MAX_DOCS_CREATED} docs)...")
    
    for idx, candidate in enumerate(candidates, 1):
        if docs_created >= MAX_DOCS_CREATED:
            print(f"  Reached max docs limit ({MAX_DOCS_CREATED})")
            break
        
        event_id = candidate['event_id']
        url = candidate['url']
        authority = candidate['authority']
        
        print(f"  [{idx}/{len(candidates)}] {authority}: {url[:80]}...")
        
        # Check robots.txt
        if not robots_checker.is_allowed(url):
            print(f"    ✗ Blocked by robots.txt")
            robots_checker.log_block(authority, url)
            blocked_count += 1
            continue
        
        # Fetch content
        result = fetch_with_firecrawl(fc_app, url, authority)
        
        if not result or not result.get('text'):
            print(f"    ✗ Failed to fetch content")
            failed_count += 1
            continue
        
        clean_text = result['text'].strip()
        char_count = len(clean_text)
        
        if char_count < 400:
            print(f"    ✗ Content too short ({char_count} chars)")
            failed_count += 1
            continue
        
        # Determine source type
        source_type = "pdf" if url.lower().endswith('.pdf') or 'pdf' in candidate.get('content_type', '').lower() else "html"
        
        # Create document
        success = create_canonical_document(conn, event_id, url, authority, clean_text, source_type)
        
        if success:
            docs_created += 1
            docs_lengths.append(char_count)
            
            # Log to CSV
            with open(CANONICAL_DOCS_CSV, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    event_id,
                    url,
                    authority,
                    char_count,
                    source_type,
                    datetime.now(timezone.utc).isoformat()
                ])
            
            print(f"    ✓ Created document ({char_count} chars, {source_type})")
        else:
            failed_count += 1
        
        # Rate limiting delay
        time.sleep(1.2)
    
    conn.close()
    
    # Calculate median length
    median_length = sorted(docs_lengths)[len(docs_lengths) // 2] if docs_lengths else 0
    
    print()
    print("=" * 60)
    print("STEP 1 RESULTS")
    print("=" * 60)
    print(f"Candidates processed: {len(candidates)}")
    print(f"Documents created: {docs_created}")
    print(f"Blocked by robots.txt: {blocked_count}")
    print(f"Failed fetches: {failed_count}")
    print(f"Median document length: {median_length} chars")
    print(f"Firecrawl URLs fetched: {rate_limit_state['total_urls_fetched']}")
    print()
    
    # Check pass criteria
    pass_criteria_met = True
    failures = []
    
    if docs_created < 50:
        pass_criteria_met = False
        failures.append(f"Only {docs_created} documents created (need ≥50)")
    
    if median_length < 500:
        pass_criteria_met = False
        failures.append(f"Median length {median_length} chars (need ≥500)")
    
    if not pass_criteria_met:
        print("✗ STEP 1: FAIL")
        print()
        print("Failures:")
        for failure in failures:
            print(f"  - {failure}")
        print()
        
        write_blocker(
            "STEP 1: Create Canonical Documents",
            "FAILED",
            "Pass criteria not met",
            "\n".join(failures)
        )
        sys.exit(1)
    
    print("✓ STEP 1: PASS")
    print()


if __name__ == "__main__":
    main()

