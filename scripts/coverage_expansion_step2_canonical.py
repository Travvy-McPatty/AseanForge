#!/usr/bin/env python3
"""
Coverage Expansion Step 2: Canonical Doc Creation

Purpose: Create canonical documents for discovered URLs using Firecrawl
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor
from firecrawl import FirecrawlApp
from psycopg2 import errors


# Import robots checker
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app.robots_checker import RobotsChecker

OUTPUT_DIR = "data/output/validation/latest"
TARGETS_CSV = os.path.join(OUTPUT_DIR, "targets_zero_doc.csv")
CANONICAL_DOCS_CSV = os.path.join(OUTPUT_DIR, "mvp_canonical_docs.csv")
ROBOTS_BLOCKED_CSV = os.path.join(OUTPUT_DIR, "robots_blocked.csv")
FETCH_FAILURES_CSV = os.path.join(OUTPUT_DIR, "fetch_failures.csv")

# Firecrawl limits (MVP)
MAX_FIRECRAWL_URLS = 400
MAX_DOCUMENTS = 250

# Authority-specific Firecrawl settings
AUTHORITY_SETTINGS = {
    'ASEAN': {'proxy': 'stealth', 'wait_for': 5000},
    'OJK': {'proxy': 'stealth', 'wait_for': 5000},
    'MCMC': {'proxy': 'stealth', 'wait_for': 5000},
    'DICT': {'proxy': 'stealth', 'wait_for': 5000},
    'IMDA': {'proxy': 'stealth', 'wait_for': 5000},
    'BNM': {'proxy': 'stealth', 'wait_for': 12000},
    'KOMINFO': {'proxy': 'stealth', 'wait_for': 12000},
    # Default settings for others
    'default': {'proxy': 'auto', 'wait_for': 2000}
}


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def load_discovered_urls() -> List[Dict]:
    """Load discovered URLs from CSV."""
    if not os.path.exists(DISCOVERED_URLS_CSV):
        raise RuntimeError(f"Discovered URLs file not found: {DISCOVERED_URLS_CSV}")

    urls = []
    with open(DISCOVERED_URLS_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            urls.append(row)

    return urls


def get_urls_needing_docs() -> List[Dict]:
    """Get target events for canonical document creation.
    Prefer reading from targets_zero_doc.csv; otherwise build from DB (zero/short-doc <400, last 365 days).
    """
    # If targets CSV exists, load it
    if os.path.exists(TARGETS_CSV):
        targets = []
        with open(TARGETS_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                targets.append({
                    'event_id': row['event_id'],
                    'url': row['url'],
                    'authority': row['authority'],
                    'pub_date': row.get('pub_date'),
                    'max_doc_length': int(row.get('current_max_doc_length', '0') or 0),
                    'reason': row.get('reason', 'no_doc')
                })
        return targets

    # Otherwise, build targets from DB and persist CSV
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    lagging_authorities = ['SC', 'PDPC', 'MIC', 'BI', 'OJK', 'DICT', 'SBV', 'IMDA']
    placeholders = ','.join(['%s'] * len(lagging_authorities))

    cur.execute("""
        WITH per_event AS (
          SELECT e.event_id, e.authority, e.url, e.pub_date,
                 COALESCE(MAX(LENGTH(d.clean_text)), 0) AS max_len
          FROM events e
          LEFT JOIN documents d ON d.event_id = e.event_id
          WHERE e.authority IN ({placeholders})
            AND e.pub_date >= NOW() - INTERVAL '365 days'
          GROUP BY e.event_id, e.authority, e.url, e.pub_date
        )
        SELECT event_id, authority, url, pub_date, max_len
        FROM per_event
        WHERE max_len < 400
        ORDER BY array_position(ARRAY{laglist}::text[], authority), pub_date DESC
        LIMIT 250
    """.replace('{laglist}', str(lagging_authorities)).replace('{placeholders}', placeholders), lagging_authorities)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    candidates = []
    for r in rows:
        candidates.append({
            'event_id': r['event_id'],
            'url': r['url'],
            'authority': r['authority'],
            'pub_date': r['pub_date'].isoformat() if r['pub_date'] else '',
            'max_doc_length': r['max_len'],
            'reason': 'no_doc' if r['max_len'] == 0 else 'short_doc'
        })

    # Write targets CSV for reproducibility
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(TARGETS_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['event_id', 'authority', 'url', 'pub_date', 'current_max_doc_length', 'reason'])
        writer.writeheader()
        for c in candidates:
            writer.writerow({
                'event_id': c['event_id'],
                'authority': c['authority'],
                'url': c['url'],
                'pub_date': c['pub_date'],
                'current_max_doc_length': c['max_doc_length'],
                'reason': c['reason']
            })

    return candidates


def get_firecrawl_settings(authority: str) -> Dict:
    """Get Firecrawl settings for authority."""
    return AUTHORITY_SETTINGS.get(authority, AUTHORITY_SETTINGS['default'])


def fetch_with_firecrawl(url: str, authority: str, fc_app: FirecrawlApp) -> Optional[Dict]:
    """Fetch document content using Firecrawl."""
    settings = get_firecrawl_settings(authority)

    try:
        result = fc_app.scrape(
            url=url,
            formats=["markdown", "html"],
            only_main_content=True,
            wait_for=settings['wait_for'],
            timeout=60000,
            parsers=["pdf"],
            proxy=settings['proxy']
        )

        # Extract content from Document object
        if hasattr(result, 'markdown'):
            text = result.markdown or ''
            html = getattr(result, 'html', '')

            if len(text) >= 100:  # Minimum viable content
                is_pdf = url.lower().endswith('.pdf')
                return {
                    'text': text,
                    'html': html,
                    'markdown': text,
                    'source_type': 'pdf' if is_pdf else 'html'
                }

        return None

    except Exception as e:
        print(f"    ✗ Firecrawl error: {e}")
        return None




def has_qualifying_doc(event_id: str) -> bool:
    """Check if event already has a qualifying document (>=400 chars)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(MAX(LENGTH(clean_text)), 0)
        FROM documents
        WHERE event_id = %s
    """, (event_id,))
    max_len = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_len or 0) >= 400


def source_url_exists(url: str) -> bool:
    """Check if a document with this source_url already exists."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM documents WHERE source_url = %s LIMIT 1", (url,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()


def get_existing_qualifying_doc(url: str) -> Optional[Dict]:
    """Return an existing qualifying document (clean_text >= 400) for this source_url, if any."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT document_id, source, source_url, title, raw_text, clean_text,
               COALESCE(page_spans, '[]') AS page_spans, rendered
        FROM documents
        WHERE source_url = %s AND LENGTH(clean_text) >= 400
        LIMIT 1
        """,
        (url,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def append_canonical_csv(row: Dict):
    """Append a single created-doc row to mvp_canonical_docs.csv (streaming)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_exists = os.path.exists(CANONICAL_DOCS_CSV)
    with open(CANONICAL_DOCS_CSV, "a", newline="") as f:
        fieldnames = [
            "event_id",
            "authority",
            "url",
            "source_type",
            "document_id",
            "clean_text_length",
            "timestamp",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if (not file_exists) or os.path.getsize(CANONICAL_DOCS_CSV) == 0:
            writer.writeheader()
        writer.writerow(row)
        f.flush()


def clone_document_to_event(event_id: str, authority: str, existing: Dict) -> Optional[Dict]:
    """Clone an existing qualifying document to the given event (event-level link-backfill).
    Returns dict with document_id and used url if successful.
    """
    conn = get_db()
    cur = conn.cursor()
    base_url = existing["source_url"]
    candidate_urls = [base_url, f"{base_url}#event={event_id}"]
    for used_url in candidate_urls:
        try:
            cur.execute(
                """
                INSERT INTO documents (
                    document_id, event_id, source, source_url,
                    title, raw_text, clean_text, page_spans, rendered
                ) VALUES (
                    gen_random_uuid(), %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                RETURNING document_id
                """,
                (
                    event_id,
                    existing.get("source", "html"),
                    used_url,
                    existing.get("title", ""),
                    existing.get("raw_text", ""),
                    existing.get("clean_text", ""),
                    existing.get("page_spans", "[]"),
                    existing.get("rendered", True),
                ),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            return {"document_id": str(new_id), "url": used_url}
        except Exception as e:
            # Likely unique constraint on source_url; try next candidate variant
            conn.rollback()
            last_err = str(e)
            if "duplicate key value" in last_err or "unique constraint" in last_err:
                continue
            # Unexpected error
            cur.close()
            conn.close()
            raise
    # All attempts failed
    cur.close()
    conn.close()
    return None



def log_fetch_failure(authority: str, url: str, message: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_exists = os.path.exists(FETCH_FAILURES_CSV)
    with open(FETCH_FAILURES_CSV, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['authority', 'url', 'error', 'timestamp'])
        writer.writerow([authority, url, message, datetime.now(timezone.utc).isoformat()])

def create_document(event_id: str, url: str, content: Dict) -> Optional[Dict]:
    """Create a new document record for this event. Returns {document_id, url} on success."""
    conn = get_db()
    cur = conn.cursor()
    title = ''
    html = content.get('html', '')
    text = content.get('text', '')
    page_spans = '[]'
    source_type = content.get('source_type', 'html')

    candidate_urls = [url, f"{url}#event={event_id}"]
    for used_url in candidate_urls:
        try:
            cur.execute(
                """
                INSERT INTO documents (
                    document_id, event_id, source, source_url,
                    title, raw_text, clean_text, page_spans, rendered
                ) VALUES (
                    gen_random_uuid(), %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                RETURNING document_id
                """,
                (
                    event_id,
                    source_type,
                    used_url,
                    title,
                    html,
                    text,
                    page_spans,
                    True,
                ),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            return {"document_id": str(new_id), "url": used_url}
        except Exception as e:
            # Likely unique constraint on source_url; try next variant
            conn.rollback()
            err = str(e)
            if "duplicate key value" in err or "unique constraint" in err:
                continue
            print(f"    ✗ Database error: {e}")
            cur.close()
            conn.close()
            return None

    # If we exhausted candidates
    cur.close()
    conn.close()
    return None


def log_robots_block(authority: str, url: str, reason: str):
    """Log robots.txt block to CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_exists = os.path.exists(ROBOTS_BLOCKED_CSV)

    with open(ROBOTS_BLOCKED_CSV, 'a', newline='') as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(['authority', 'url', 'reason', 'timestamp'])

        writer.writerow([
            authority,
            url,
            reason,
            datetime.now(timezone.utc).isoformat()
        ])


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 2: Canonical Doc Creation")
    print("=" * 60)
    print()

    # Load targets (prefer CSV from Step B; otherwise build from DB)
    print("Loading target events (zero/short-doc focus)...")
    candidates = get_urls_needing_docs()
    print(f"  ✓ Found {len(candidates)} candidate events")
    print()

    if len(candidates) == 0:
        print("No URLs need canonical documents. Skipping Step 2.")
        print("✓ STEP 2: PASS (no work needed)")
        sys.exit(0)

    # Limit to MAX_DOCUMENTS and prioritize by authority
    if len(candidates) > MAX_DOCUMENTS:
        print(f"Limiting to {MAX_DOCUMENTS} candidates (from {len(candidates)})")
        # Prioritize by authority diversity and document length
        candidates_by_auth = {}
        for candidate in candidates:
            auth = candidate['authority']
            if auth not in candidates_by_auth:
                candidates_by_auth[auth] = []
            candidates_by_auth[auth].append(candidate)

        # Take up to 30 per authority to ensure diversity
        limited_candidates = []
        per_auth_limit = min(30, MAX_DOCUMENTS // len(candidates_by_auth))

        for auth, auth_candidates in candidates_by_auth.items():
            limited_candidates.extend(auth_candidates[:per_auth_limit])

        candidates = limited_candidates[:MAX_DOCUMENTS]

    # Initialize Firecrawl and robots checker
    fc_app = FirecrawlApp(api_key=os.getenv('FIRECRAWL_API_KEY'))
    robots_checker = RobotsChecker(os.getenv('ROBOTS_UA', 'AseanForgeBot/1.0'))

    # Process candidates
    print(f"Processing {len(candidates)} candidates...")
    print()

    created_docs = []
    firecrawl_urls_used = 0
    robots_blocks = 0
    failed_fetches = 0
    link_backfills = 0
    scrapes = 0

    for i, candidate in enumerate(candidates, 1):
        url = candidate['url']
        authority = candidate['authority']
        event_id = candidate['event_id']

        print(f"  [{i}/{len(candidates)}] {authority}: {url[:60]}...")

        # Skip if already has qualifying doc
        if has_qualifying_doc(event_id):
            print("    ✗ Skipping: event already has qualifying document (>=400 chars)")
            continue

        # Link-backfill first: if a qualifying doc exists anywhere, clone it to this event
        existing = get_existing_qualifying_doc(url)
        if existing:
            clone = clone_document_to_event(event_id, authority, existing)
            if clone:
                length = len(existing.get('clean_text') or '')
                row = {
                    'event_id': event_id,
                    'authority': authority,
                    'url': clone['url'],
                    'source_type': 'link_backfill',
                    'document_id': clone['document_id'],
                    'clean_text_length': length,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
                created_docs.append(row)
                append_canonical_csv(row)
                link_backfills += 1
                print(f"    ✓ Link-backfilled document ({length} chars)")
                # Respectful pacing even on link-backfill
                time.sleep(0.2)
                continue
            else:
                print("    ✗ Link-backfill attempt failed; will try scrape")

        # Check Firecrawl limit
        if firecrawl_urls_used >= MAX_FIRECRAWL_URLS:
            print(f"    ✗ Reached Firecrawl URL limit ({MAX_FIRECRAWL_URLS})")
            break

        # Check robots.txt
        if not robots_checker.is_allowed(url):
            print(f"    ✗ Blocked by robots.txt")
            log_robots_block(authority, url, "disallowed by robots.txt")
            robots_blocks += 1
            continue

        # Fetch with Firecrawl (with timeout)
        firecrawl_urls_used += 1
        try:
            content = fetch_with_firecrawl(url, authority, fc_app)
        except Exception as e:
            print(f"    ✗ Firecrawl exception: {e}")
            failed_fetches += 1
            log_fetch_failure(authority, url, f"firecrawl_exception: {e}")
            continue

        if content is None:
            print(f"    ✗ Failed to fetch content")
            failed_fetches += 1
            log_fetch_failure(authority, url, "no_content")
            continue

        # Check content length
        text_length = len(content['text'])
        if text_length < 400:
            print(f"    ✗ Content too short ({text_length} chars)")
            failed_fetches += 1
            log_fetch_failure(authority, url, f"short_content:{text_length}")
            continue

        # Create document (per-event insert with fallback if source_url is unique)
        created = create_document(event_id, url, content)
        if created:
            print(f"    ✓ Created document ({text_length} chars)")
            row = {
                'event_id': event_id,
                'authority': authority,
                'url': created['url'],
                'source_type': 'scrape',
                'document_id': created['document_id'],
                'clean_text_length': text_length,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
            created_docs.append(row)
            append_canonical_csv(row)
            scrapes += 1
        else:
            print(f"    ✗ Failed to create document")
            failed_fetches += 1
            log_fetch_failure(authority, url, "db_insert_failed")

        # Rate limiting and progress update
        time.sleep(1.0)  # Increased delay to be more respectful

        # Progress checkpoint every 10 items
        if i % 10 == 0:
            print(f"    Progress: {i}/{len(candidates)} processed, {len(created_docs)} created")

    print()
    print("CANONICAL DOC CREATION RESULTS")
    print("-" * 40)
    print(f"Documents created: {len(created_docs)} (link-backfills: {link_backfills}, scrapes: {scrapes})")
    print(f"Firecrawl URLs used: {firecrawl_urls_used}")
    print(f"Robots.txt blocks: {robots_blocks}")
    print(f"Failed fetches: {failed_fetches}")

    if created_docs:
        lengths = [doc['clean_text_length'] for doc in created_docs]
        median_length = sorted(lengths)[len(lengths) // 2]
        print(f"Median document length: {median_length} chars")

    print()

    # MVP pass criteria: do not hard-fail; proceed even with small cohorts
    if len(created_docs) == 0:
        print("! STEP 2: COMPLETE (no documents created) – check targets_zero_doc.csv and robots/fetch logs")
    else:
        print(f"✓ STEP 2: PASS (MVP) – created {len(created_docs)} documents → {CANONICAL_DOCS_CSV}")
    print()


if __name__ == "__main__":
    main()
