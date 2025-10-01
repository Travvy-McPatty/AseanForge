#!/usr/bin/env python3
"""
Create Canonical Documents for Events Missing Text

Fetches canonical content using Firecrawl for events that lack documents or have no clean_text.
Limits to 60 events maximum, prioritizes last 90 days, checks robots.txt.

Usage:
    .venv/bin/python scripts/create_canonical_docs.py
"""

import os
import sys
import json
import hashlib
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from firecrawl import FirecrawlApp


MAX_EVENTS = 60
MIN_TEXT_LENGTH = 100
MEDIAN_TARGET = 500


def check_robots_txt(url, user_agent):
    """Check if URL is allowed by robots.txt."""
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        return rp.can_fetch(user_agent, url)
    except Exception:
        # If robots.txt check fails, allow by default
        return True


def compute_doc_hash(event_id, url):
    """Compute document hash for deduplication."""
    content = f"{event_id}:{url}"
    return hashlib.sha256(content.encode()).hexdigest()


def main():
    try:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
        cur = conn.cursor()
        
        firecrawl_api_key = os.getenv("FIRECRAWL_API_KEY")
        if not firecrawl_api_key:
            raise Exception("FIRECRAWL_API_KEY not found in .env")
        
        app = FirecrawlApp(api_key=firecrawl_api_key)
        
        robots_ua = os.getenv("ROBOTS_UA", "AseanForgeBot/1.0")
        
        print("=== Step 3: Create Canonical Documents ===\n")
        
        # Find events needing canonical docs (prioritize last 90 days)
        ninety_days_ago = (datetime.now(timezone.utc) - timedelta(days=90)).date()
        
        cur.execute("""
            SELECT e.event_id, e.url, e.authority, e.pub_date
            FROM events e
            WHERE (
                NOT EXISTS (
                    SELECT 1 FROM documents d WHERE d.event_id = e.event_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM documents d 
                    WHERE d.event_id = e.event_id 
                      AND d.clean_text IS NOT NULL 
                      AND LENGTH(d.clean_text) >= %s
                )
            )
            ORDER BY 
                CASE WHEN e.pub_date >= %s THEN 0 ELSE 1 END,
                e.pub_date DESC
            LIMIT %s;
        """, (MIN_TEXT_LENGTH, ninety_days_ago, MAX_EVENTS))
        
        events_to_process = cur.fetchall()
        
        print(f"Found {len(events_to_process)} events needing canonical docs (limit: {MAX_EVENTS})")
        print()
        
        # Track outcomes
        robots_blocked = []
        fc_errors = []
        created_docs = []
        clean_text_lengths = []
        
        for idx, (event_id, url, authority, pub_date) in enumerate(events_to_process, 1):
            print(f"[{idx}/{len(events_to_process)}] Processing: {url[:60]}...")
            
            # Check robots.txt
            if not check_robots_txt(url, robots_ua):
                print(f"  ⚠ Blocked by robots.txt")
                robots_blocked.append({
                    "url": url,
                    "authority": authority,
                    "reason": "robots.txt disallow"
                })
                continue
            
            # Fetch with Firecrawl
            try:
                # Determine waitFor based on authority
                wait_for = 5000 if authority in ["ASEAN", "IMDA"] else 2000
                
                result = app.scrape_url(url, params={
                    "formats": ["markdown", "html"],
                    "waitFor": wait_for,
                    "timeout": 60000
                })
                
                if not result or not result.get("markdown"):
                    print(f"  ❌ No content returned")
                    fc_errors.append({
                        "url": url,
                        "authority": authority,
                        "error": "No content returned"
                    })
                    continue
                
                clean_text = result["markdown"]
                
                # Apply quality gates
                if len(clean_text) < MIN_TEXT_LENGTH:
                    print(f"  ⚠ Thin content ({len(clean_text)} chars)")
                    fc_errors.append({
                        "url": url,
                        "authority": authority,
                        "error": f"Thin content ({len(clean_text)} chars)"
                    })
                    continue
                
                # Create document
                doc_hash = compute_doc_hash(event_id, url)
                
                cur.execute("""
                    INSERT INTO documents (event_id, source_url, clean_text, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING;
                """, (event_id, url, clean_text))
                conn.commit()
                
                print(f"  ✓ Created document ({len(clean_text)} chars)")
                created_docs.append({
                    "event_id": str(event_id),
                    "url": url,
                    "authority": authority,
                    "clean_text_length": len(clean_text)
                })
                clean_text_lengths.append(len(clean_text))
                
                # Rate limiting
                time.sleep(1.2)
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ❌ Error: {error_msg[:50]}")
                fc_errors.append({
                    "url": url,
                    "authority": authority,
                    "error": error_msg
                })
        
        # Save robots_blocked.csv
        if robots_blocked:
            robots_csv_path = "data/output/validation/latest/robots_blocked.csv"
            os.makedirs(os.path.dirname(robots_csv_path), exist_ok=True)
            with open(robots_csv_path, "w") as f:
                f.write("url,authority,reason\n")
                for item in robots_blocked:
                    f.write(f"{item['url']},{item['authority']},{item['reason']}\n")
            print(f"\n✓ Robots blocked URLs saved to: {robots_csv_path}")
        
        # Update fc_errors.csv
        if fc_errors:
            fc_errors_path = "data/output/validation/latest/fc_errors.csv"
            os.makedirs(os.path.dirname(fc_errors_path), exist_ok=True)
            with open(fc_errors_path, "w") as f:
                f.write("url,authority,error\n")
                for item in fc_errors:
                    f.write(f"{item['url']},{item['authority']},\"{item['error']}\"\n")
            print(f"✓ Firecrawl errors saved to: {fc_errors_path}")
        
        # Verify pass criteria
        print("\n=== Verification ===\n")
        
        # Count events still lacking documents
        cur.execute("""
            SELECT COUNT(*)
            FROM events e
            WHERE NOT EXISTS (
                SELECT 1 FROM documents d WHERE d.event_id = e.event_id
            );
        """)
        events_without_docs = cur.fetchone()[0]
        print(f"Events without docs: {events_without_docs} (≤5 required)")
        
        global_pass = events_without_docs <= 5
        print(f"  {'✓ PASS' if global_pass else '❌ FAIL'}: Global ≤5")
        
        # Per-authority check
        cur.execute("""
            SELECT e.authority, COUNT(*)
            FROM events e
            WHERE NOT EXISTS (
                SELECT 1 FROM documents d WHERE d.event_id = e.event_id
            )
            GROUP BY e.authority
            ORDER BY COUNT(*) DESC;
        """)
        
        per_auth_pass = True
        max_per_auth = 0
        failing_authorities = []
        
        print("\nPer-Authority Events Without Docs:")
        for auth, count in cur.fetchall():
            status = "✓" if count <= 2 else "❌"
            print(f"  {status} {auth}: {count}")
            max_per_auth = max(max_per_auth, count)
            if count > 2:
                per_auth_pass = False
                failing_authorities.append((auth, count))
        
        print(f"\n  {'✓ PASS' if per_auth_pass else '❌ FAIL'}: All authorities ≤2")
        
        # Median clean_text length
        if clean_text_lengths:
            clean_text_lengths.sort()
            median_length = clean_text_lengths[len(clean_text_lengths) // 2]
        else:
            median_length = 0
        
        print(f"\nMedian clean_text length: {median_length} chars (≥{MEDIAN_TARGET} required)")
        
        quality_pass = median_length >= MEDIAN_TARGET
        print(f"  {'✓ PASS' if quality_pass else '❌ FAIL'}: Median ≥{MEDIAN_TARGET}")
        
        # Final verdict
        all_pass = global_pass and per_auth_pass and quality_pass
        
        print(f"\n{'='*50}")
        print(f"Step 3 Result: {'✓ PASS' if all_pass else '❌ FAIL'}")
        print(f"{'='*50}\n")
        
        print(f"Summary:")
        print(f"  Created docs: {len(created_docs)}")
        print(f"  Robots blocked: {len(robots_blocked)}")
        print(f"  Firecrawl errors: {len(fc_errors)}")
        
        conn.close()
        
        if not all_pass:
            # Create blockers.md
            blockers_path = "data/output/validation/latest/blockers.md"
            os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
            
            with open(blockers_path, "w", encoding="utf-8") as f:
                f.write("# Step 3: Create Canonical Documents - FAILED\n\n")
                f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
                
                if not global_pass:
                    f.write(f"## Global Check FAILED\n\n")
                    f.write(f"- Events without docs: {events_without_docs}\n")
                    f.write(f"- Expected: ≤5\n\n")
                
                if not per_auth_pass:
                    f.write(f"## Per-Authority Check FAILED\n\n")
                    f.write(f"Authorities exceeding 2 events without docs:\n\n")
                    for auth, count in failing_authorities:
                        f.write(f"- {auth}: {count}\n")
                    f.write("\n")
                
                if not quality_pass:
                    f.write(f"## Quality Check FAILED\n\n")
                    f.write(f"- Median clean_text length: {median_length} chars\n")
                    f.write(f"- Expected: ≥{MEDIAN_TARGET} chars\n\n")
                
                f.write(f"## Problematic URLs\n\n")
                f.write(f"Robots blocked: {len(robots_blocked)}\n")
                f.write(f"Firecrawl errors: {len(fc_errors)}\n\n")
            
            print(f"❌ Blockers documented in: {blockers_path}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ FAIL: {e}")
        import traceback
        traceback.print_exc()
        
        # Create blockers.md
        blockers_path = "data/output/validation/latest/blockers.md"
        os.makedirs(os.path.dirname(blockers_path), exist_ok=True)
        
        with open(blockers_path, "w", encoding="utf-8") as f:
            f.write("# Step 3: Create Canonical Documents - FAILED\n\n")
            f.write(f"**Timestamp**: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(f"**Error**: {str(e)}\n\n")
        
        print(f"\n❌ Blockers documented in: {blockers_path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

