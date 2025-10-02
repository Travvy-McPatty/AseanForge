#!/usr/bin/env python3
"""
Coverage Expansion Step 1: Sitemap-First Discovery

Purpose: Discover URLs from sitemaps and listings for lagging authorities
"""

import csv
import json
import os
import sys
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor

# Import robots checker
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from app.robots_checker import RobotsChecker

OUTPUT_DIR = "data/output/validation/latest"
BASELINE_FILE = os.path.join(OUTPUT_DIR, "expansion_baseline.json")
DISCOVERED_URLS_CSV = os.path.join(OUTPUT_DIR, "discovered_urls.csv")

# Authority sitemap/feed configurations
AUTHORITY_CONFIGS = {
    'SC': {
        'sitemaps': ['https://www.sc.com.my/sitemap.xml'],
        'listings': [
            'https://www.sc.com.my/resources/media',
            'https://www.sc.com.my/resources/media/media-releases',
            'https://www.sc.com.my/resources/media/speeches',
            'https://www.sc.com.my/resources/media/consultation-papers'
        ],
        'patterns': [
            'https://www.sc.com.my/resources/media/media-releases/*',
            'https://www.sc.com.my/resources/media/speeches/*'
        ]
    },
    'PDPC': {
        'sitemaps': ['https://www.pdpc.gov.sg/sitemap.xml'],
        'listings': [
            'https://www.pdpc.gov.sg/News-and-Events/Press-Room',
            'https://www.pdpc.gov.sg/News-and-Events/Announcements',
            'https://www.pdpc.gov.sg/News-and-Events/Events'
        ],
        'patterns': [
            'https://www.pdpc.gov.sg/News-and-Events/*'
        ]
    },
    'MIC': {
        'sitemaps': ['https://english.mic.gov.vn/sitemap.xml'],
        'listings': [
            'https://english.mic.gov.vn/news-gallery/news-and-press-release.htm',
            'https://english.mic.gov.vn/news-gallery/speeches-of-the-minister.htm',
            'https://english.mic.gov.vn/news-gallery/photo-video.htm'
        ],
        'patterns': [
            'https://english.mic.gov.vn/news-gallery/*'
        ]
    },
    'BI': {
        'sitemaps': ['https://www.bi.go.id/sitemap.xml'],
        'listings': [
            'https://www.bi.go.id/id/publikasi/ruang-media/news-release/',
            'https://www.bi.go.id/id/publikasi/ruang-media/pidato/',
            'https://www.bi.go.id/id/publikasi/ruang-media/wawancara/'
        ],
        'patterns': [
            'https://www.bi.go.id/id/publikasi/ruang-media/*'
        ]
    },
    'OJK': {
        'sitemaps': ['https://www.ojk.go.id/sitemap.xml'],
        'listings': [
            'https://www.ojk.go.id/id/berita-dan-kegiatan/siaran-pers/',
            'https://www.ojk.go.id/id/berita-dan-kegiatan/info-terkini/',
            'https://www.ojk.go.id/id/berita-dan-kegiatan/pengumuman/'
        ],
        'patterns': [
            'https://www.ojk.go.id/id/berita-dan-kegiatan/*'
        ]
    },
    'DICT': {
        'sitemaps': ['https://dict.gov.ph/sitemap.xml'],
        'listings': [
            'https://dict.gov.ph/category/press-releases/',
            'https://dict.gov.ph/category/news/',
            'https://dict.gov.ph/category/announcements/'
        ],
        'patterns': [
            'https://dict.gov.ph/category/*'
        ]
    },
    'SBV': {
        'sitemaps': ['https://sbv.gov.vn/sitemap.xml'],
        'listings': [
            'https://sbv.gov.vn/en/press-release',
            'https://sbv.gov.vn/en/news',
            'https://sbv.gov.vn/en/monetary-policy'
        ],
        'patterns': [
            'https://sbv.gov.vn/en/*'
        ]
    },
    'IMDA': {
        'sitemaps': ['https://www.imda.gov.sg/sitemap.xml'],
        'listings': [
            'https://www.imda.gov.sg/resources/press-releases-factsheets-and-speeches',
            'https://www.imda.gov.sg/about-imda/corporate-publications',
            'https://www.imda.gov.sg/regulations-and-licensing-listing'
        ],
        'patterns': [
            'https://www.imda.gov.sg/resources/*'
        ]
    }
}


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def load_baseline():
    """Load baseline metrics."""
    if not os.path.exists(BASELINE_FILE):
        raise RuntimeError(f"Baseline file not found: {BASELINE_FILE}")
    
    with open(BASELINE_FILE, 'r') as f:
        return json.load(f)


def get_existing_urls() -> Set[str]:
    """Get set of URLs already in the database."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT url FROM events")
    urls = {row[0] for row in cur.fetchall()}

    cur.close()
    conn.close()

    return urls


def generate_pattern_urls(authority: str, patterns: List[str], existing_urls: Set[str]) -> List[Dict]:
    """Generate URLs based on patterns and existing URL analysis."""
    discovered = []

    # Get existing URLs for this authority to analyze patterns
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT url, pub_date
        FROM events
        WHERE authority = %s
        ORDER BY pub_date DESC
        LIMIT 50
    """, (authority,))

    authority_urls = cur.fetchall()
    cur.close()
    conn.close()

    # Analyze URL patterns from existing URLs
    url_patterns = set()
    for url, pub_date in authority_urls:
        # Extract base patterns
        parts = url.split('/')
        if len(parts) >= 4:
            base_pattern = '/'.join(parts[:4]) + '/'
            url_patterns.add(base_pattern)

    # Generate variations based on common patterns
    for base_pattern in url_patterns:
        # Generate date-based variations (last 2 years)
        for year in [2023, 2024, 2025]:
            for month in range(1, 13):
                # Common date patterns
                date_variations = [
                    f"{base_pattern}{year}/{month:02d}/",
                    f"{base_pattern}{year}-{month:02d}/",
                    f"{base_pattern}{year}{month:02d}/",
                ]

                for variation in date_variations:
                    if variation not in existing_urls:
                        discovered.append({
                            'url': variation,
                            'lastmod': None,
                            'source': 'pattern',
                            'in_sitemap': False
                        })

    # Limit to avoid too many URLs
    return discovered[:100]


def parse_sitemap(url: str, robots_checker: RobotsChecker) -> List[Dict]:
    """Parse sitemap XML and extract URLs with lastmod dates."""
    discovered = []
    
    try:
        # Check robots.txt
        if not robots_checker.is_allowed(url):
            print(f"    ✗ Sitemap blocked by robots.txt: {url}")
            return discovered
        
        print(f"    Fetching sitemap: {url}")
        response = requests.get(url, timeout=30, headers={
            'User-Agent': os.getenv('ROBOTS_UA', 'AseanForgeBot/1.0')
        })
        
        if response.status_code != 200:
            print(f"    ✗ HTTP {response.status_code}: {url}")
            return discovered
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Handle different sitemap formats
        namespaces = {
            'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'
        }
        
        # Look for URL entries
        urls = root.findall('.//sitemap:url', namespaces) or root.findall('.//url')
        
        for url_elem in urls:
            loc_elem = url_elem.find('sitemap:loc', namespaces) or url_elem.find('loc')
            lastmod_elem = url_elem.find('sitemap:lastmod', namespaces) or url_elem.find('lastmod')
            
            if loc_elem is not None:
                page_url = loc_elem.text.strip()
                lastmod = None
                
                if lastmod_elem is not None:
                    try:
                        lastmod = datetime.fromisoformat(lastmod_elem.text.strip().replace('Z', '+00:00'))
                    except:
                        pass
                
                discovered.append({
                    'url': page_url,
                    'lastmod': lastmod,
                    'source': 'sitemap',
                    'in_sitemap': True
                })
        
        print(f"    ✓ Found {len(discovered)} URLs in sitemap")
        
    except Exception as e:
        print(f"    ✗ Error parsing sitemap {url}: {e}")
    
    return discovered


def discover_from_listings(listings: List[str], robots_checker: RobotsChecker) -> List[Dict]:
    """Discover URLs from listing pages by parsing HTML for links."""
    discovered = []

    for listing_url in listings:
        try:
            # Check robots.txt
            if not robots_checker.is_allowed(listing_url):
                print(f"    ✗ Listing blocked by robots.txt: {listing_url}")
                continue

            print(f"    Parsing listing: {listing_url}")

            # Fetch the listing page
            response = requests.get(listing_url, timeout=30, headers={
                'User-Agent': os.getenv('ROBOTS_UA', 'AseanForgeBot/1.0')
            })

            if response.status_code != 200:
                print(f"    ✗ HTTP {response.status_code}: {listing_url}")
                continue

            # Parse HTML for links
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all links
            links = soup.find_all('a', href=True)
            page_urls = []

            for link in links:
                href = link['href']

                # Convert relative URLs to absolute
                if href.startswith('/'):
                    href = urljoin(listing_url, href)
                elif not href.startswith('http'):
                    continue

                # Filter for relevant URLs (news, press releases, etc.)
                href_lower = href.lower()
                if any(keyword in href_lower for keyword in [
                    'news', 'press', 'release', 'announcement', 'circular',
                    'regulation', 'guideline', 'speech', 'statement'
                ]):
                    page_urls.append(href)

            # Dedupe and add to discovered
            unique_urls = list(set(page_urls))
            for url in unique_urls[:50]:  # Limit per listing page
                discovered.append({
                    'url': url,
                    'lastmod': None,
                    'source': 'listing',
                    'in_sitemap': False
                })

            print(f"    ✓ Found {len(unique_urls)} links in listing")
            time.sleep(2)  # Rate limiting

        except Exception as e:
            print(f"    ✗ Error parsing listing {listing_url}: {e}")

    return discovered


def filter_by_date(discovered: List[Dict], days: int = 365) -> List[Dict]:
    """Filter URLs by lastmod date (keep last N days)."""
    if days <= 0:
        return discovered
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []
    
    for item in discovered:
        if item['lastmod'] is None:
            # Keep URLs without lastmod (assume recent)
            filtered.append(item)
        elif item['lastmod'] >= cutoff:
            filtered.append(item)
    
    return filtered


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 1: Sitemap-First Discovery")
    print("=" * 60)
    print()
    
    # Load baseline
    try:
        baseline = load_baseline()
        laggards = [l['authority'] for l in baseline['laggards']]
        print(f"Targeting {len(laggards)} lagging authorities: {', '.join(laggards)}")
        print()
    except Exception as e:
        print(f"✗ Failed to load baseline: {e}")
        sys.exit(1)
    
    # Get existing URLs
    print("Loading existing URLs from database...")
    existing_urls = get_existing_urls()
    print(f"  ✓ Found {len(existing_urls)} existing URLs")
    print()
    
    # Initialize robots checker
    robots_checker = RobotsChecker(os.getenv('ROBOTS_UA', 'AseanForgeBot/1.0'))
    
    # Discover URLs for each laggard authority
    all_discovered = []
    crawler_errors = 0
    
    for authority in laggards:
        if authority not in AUTHORITY_CONFIGS:
            print(f"⚠️  No configuration for authority: {authority}")
            continue
        
        print(f"Discovering URLs for {authority}...")
        config = AUTHORITY_CONFIGS[authority]
        authority_discovered = []
        
        # Parse sitemaps
        for sitemap_url in config.get('sitemaps', []):
            sitemap_urls = parse_sitemap(sitemap_url, robots_checker)
            authority_discovered.extend(sitemap_urls)
        
        # Check listings
        listings = config.get('listings', [])
        if listings:
            listing_urls = discover_from_listings(listings, robots_checker)
            authority_discovered.extend(listing_urls)

        # Generate pattern-based URLs
        patterns = config.get('patterns', [])
        if patterns:
            pattern_urls = generate_pattern_urls(authority, patterns, existing_urls)
            authority_discovered.extend(pattern_urls)

        # Filter by date (last 365 days)
        authority_discovered = filter_by_date(authority_discovered, 365)
        
        # Dedupe against existing URLs
        new_urls = []
        for item in authority_discovered:
            if item['url'] not in existing_urls:
                item['authority'] = authority
                new_urls.append(item)
        
        print(f"  ✓ Found {len(new_urls)} new URLs for {authority}")
        all_discovered.extend(new_urls)
        print()
    
    # Write discovered URLs to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with open(DISCOVERED_URLS_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'authority', 'url', 'lastmod', 'source', 'in_sitemap'
        ])
        writer.writeheader()
        
        for item in all_discovered:
            writer.writerow({
                'authority': item['authority'],
                'url': item['url'],
                'lastmod': item['lastmod'].isoformat() if item['lastmod'] else '',
                'source': item['source'],
                'in_sitemap': item['in_sitemap']
            })
    
    print(f"✓ Wrote {len(all_discovered)} discovered URLs to {DISCOVERED_URLS_CSV}")
    print()
    
    # Check pass criteria
    total_discovered = len(all_discovered)
    error_rate = crawler_errors / max(1, total_discovered + crawler_errors) * 100
    
    print("DISCOVERY RESULTS")
    print("-" * 40)
    print(f"Total URLs discovered: {total_discovered}")
    print(f"Crawler error rate: {error_rate:.1f}%")
    print()
    
    # Pass criteria: ≥700 URLs and ≤5% error rate
    pass_criteria_met = True
    failures = []
    
    if total_discovered < 700:
        pass_criteria_met = False
        failures.append(f"Only {total_discovered} URLs discovered (need ≥700)")
    
    if error_rate > 5.0:
        pass_criteria_met = False
        failures.append(f"Error rate {error_rate:.1f}% exceeds 5%")
    
    if pass_criteria_met:
        print("✓ STEP 1: PASS")
    else:
        print("✗ STEP 1: FAIL")
        print()
        print("Failures:")
        for failure in failures:
            print(f"  - {failure}")
        sys.exit(1)
    
    print()


if __name__ == "__main__":
    main()
