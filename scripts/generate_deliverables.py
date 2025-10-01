#!/usr/bin/env python3
"""
Generate CSV Deliverables

Produces investor-ready CSV files:
- sampler_24h.csv
- sampler_7d.csv
- coverage_by_authority.csv
- failures_top_domains.csv

Usage:
    .venv/bin/python scripts/generate_deliverables.py
"""

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Tuple
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2


def get_db():
    """Get database connection."""
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL not set")
    return psycopg2.connect(url)


def generate_sampler_24h(conn, output_dir: str):
    """Generate sampler_24h.csv."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
            e.authority,
            e.title,
            COALESCE(d.source_url, e.url) AS url,
            LEFT(COALESCE(d.clean_text, e.summary_en, e.title, ''), 200) AS preview_200
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        WHERE e.access_ts >= NOW() - INTERVAL '24 hours'
        ORDER BY e.access_ts DESC
        LIMIT 50;
    """)
    
    rows = cur.fetchall()
    
    output_path = os.path.join(output_dir, "sampler_24h.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "authority", "title", "url", "preview_200"])
        writer.writerows(rows)
    
    print(f"  ✓ sampler_24h.csv ({len(rows)} rows)")


def generate_sampler_7d(conn, output_dir: str):
    """Generate sampler_7d.csv."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
            e.authority,
            e.title,
            COALESCE(d.source_url, e.url) AS url,
            LEFT(COALESCE(d.clean_text, e.summary_en, e.title, ''), 200) AS preview_200
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        WHERE e.access_ts >= NOW() - INTERVAL '7 days'
        ORDER BY e.access_ts DESC
        LIMIT 200;
    """)
    
    rows = cur.fetchall()
    
    output_path = os.path.join(output_dir, "sampler_7d.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "authority", "title", "url", "preview_200"])
        writer.writerows(rows)
    
    print(f"  ✓ sampler_7d.csv ({len(rows)} rows)")


def generate_coverage_by_authority(conn, output_dir: str):
    """Generate coverage_by_authority.csv."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            e.authority,
            COUNT(DISTINCT e.event_id) AS event_count,
            COUNT(DISTINCT d.document_id) AS document_count,
            to_char(MAX(e.pub_date) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS last_pub_date,
            EXTRACT(DAY FROM NOW() - MAX(e.pub_date))::INTEGER AS days_since_last_pub
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        GROUP BY e.authority
        ORDER BY e.authority;
    """)
    
    rows = cur.fetchall()
    
    output_path = os.path.join(output_dir, "coverage_by_authority.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["authority", "event_count", "document_count", "last_pub_date", "days_since_last_pub"])
        writer.writerows(rows)
    
    print(f"  ✓ coverage_by_authority.csv ({len(rows)} rows)")


def generate_failures_top_domains(output_dir: str):
    """Generate failures_top_domains.csv."""
    failures = defaultdict(lambda: {"count": 0, "sample_error": "", "sample_url": ""})
    
    # Parse fc_errors.csv
    fc_errors_path = "data/output/validation/latest/fc_errors.csv"
    if os.path.exists(fc_errors_path):
        try:
            with open(fc_errors_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    domain = row.get("domain", "unknown")
                    url = row.get("url", "")
                    error = row.get("error", "")
                    failures[domain]["count"] += 1
                    if not failures[domain]["sample_error"]:
                        failures[domain]["sample_error"] = error[:100]
                        failures[domain]["sample_url"] = url
        except Exception:
            pass
    
    # Parse provider_events.csv for errors
    provider_events_path = "data/output/validation/latest/provider_events.csv"
    if os.path.exists(provider_events_path):
        try:
            with open(provider_events_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    status = row.get("status_code_or_error", "")
                    if status and status not in ("ok", "200", "fallback"):
                        url = row.get("url", "")
                        domain = urlparse(url).netloc if url else "unknown"
                        failures[domain]["count"] += 1
                        if not failures[domain]["sample_error"]:
                            failures[domain]["sample_error"] = status[:100]
                            failures[domain]["sample_url"] = url
        except Exception:
            pass
    
    # Sort by count descending
    sorted_failures = sorted(
        [(domain, data["count"], data["sample_error"], data["sample_url"]) for domain, data in failures.items()],
        key=lambda x: x[1],
        reverse=True
    )[:20]  # Top 20
    
    output_path = os.path.join(output_dir, "failures_top_domains.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["domain", "error_count", "sample_error", "sample_url"])
        writer.writerows(sorted_failures)
    
    print(f"  ✓ failures_top_domains.csv ({len(sorted_failures)} rows)")


def main():
    output_dir = "data/output/validation/latest/deliverables"
    os.makedirs(output_dir, exist_ok=True)
    
    print("Generating CSV deliverables...")
    
    conn = get_db()
    
    generate_sampler_24h(conn, output_dir)
    generate_sampler_7d(conn, output_dir)
    generate_coverage_by_authority(conn, output_dir)
    
    conn.close()
    
    generate_failures_top_domains(output_dir)
    
    print(f"\n✓ All deliverables written to: {output_dir}")


if __name__ == "__main__":
    main()

