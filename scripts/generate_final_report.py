#!/usr/bin/env python3
"""
Generate Final Executive Report

Produces investor-ready final_report.md with coverage, freshness, failures, and cost metrics.

Usage:
    .venv/bin/python scripts/generate_final_report.py
"""

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

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


def get_coverage_by_authority(conn) -> List[Tuple]:
    """Get event/document counts and last pub date by authority."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            e.authority,
            COUNT(DISTINCT e.event_id) AS event_count,
            COUNT(DISTINCT d.document_id) AS document_count,
            MAX(e.pub_date) AS last_pub_date,
            EXTRACT(DAY FROM NOW() - MAX(e.pub_date)) AS days_since_last_pub
        FROM events e
        LEFT JOIN documents d ON d.event_id = e.event_id
        GROUP BY e.authority
        ORDER BY e.authority;
    """)
    
    return cur.fetchall()


def get_freshness_metrics(conn) -> Dict:
    """Get freshness metrics (% of events in last 7/30/90 days)."""
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM events")
    total_events = cur.fetchone()[0]
    
    if total_events == 0:
        return {"total": 0, "last_7d": 0, "last_30d": 0, "last_90d": 0, "pct_7d": 0, "pct_30d": 0, "pct_90d": 0}
    
    cur.execute("SELECT COUNT(*) FROM events WHERE pub_date >= NOW() - INTERVAL '7 days'")
    last_7d = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM events WHERE pub_date >= NOW() - INTERVAL '30 days'")
    last_30d = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM events WHERE pub_date >= NOW() - INTERVAL '90 days'")
    last_90d = cur.fetchone()[0]
    
    return {
        "total": total_events,
        "last_7d": last_7d,
        "last_30d": last_30d,
        "last_90d": last_90d,
        "pct_7d": round(100.0 * last_7d / total_events, 1),
        "pct_30d": round(100.0 * last_30d / total_events, 1),
        "pct_90d": round(100.0 * last_90d / total_events, 1)
    }


def parse_failures() -> List[Tuple[str, int, str]]:
    """Parse fc_errors.csv and provider_events.csv to get top failures."""
    failures = defaultdict(lambda: {"count": 0, "sample_error": ""})
    
    # Parse fc_errors.csv
    fc_errors_path = "data/output/validation/latest/fc_errors.csv"
    if os.path.exists(fc_errors_path):
        try:
            with open(fc_errors_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    domain = row.get("domain", "unknown")
                    error = row.get("error", "")
                    failures[domain]["count"] += 1
                    if not failures[domain]["sample_error"]:
                        failures[domain]["sample_error"] = error[:100]
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
                        from urllib.parse import urlparse
                        domain = urlparse(url).netloc if url else "unknown"
                        failures[domain]["count"] += 1
                        if not failures[domain]["sample_error"]:
                            failures[domain]["sample_error"] = status[:100]
        except Exception:
            pass
    
    # Sort by count descending
    sorted_failures = sorted(
        [(domain, data["count"], data["sample_error"]) for domain, data in failures.items()],
        key=lambda x: x[1],
        reverse=True
    )
    
    return sorted_failures[:10]  # Top 10


def count_robots_blocks() -> int:
    """Count URLs blocked by robots.txt."""
    robots_blocked_path = "data/output/validation/latest/robots_blocked.csv"
    
    if not os.path.exists(robots_blocked_path):
        return 0
    
    try:
        with open(robots_blocked_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return sum(1 for _ in reader)
    except Exception:
        return 0


def main():
    output_path = "data/output/validation/latest/final_report.md"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print("Generating final executive report...")
    
    conn = get_db()
    
    # Get metrics
    coverage = get_coverage_by_authority(conn)
    freshness = get_freshness_metrics(conn)
    failures = parse_failures()
    robots_blocks = count_robots_blocks()
    
    # Get total counts
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM events")
    total_events = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM documents")
    total_documents = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(pub_date), MAX(pub_date) FROM events")
    date_range = cur.fetchone()
    
    conn.close()
    
    # Write report
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# AseanForge Final Executive Report\n\n")
        f.write(f"**Generated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
        
        f.write("## Summary\n\n")
        f.write(f"- **Total Events**: {total_events:,}\n")
        f.write(f"- **Total Documents**: {total_documents:,}\n")
        if date_range and date_range[0] and date_range[1]:
            f.write(f"- **Date Range**: {date_range[0].strftime('%Y-%m-%d')} to {date_range[1].strftime('%Y-%m-%d')}\n")
        f.write("\n")
        
        f.write("## Coverage by Authority\n\n")
        f.write("| Authority | Events | Documents | Last Pub Date | Days Since | Status |\n")
        f.write("|-----------|--------|-----------|---------------|------------|--------|\n")
        
        for auth, event_count, doc_count, last_pub, days_since in coverage:
            status = "Active" if days_since and days_since < 30 else "Stale"
            last_pub_str = last_pub.strftime('%Y-%m-%d') if last_pub else "N/A"
            days_str = f"{int(days_since)}" if days_since else "N/A"
            f.write(f"| {auth} | {event_count:,} | {doc_count:,} | {last_pub_str} | {days_str} | {status} |\n")
        
        f.write("\n")
        
        f.write("## Freshness Metrics\n\n")
        f.write(f"- **Last 7 days**: {freshness['last_7d']:,} events ({freshness['pct_7d']}%)\n")
        f.write(f"- **Last 30 days**: {freshness['last_30d']:,} events ({freshness['pct_30d']}%)\n")
        f.write(f"- **Last 90 days**: {freshness['last_90d']:,} events ({freshness['pct_90d']}%)\n")
        f.write("\n")
        
        f.write("## Top Failures\n\n")
        if failures:
            f.write("| Domain | Error Count | Sample Error |\n")
            f.write("|--------|-------------|-------------|\n")
            for domain, count, sample_error in failures:
                f.write(f"| {domain} | {count} | {sample_error} |\n")
        else:
            f.write("No failures recorded.\n")
        f.write("\n")
        
        f.write("## robots.txt Compliance\n\n")
        f.write(f"- **URLs Blocked**: {robots_blocks}\n")
        f.write("\n")
        
        f.write("## Cost Summary\n\n")
        f.write("*Cost tracking requires integration with Firecrawl and OpenAI usage APIs.*\n\n")
        f.write("- **Firecrawl Credits**: See `account_usage_start.json` and `account_usage_end.json` for deltas\n")
        f.write("- **OpenAI Batch API**: See `enrichment_report.md` for projected costs\n")
        f.write("\n")
    
    print(f"✓ Final report written to: {output_path}")


if __name__ == "__main__":
    main()

