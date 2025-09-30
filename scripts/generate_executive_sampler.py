#!/usr/bin/env python3
import csv
import os
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from dotenv import load_dotenv

DELIM = "|"


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_events(db_url: str, interval: str, limit: int = 12) -> List[Dict[str, str]]:
    sql = (
        "COPY ("
        " SELECT to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts,"
        " e.authority, e.title, COALESCE(d.source_url, e.url) AS url,"
        " LEFT(COALESCE(d.clean_text, e.summary_en, e.title, ''), 200) AS preview_200"
        " FROM events e LEFT JOIN documents d ON d.event_id = e.event_id"
        f" WHERE e.access_ts >= NOW() - INTERVAL '{interval}'"
        " ORDER BY e.access_ts DESC"
        f" LIMIT {limit}"
        ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)"
    )
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql]
    res = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    text = res.stdout.decode("utf-8", errors="ignore")
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(text.splitlines(), delimiter=DELIM)
    for r in reader:
        rows.append(r)
    return rows


def main():
    load_dotenv("app/.env")
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise SystemExit("NEON_DATABASE_URL not set; configure app/.env")

    out_md = os.path.join("deliverables", "executive_sampler.md")
    os.makedirs("deliverables", exist_ok=True)

    # Adaptive window: start with 14d, then 30d
    intervals = ["14 days", "30 days"]
    used_interval = intervals[0]
    rows: List[Dict[str, str]] = []
    for it in intervals:
        rows = fetch_events(db_url, it, limit=12)
        used_interval = it
        if rows:
            break

    now = datetime.now(timezone.utc)
    start = now - (timedelta(days=14) if used_interval.startswith("14") else timedelta(days=30))

    # Group by authority
    by_auth: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        by_auth.setdefault(r.get("authority") or "?", []).append(r)

    with open(out_md, "w", encoding="utf-8") as fh:
        fh.write("# Executive Policy Sampler\n\n")
        fh.write(f"Period: {start.date()} to {now.date()}\n")
        fh.write(f"Authorities: {len(by_auth)}\n")
        fh.write(f"Total events: {len(rows)}\n")
        fh.write(f"Effective window: {used_interval}\n\n")
        if not rows:
            fh.write("No events available in the last 14-30 days.\n")
        else:
            for auth in sorted(by_auth.keys()):
                fh.write(f"## {auth}\n\n")
                for r in by_auth[auth]:
                    title = r.get("title") or "(untitled)"
                    url = r.get("url") or ""
                    date = (r.get("ts") or "").split("T")[0]
                    preview = r.get("preview_200") or ""
                    fh.write(f"### [{title}]({url})\n")
                    fh.write(f"**Date**: {date}\n\n")
                    fh.write(f"{preview}\n\n")
                    fh.write("---\n\n")

    print(f"Wrote {out_md}")


if __name__ == "__main__":
    main()

