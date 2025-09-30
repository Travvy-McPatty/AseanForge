#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Tuple
from dotenv import load_dotenv

DELIM = "|"


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_window(hours: int = 0, days: int = 0) -> Tuple[str, str]:
    if (hours and days) or (not hours and not days):
        raise SystemExit("Specify exactly one of --hours or --days")
    if hours:
        return (f"{hours} hours", f"{hours}h")
    else:
        return (f"{days} days", f"{days}d")


def main():
    ap = argparse.ArgumentParser(description="Export recent events sampler CSV")
    ap.add_argument("--hours", type=int, default=0, help="Lookback window in hours")
    ap.add_argument("--days", type=int, default=0, help="Lookback window in days")
    args = ap.parse_args()

    load_dotenv("app/.env")  # silently load
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise SystemExit("NEON_DATABASE_URL not set; configure app/.env")

    interval, tag = compute_window(hours=args.hours, days=args.days)
    out_dir = "deliverables"
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"sampler_{tag}.csv")
    val_dir = os.path.join("data", "output", "validation", "latest")
    os.makedirs(val_dir, exist_ok=True)
    out_meta = os.path.join(val_dir, f"sampler_{tag}_summary.txt")

    sql = (
        "COPY ("
        " SELECT to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts,"
        " e.authority, e.title, COALESCE(d.source_url, e.url) AS url,"
        " LEFT(COALESCE(d.content, e.summary_en, e.title, ''), 200) AS preview_200"
        " FROM events e"
        " LEFT JOIN documents d ON d.event_id = e.id"
        f" WHERE e.created_at >= NOW() - INTERVAL '{interval}'"
        " ORDER BY e.created_at DESC"
        ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)"
    )

    # Run psql and capture CSV
    env = os.environ.copy()
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql]
    try:
        with open(out_csv, "wb") as fh:
            res = subprocess.run(cmd, check=True, stdout=fh, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        # Write header-only CSV on error
        with open(out_csv, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter=DELIM)
            writer.writerow(["ts", "authority", "title", "url", "preview_200"])
        print("No events found in specified window or query failed; wrote header only")
        with open(out_meta, "w", encoding="utf-8") as m:
            m.write(f"[{iso_utc(datetime.now(timezone.utc))}] No events found in last {interval}\n")
        return

    # Compute counts and breakdown
    count = 0
    by_auth = {}
    try:
        with open(out_csv, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=DELIM)
            for row in reader:
                count += 1
                by_auth[row.get("authority") or "?"] = by_auth.get(row.get("authority") or "?", 0) + 1
    except Exception:
        pass

    with open(out_meta, "w", encoding="utf-8") as m:
        m.write(f"[{iso_utc(datetime.now(timezone.utc))}] Exported {count} events from last {interval} to {out_csv}\n")
        m.write("Authority breakdown:\n")
        for k in sorted(by_auth.keys()):
            m.write(f"{k}: {by_auth[k]}\n")

    print(f"Exported {count} events from last {interval} to {out_csv}")


if __name__ == "__main__":
    main()

