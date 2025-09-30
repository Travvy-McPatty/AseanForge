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
        " SELECT to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts,"
        " e.authority, e.title, COALESCE(d.source_url, e.url) AS url,"
        " LEFT(COALESCE(d.clean_text, e.summary_en, e.title, ''), 200) AS preview_200"
        " FROM events e"
        " LEFT JOIN documents d ON d.event_id = e.event_id"
        f" WHERE e.access_ts >= NOW() - INTERVAL '{interval}'"
        " ORDER BY e.access_ts DESC"
        ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)"
    )

    # Run psql and capture CSV
    env = os.environ.copy()
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql]
    try:
        with open(out_csv, "wb") as fh:
            res = subprocess.run(cmd, check=True, stdout=fh, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        # Write header-only CSV on error, then attempt fallbacks below
        with open(out_csv, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter=DELIM)
            writer.writerow(["ts", "authority", "title", "url", "preview_200"])
        print("Initial query failed; wrote header only and will attempt fallback windows")

    # Compute counts and breakdown
    def count_rows(csv_path: str) -> tuple[int, dict]:
        c = 0
        by = {}
        try:
            with open(csv_path, "r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh, delimiter=DELIM)
                for row in reader:
                    c += 1
                    by[row.get("authority") or "?"] = by.get(row.get("authority") or "?", 0) + 1
        except Exception:
            pass
        return c, by

    count, by_auth = count_rows(out_csv)

    # Adaptive fallback windows
    effective_interval = interval
    fallback_applied = False
    def run_psql_with_interval(interval_str: str) -> bool:
        _sql = (
            "COPY ("
            " SELECT to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts,"
            " e.authority, e.title, COALESCE(d.source_url, e.url) AS url,"
            " LEFT(COALESCE(d.clean_text, e.summary_en, e.title, ''), 200) AS preview_200"
            " FROM events e"
            " LEFT JOIN documents d ON d.event_id = e.event_id"
            f" WHERE e.access_ts >= NOW() - INTERVAL '{interval_str}'"
            " ORDER BY e.access_ts DESC"
            ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)"
        )
        _cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", _sql]
        try:
            with open(out_csv, "wb") as fh:
                subprocess.run(_cmd, check=True, stdout=fh, stderr=subprocess.PIPE)
            return True
        except subprocess.CalledProcessError:
            return False

    # Fallbacks as specified
    if count == 0 and args.days == 7:
        for win in ("14 days", "30 days"):
            if run_psql_with_interval(win):
                c2, by2 = count_rows(out_csv)
                if c2 > 0:
                    count, by_auth = c2, by2
                    effective_interval = win
                    fallback_applied = True
                    break
    if count == 0 and args.hours == 24:
        for win in ("72 hours",):
            if run_psql_with_interval(win):
                c2, by2 = count_rows(out_csv)
                if c2 > 0:
                    count, by_auth = c2, by2
                    effective_interval = win
                    fallback_applied = True
                    break

    # Write summary
    with open(out_meta, "w", encoding="utf-8") as m:
        m.write(f"[{iso_utc(datetime.now(timezone.utc))}] Exported {count} events from last {effective_interval} to {out_csv}\n")
        m.write("Authority breakdown:\n")
        for k in sorted(by_auth.keys()):
            m.write(f"{k}: {by_auth[k]}\n")
        # Effective window line and field note
        if args.days:
            m.write(f"Effective window: {effective_interval} (fallback applied: {'yes' if fallback_applied else 'no'})\n")
        else:
            m.write(f"Effective window: {effective_interval} (fallback applied: {'yes' if fallback_applied else 'no'})\n")
        m.write("Date field: events.access_ts\n")

    print(f"Exported {count} events from last {effective_interval} to {out_csv}")


if __name__ == "__main__":
    main()

