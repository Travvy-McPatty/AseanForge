#!/usr/bin/env python3
"""
Report Stub Extensions

Usage:
  python app/report_stub.py matches --since-days=1 --top=3

Emits a markdown section summarizing top matches created in the last N days.
"""
from __future__ import annotations
import argparse, os, sys, datetime as dt
from typing import List, Dict, Any

from pathlib import Path
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
except Exception:
    pass

import psycopg2


def get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def fmt_cap(p: Dict[str, Any]) -> str:
    v = p.get("capacity_value")
    u = p.get("capacity_unit") or ""
    return f"{v} {u}" if v is not None else "-"


def cmd_matches(since_days: int, top: int) -> int:
    conn = get_db()
    since_ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=int(since_days))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.score_numeric, m.drivers, m.blockers, m.next_action,
                   p.project_id, p.sponsor_name, p.country_iso3, p.technology, p.capacity_value, p.capacity_unit, p.data_room_url,
                   i.investor_id, i.name, i.type
            FROM matches m
            JOIN projects p ON p.project_id = m.project_id
            JOIN investors i ON i.investor_id = m.investor_id
            WHERE m.created_at >= %s
            ORDER BY m.score_numeric DESC
            LIMIT %s
            """,
            (since_ts, int(top))
        )
        rows = cur.fetchall()
        if not rows:
            print("### Top Matches\n\n_No recent matches found._\n")
            return 0
        print("### Top Matches\n")
        print("| Project | Investor | Score | Drivers | Blockers | Next Action | Link |")
        print("|---|---|---:|---|---|---|---|")
        for (score, drivers, blockers, next_action, pid, sponsor, iso3, tech, capv, capu, dr_url, iid, iname, itype) in rows:
            proj = f"{sponsor} ({iso3}) — {tech} — {capv} {capu}"
            inv = f"{iname} ({itype})"
            drivers_txt = "; ".join((drivers or [])[:5]) if drivers else ""
            blockers_txt = "; ".join((blockers or [])[:2]) if blockers else ""
            link = dr_url or ""
            print(f"| {proj} | {inv} | {float(score):.1f} | {drivers_txt} | {blockers_txt} | {next_action or ''} | {link} |")
        print()
    return 0


def main():
    ap = argparse.ArgumentParser(description="ASEANForge Report Stub")
    sub = ap.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("matches", help="Emit a markdown brief of top matches")
    m.add_argument("--since-days", type=int, default=1)
    m.add_argument("--top", type=int, default=3)

    args = ap.parse_args()
    if args.cmd == "matches":
        rc = cmd_matches(args.since_days, args.top)
        sys.exit(rc)


if __name__ == "__main__":
    main()

