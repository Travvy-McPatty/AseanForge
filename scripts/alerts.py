#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Set
from dotenv import load_dotenv
import yaml

DELIM = "|"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_rules(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    rules = data.get("rules") or []
    out = []
    for r in rules:
        name = r.get("name"); match = r.get("match") or []; auths = r.get("authorities") or []
        if not name or not match or not auths:
            # Skip incomplete rules
            continue
        out.append({
            "name": str(name),
            "match": [str(x).lower() for x in match],
            "authorities": [str(a).upper() for a in auths]
        })
    return out


def fetch_events(db_url: str, window_hours: int) -> List[Dict]:
    sql = (
        "COPY ("
        " SELECT e.event_id, to_char(e.access_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts,"
        " e.authority, e.title, e.url, e.summary_en, d.source_url, d.clean_text"
        " FROM events e LEFT JOIN documents d ON d.event_id = e.event_id"
        f" WHERE e.access_ts >= NOW() - INTERVAL '{window_hours} hours'"
        " ORDER BY e.access_ts DESC"
        ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)"
    )
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql]
    res = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    text = res.stdout.decode("utf-8", errors="ignore")
    rows: List[Dict] = []
    reader = csv.DictReader(text.splitlines(), delimiter=DELIM)
    for r in reader:
        rows.append(r)
    return rows


def run_alerts(rules_path: str, window_hours: int) -> Tuple[int, Dict[str, int]]:
    load_dotenv("app/.env")
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise SystemExit("NEON_DATABASE_URL not set; configure app/.env")
    rules = load_rules(rules_path)
    events = fetch_events(db_url, window_hours)

    out_path = os.path.join("deliverables", "alerts_latest.csv")
    os.makedirs("deliverables", exist_ok=True)
    val_dir = os.path.join("data", "output", "validation", "latest")
    os.makedirs(val_dir, exist_ok=True)
    summary_path = os.path.join(val_dir, "alerts_summary.txt")

    # Matching
    alerts: List[Tuple[str, Dict]] = []
    seen: Set[Tuple[str, str]] = set()
    for ev in events:
        ev_id = ev.get("id")
        auth = (ev.get("authority") or "").upper()
        hay = " ".join([
            ev.get("title") or "",
            ev.get("summary_en") or "",
            ev.get("content") or "",
        ]).lower()
        for rule in rules:
            if auth not in rule["authorities"]:
                continue
            if any(k in hay for k in rule["match"]):
                key = (rule["name"], ev_id)
                if key in seen:
                    continue
                seen.add(key)
                alerts.append((rule["name"], ev))

    # Deterministic ordering
    def sort_key(item: Tuple[str, Dict]):
        rule_name, ev = item
        return (
            ev.get("ts") or "",
            rule_name,
            ev.get("id") or "",
        )
    alerts.sort(key=sort_key, reverse=True)

    # If empty, adaptively widen window
    effective_hours = window_hours
    fallback_applied = False
    if not alerts and window_hours == 168:
        for nh in (336, 720):  # 14d, 30d
            events = fetch_events(db_url, nh)
            alerts = []
            seen.clear()
            for ev in events:
                ev_id = ev.get("id")
                auth = (ev.get("authority") or "").upper()
                hay = " ".join([
                    ev.get("title") or "",
                    ev.get("summary_en") or "",
                    ev.get("content") or "",
                ]).lower()
                for rule in rules:
                    if auth not in rule["authorities"]:
                        continue
                    if any(k in hay for k in rule["match"]):
                        key = (rule["name"], ev_id)
                        if key in seen:
                            continue
                        seen.add(key)
                        alerts.append((rule["name"], ev))
            alerts.sort(key=sort_key, reverse=True)
            if alerts:
                effective_hours = nh
                fallback_applied = True
                break

    # Write CSV (final alerts set)
    written = 0
    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter=DELIM)
        w.writerow(["rule", "ts", "authority", "title", "url", "preview_200"])
        for rule_name, ev in alerts:
            url = ev.get("source_url") or ev.get("url") or ""
            preview = (ev.get("content") or ev.get("summary_en") or ev.get("title") or "")[:200]
            w.writerow([rule_name, ev.get("ts"), ev.get("authority"), ev.get("title"), url, preview])
            written += 1

    # Summary
    by_rule: Dict[str, int] = {}
    for r in [r["name"] for r in rules]:
        by_rule[r] = 0
    for rule_name, _ in alerts:
        by_rule[rule_name] = by_rule.get(rule_name, 0) + 1

    with open(summary_path, "w", encoding="utf-8") as fh:
        fh.write(f"[{iso_utc_now()}] Alerts run completed\n")
        fh.write(f"Window: last {effective_hours} hours\n")
        fh.write(f"Effective window: {effective_hours} hours (fallback applied: {'yes' if fallback_applied else 'no'})\n")
        fh.write(f"Total events scanned: {len(events)}\n")
        fh.write(f"Total alerts generated: {written}\n\n")
        fh.write("Breakdown by rule:\n")
        for k in sorted(by_rule.keys()):
            fh.write(f"{k}: {by_rule[k]} alerts\n")
    print(f"Generated {written} alerts across {len(rules)} rules → deliverables/alerts_latest.csv (effective window {effective_hours}h)")
    return written, by_rule


def main():
    ap = argparse.ArgumentParser(description="Keyword-based alerts over recent policy events")
    ap.add_argument("--rules", default="configs/alerts.yaml", help="Path to alerts YAML config")
    ap.add_argument("--window-hours", type=int, default=168, help="Lookback window in hours (default 168=7d)")
    args = ap.parse_args()
    run_alerts(args.rules, args.window_hours)


if __name__ == "__main__":
    main()

