#!/usr/bin/env python3
import csv
import os
from datetime import datetime, timezone
from typing import Dict, List

DELIM = "|"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    alerts_csv = os.path.join("deliverables", "alerts_latest.csv")
    summary_txt = os.path.join("data", "output", "validation", "latest", "alerts_summary.txt")
    out_md = os.path.join("deliverables", "alerts_digest.md")

    os.makedirs("deliverables", exist_ok=True)

    # Load alerts
    alerts: List[Dict[str, str]] = []
    if os.path.exists(alerts_csv):
        with open(alerts_csv, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=DELIM)
            for r in reader:
                alerts.append(r)

    # Read effective window line if present
    eff_window = "unknown"
    if os.path.exists(summary_txt):
        with open(summary_txt, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.lower().startswith("effective window:"):
                    eff_window = line.strip().split(":", 1)[1].strip()
                    break

    # Group by rule and sort by ts desc
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for a in alerts:
        grouped.setdefault(a.get("rule", "(unknown)"), []).append(a)
    for k in grouped:
        grouped[k].sort(key=lambda x: x.get("ts") or "", reverse=True)

    total_alerts = sum(len(v) for v in grouped.values())

    with open(out_md, "w", encoding="utf-8") as fh:
        fh.write("# Alerts Digest\n\n")
        fh.write(f"Generated: {iso_utc_now()}\n")
        fh.write(f"Window: {eff_window}\n")
        fh.write(f"Total alerts: {total_alerts}\n\n")
        if total_alerts == 0:
            fh.write("No alerts available for the effective window.\n")
        else:
            for rule_name in sorted(grouped.keys()):
                fh.write(f"## {rule_name} ({len(grouped[rule_name])} alerts)\n\n")
                for i, a in enumerate(grouped[rule_name][:10], start=1):
                    title = a.get("title") or "(untitled)"
                    auth = a.get("authority") or "?"
                    ts = (a.get("ts") or "").split("T")[0]
                    url = a.get("url") or ""
                    preview = a.get("preview_200") or ""
                    fh.write(f"{i}. **{title}** ({auth}, {ts})\n")
                    fh.write(f"   - URL: {url}\n")
                    fh.write(f"   - Preview: {preview}\n\n")

    print(f"Wrote {out_md}")


if __name__ == "__main__":
    main()

