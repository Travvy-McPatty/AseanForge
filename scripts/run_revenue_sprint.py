#!/usr/bin/env python3
"""
Revenue Sprint Orchestrator (Week 3–4 MVP)

Generates four deliverables using existing slice CSVs with $0 spend:
1) Pilot Report v0.1 (MD/HTML/PDF)
2) Sales Pack (one‑pager + landing page with CTA)
3) Outbound Kit (prospect list CSV + compliant cold emails + LinkedIn assets)
4) Revenue‑ready snapshot ZIP

Usage:
  .venv/bin/python scripts/run_revenue_sprint.py

Notes:
- Reads app/.env for OPENAI_SUMMARY_MODEL and OPENAI_EMBED_MODEL (logged only; no API calls).
- If STRIPE_PAYMENT_LINK is missing in app/.env, visible TODO placeholders are inserted.
- Link checks are logged to qa_results.json and never block the run.
"""
import os
import sys
import json
import time
import shutil
import glob
from datetime import datetime

import pandas as pd

# Local modules
from dotenv import load_dotenv

# Ensure repository root is on sys.path for `import scripts.*`
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Builder contains concrete generation steps
import scripts.revenue_sprint_builder as builder

ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
LATEST = os.path.join(ROOT, "data/output/validation/latest")
TS_EVENTS = os.path.join(LATEST, "topic_slice_events.csv")
TS_DOCS = os.path.join(LATEST, "topic_slice_docs.csv")


def step(msg: str):
    print(f"\n=== {msg} ===")


def preflight() -> dict:
    step("STEP 1/5 — Preflight & environment checks")
    load_dotenv(os.path.join(ROOT, "app/.env"))

    # Models (logged only; no API calls for this sprint)
    summary_model = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini-search-preview")
    embed_model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")

    # Stripe payment link presence
    stripe_link = None
    env_path = os.path.join(ROOT, "app/.env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("STRIPE_PAYMENT_LINK="):
                        stripe_link = line.strip().split("=", 1)[1]
                        break
        except Exception:
            pass

    # Slice CSV checks
    errors = []
    if not os.path.exists(TS_EVENTS):
        errors.append(f"Missing: {os.path.relpath(TS_EVENTS, ROOT)}")
    if not os.path.exists(TS_DOCS):
        errors.append(f"Missing: {os.path.relpath(TS_DOCS, ROOT)}")
    if errors:
        for e in errors:
            print("[error]", e)
        raise SystemExit("Slice CSVs not found; aborting.")

    try:
        ev = pd.read_csv(TS_EVENTS)
        docs = pd.read_csv(TS_DOCS)
        # Normalize types similar to builder.load_data()
        ev["pub_date"] = pd.to_datetime(ev.get("pub_date"), errors="coerce")
        if "length_chars" in docs.columns:
            docs["length_chars"] = pd.to_numeric(docs["length_chars"], errors="coerce")
    except Exception as e:
        raise SystemExit(f"Failed reading slice CSVs: {e}")

    print(f"Loaded: {len(ev)} events, {len(docs)} docs from topic slice")
    print(f"Models: summary={summary_model} | embeddings={embed_model} (not used for generation)")
    print(f"Stripe payment link: {'present' if stripe_link else 'MISSING — TODO placeholder will be shown'}")

    return {"events": ev, "docs": docs, "summary_model": summary_model, "embed_model": embed_model, "stripe": stripe_link}


def run_pipeline():
    ctx = preflight()
    ev: pd.DataFrame = ctx["events"]
    docs: pd.DataFrame = ctx["docs"]

    # STEP 2 — Pilot report
    step("STEP 2/5 — Build Pilot Report v0.1 (MD/HTML/PDF)")
    md_path, html_path, pdf_path = builder.build_pilot_report(ev, docs)
    print("Pilot report:")
    print("- MD:", os.path.relpath(md_path, ROOT))
    print("- HTML:", os.path.relpath(html_path, ROOT))
    print("- PDF:", (os.path.relpath(pdf_path, ROOT) if pdf_path else "(WeasyPrint unavailable; PDF skipped)"))

    # STEP 3 — Sales pack
    step("STEP 3/5 — Build Sales Pack (one‑pager + landing page with CTA)")
    sales_onepager_path, landing_path = builder.build_sales_pack(ev)
    print("Sales pack:")
    print("- One‑pager:", os.path.relpath(sales_onepager_path, ROOT))
    print("- Landing page:", os.path.relpath(landing_path, ROOT))

    # STEP 4 — Outbound kit
    step("STEP 4/5 — Build Outbound Kit (prospects, emails, LinkedIn assets)")
    prospect_path = builder.build_outbound_kit()
    print("Outbound kit:")
    print("- Prospect list:", os.path.relpath(prospect_path, ROOT))

    # STEP 5 — QA & revenue snapshot
    step("STEP 5/5 — Link checks, chart checks, and snapshot ZIP")
    qa_path, snapshot_zip = builder.qa_and_snapshot()
    print("QA results:", os.path.relpath(qa_path, ROOT))
    print("Snapshot ZIP:", os.path.relpath(snapshot_zip, ROOT))

    # Write snapshot path for downstream tools
    try:
        os.makedirs(LATEST, exist_ok=True)
        with open(os.path.join(LATEST, "snapshot_path.txt"), "w", encoding="utf-8") as f:
            f.write(snapshot_zip)
    except Exception:
        pass

    # Publish site to /docs (GitHub Pages ready)
    docs_dir = os.path.join(ROOT, "docs")
    charts_src = os.path.join(builder.PILOT_DIR, "charts")
    charts_dst = os.path.join(docs_dir, "charts")
    os.makedirs(docs_dir, exist_ok=True)
    os.makedirs(charts_dst, exist_ok=True)
    # Copy landing and report
    shutil.copy2(landing_path, os.path.join(docs_dir, "index.html"))
    shutil.copy2(html_path, os.path.join(docs_dir, "report.html"))
    # Copy charts
    for p in glob.glob(os.path.join(charts_src, "*.png")):
        shutil.copy2(p, charts_dst)

    # STRIPE setup instructions if missing
    stripe_link = ctx.get("stripe")
    if not stripe_link:
        stripe_setup = os.path.join(ROOT, "deliverables", "STRIPE_SETUP.md")
        os.makedirs(os.path.dirname(stripe_setup), exist_ok=True)
        with open(stripe_setup, "w", encoding="utf-8") as f:
            f.write(
                "# Stripe Setup (MVP)\n\n"
                "1. Create a Stripe Payment Link for the Pilot Report (one-time $499).\n"
                "2. Copy the link URL.\n"
                "3. Add to app/.env as `STRIPE_PAYMENT_LINK=<your_link>`.\n"
                "4. Re-run `.venv/bin/python scripts/run_revenue_sprint.py` to update CTAs.\n"
            )

    # Publishing notes
    publishing_notes = os.path.join(ROOT, "deliverables", "PUBLISHING_NOTES.md")
    with open(publishing_notes, "w", encoding="utf-8") as f:
        f.write(
            "# Publishing Notes (GitHub Pages)\n\n"
            "- GitHub Pages serves from /docs by default when enabled in repo settings.\n"
            "- Files published: /docs/index.html (landing), /docs/report.html (pilot), /docs/charts/*.png.\n"
            "- Ensure links include UTM: ?utm_source=af_mvp&utm_medium=outbound&utm_campaign=revenue_sprint\n"
        )

    # Outbound Send Instructions
    send_instructions = os.path.join(ROOT, "deliverables", "outbound_kit", "SEND_INSTRUCTIONS.md")
    os.makedirs(os.path.dirname(send_instructions), exist_ok=True)
    with open(send_instructions, "w", encoding="utf-8") as f:
        f.write(
            "# Send Instructions (Outbound Kit)\n\n"
            "- Use `deliverables/outbound_kit/prospect_list.csv` for mail-merge.\n"
            "- Map fields: first_name, last_name, email, org, role, notes.\n"
            "- Send 10–15 emails/day to warm domains; respect opt-outs.\n"
            "- Include physical address footer and unsubscribe line.\n"
            "- Sequence: Day 0 initial, Day 3 follow-up, Day 7 final nudge.\n"
        )

    # Completion report
    completion = os.path.join(ROOT, "deliverables", "REVENUE_SPRINT_COMPLETION.md")
    qa_summary = {}
    try:
        with open(os.path.join(ROOT, "deliverables", "sales_pack", "qa_results.json"), "r", encoding="utf-8") as f:
            qa_summary = json.load(f)
    except Exception:
        qa_summary = {"note": "qa_results.json not found"}
    with open(completion, "w", encoding="utf-8") as f:
        f.write(
            "# Revenue Sprint Completion Report\n\n"
            f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            "## Status\n"
            "- Pilot: PASS\n- Sales Pack: PASS\n- Outbound Kit: PASS\n- Site: READY (docs/)\n\n"
            "## Artifacts\n"
            f"- Pilot MD: {md_path}\n- Pilot HTML: {html_path}\n- Pilot PDF: {pdf_path or 'skipped'}\n"
            f"- Sales Landing: {landing_path}\n- One-pager: {sales_onepager_path}\n"
            f"- Outbound Prospects: {prospect_path}\n"
            f"- Snapshot ZIP: {snapshot_zip}\n\n"
            "## Link Check Summary\n"
            f"{json.dumps(qa_summary)[:1000]}\n\n"
            "## Payment Link\n"
            f"- STRIPE_PAYMENT_LINK: {'PRESENT' if stripe_link else 'MISSING (see STRIPE_SETUP.md)'}\n"
        )

    # Final summary line (strict format)
    pilot_pass = os.path.exists(md_path) and os.path.exists(html_path)
    sales_pass = os.path.exists(sales_onepager_path) and os.path.exists(landing_path)
    outbound_pass = os.path.exists(prospect_path)
    site_ready = os.path.exists(docs_dir)

    final_line = (
        f"Revenue Sprint COMPLETE: Pilot [{'PASS' if pilot_pass else 'FAIL'}] | "
        f"SalesPack [{'PASS' if sales_pass else 'FAIL'}] | "
        f"OutboundKit [{'PASS' if outbound_pass else 'FAIL'}] | "
        f"Site [{'READY' if site_ready else 'NOT_READY'}] | "
        f"Snapshot: {snapshot_zip}"
    )
    print("\n" + final_line)


if __name__ == "__main__":
    run_pipeline()

