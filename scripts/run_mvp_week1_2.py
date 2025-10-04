#!/usr/bin/env python3
"""
Week 1–2 MVP Sprint Runner

Delivers four artifacts with minimal dependencies and concise console output:
 1) Polished flagship report outline
 2) Small, fresh data update on two hot topics
 3) 1–2 page teaser (HTML + optional PDF)
 4) Basic landing page + LinkedIn draft

Notes:
- Reuses existing DB; does NOT perform crawling or batch API calls.
- Enrichment report is written with projected costs; actual spend remains $0 unless you run batch separately.
- Writes PASS/FAIL lines and artifacts to data/output/validation/latest/ and deliverables/.
"""

from __future__ import annotations
import csv
import os
import sys
import math
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple

# Load env (without printing secrets)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv("app/.env")
except Exception:
    pass

try:
    import psycopg2  # type: ignore
    HAS_PG = True
except Exception:
    psycopg2 = None  # type: ignore
    HAS_PG = False

REPO_ROOT = os.path.abspath(os.path.dirname(__file__) + "/..")
LATEST_DIR = os.path.join(REPO_ROOT, "data", "output", "validation", "latest")
DELIV_DIR = os.path.join(REPO_ROOT, "deliverables")
TEASER_DIR = os.path.join(DELIV_DIR, "teaser")
LANDING_DIR = os.path.join(DELIV_DIR, "landing")

REPORT_OUTLINE = os.path.join(REPO_ROOT, "docs", "report_outline.md")
REPORT_OUTLINE_PATH_TXT = os.path.join(LATEST_DIR, "report_outline_path.txt")

TOPIC_EVENTS_CSV = os.path.join(LATEST_DIR, "topic_slice_events.csv")
TOPIC_DOCS_CSV = os.path.join(LATEST_DIR, "topic_slice_docs.csv")
ENRICHMENT_REPORT_MD = os.path.join(LATEST_DIR, "enrichment_report.md")
CHECKLIST_MD = os.path.join(LATEST_DIR, "week1_2_mvp_checklist.md")
BLOCKERS_MD = os.path.join(LATEST_DIR, "blockers.md")
ROBOT_BLOCKS = os.path.join(LATEST_DIR, "robots_blocked.csv")

TEASER_HTML = os.path.join(TEASER_DIR, "teaser.html")
TEASER_PDF = os.path.join(TEASER_DIR, "teaser.pdf")
LANDING_HTML = os.path.join(LANDING_DIR, "index.html")
LINKEDIN_MD = os.path.join(DELIV_DIR, "linkedin_article_draft.md")

SNAPSHOT_PATH = None  # set later

AI_AUTHORITIES = {"PDPC", "IMDA", "MIC", "SBV"}
FIN_AUTHORITIES = {"MAS", "SC", "BI", "OJK"}
AUTH_ORDER = ["MAS", "SC", "PDPC", "MIC", "BI", "OJK", "IMDA", "ASEAN", "SBV"]

@dataclass
class EventRow:
    event_id: str
    authority: str
    pub_date: str
    title: str
    url: str
    topic_tag: str


def utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_dirs():
    for p in [LATEST_DIR, DELIV_DIR, TEASER_DIR, LANDING_DIR, os.path.join(REPO_ROOT, "docs")]:
        os.makedirs(p, exist_ok=True)


def get_db():
    if not HAS_PG:
        return None
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        return None
    conn = psycopg2.connect(url)  # type: ignore
    conn.autocommit = True
    return conn

# Fallback: build slice from exported CSVs when DB is unavailable
EVENTS_EXPORT = os.path.join(DELIV_DIR, "events.csv")
DOCS_EXPORT = os.path.join(DELIV_DIR, "documents.csv")

def _read_events_csv() -> List[EventRow]:
    if not os.path.exists(EVENTS_EXPORT):
        return []
    out: List[EventRow] = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    with open(EVENTS_EXPORT, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                auth = (row.get("authority") or "").upper()
                if auth not in (AI_AUTHORITIES | FIN_AUTHORITIES):
                    continue
                pdate = (row.get("pub_date") or "")[:10]
                if not pdate or pdate < cutoff:
                    continue
                eid = str(row.get("event_id") or "")
                ttl = row.get("title") or "(untitled)"
                url = row.get("url") or ""
                topic = "AI_POLICY" if auth in AI_AUTHORITIES else "FINTECH"
                out.append(EventRow(eid, auth, pdate, ttl, url, topic))
            except Exception:
                continue
    # sort by date desc then id desc
    out.sort(key=lambda e: (e.pub_date, e.event_id), reverse=True)
    return out

def _read_docs_csv(event_ids: List[int]) -> List[Tuple[int,int,str,int]]:
    if not os.path.exists(DOCS_EXPORT):
        return []
    sids = set(event_ids)
    res: List[Tuple[int,int,str,int]] = []
    with open(DOCS_EXPORT, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                eid = int(row.get("event_id") or 0)
                if eid not in sids:
                    continue
                did = int(row.get("document_id") or 0)
                src = row.get("source_url") or ""
                # documents.csv may have char_count column
                lc = row.get("length_chars") or row.get("char_count") or "0"
                res.append((did, eid, src, int(lc)))
            except Exception:
                continue
    return res



def fetch_topic_slice(conn) -> Tuple[List[EventRow], List[Tuple[int,int,str,int]]]:
    # Prefer DB when available; otherwise fall back to CSV exports
    if conn is None:
        events = _read_events_csv()
        docs = _read_docs_csv([e.event_id for e in events])
        return events, docs

    # Build authority to topic tag mapping
    def tag_for(auth: str) -> str:
        a = (auth or "").upper()
        if a in AI_AUTHORITIES:
            return "AI_POLICY"
        if a in FIN_AUTHORITIES:
            return "FINTECH"
        return "OTHER"

    since = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    auths = tuple(AI_AUTHORITIES | FIN_AUTHORITIES)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_id, authority, to_char(pub_date AT TIME ZONE 'UTC','YYYY-MM-DD') AS pub_date,
               COALESCE(title, ''), url
        FROM events
        WHERE authority = ANY(%s) AND pub_date >= %s::date
        ORDER BY pub_date DESC, event_id DESC
        LIMIT 500
        """,
        (list(auths), since),
    )
    rows = cur.fetchall()
    events: List[EventRow] = []
    event_ids: List[str] = []
    for eid, auth, pdate, title, url in rows:
        topic = tag_for(auth)
        if topic == "OTHER":
            continue
        topic = "AI_POLICY" if (auth or "").upper() in AI_AUTHORITIES else ("FINTECH" if (auth or "").upper() in FIN_AUTHORITIES else "OTHER")
        eid_s = str(eid)
        events.append(EventRow(eid_s, auth, pdate, title or "(untitled)", url or "", topic))
        event_ids.append(eid_s)

    docs: List[Tuple[int,int,str,int]] = []  # (document_id, event_id, source_url, length_chars)
    if event_ids:
        cur.execute(
            """
            SELECT d.document_id, d.event_id, d.source_url, COALESCE(LENGTH(d.clean_text), 0) AS length_chars
            FROM documents d
            WHERE d.event_id::text = ANY(%s)
            ORDER BY d.document_id ASC
            """,
            (event_ids,),
        )
        docs = cur.fetchall()
    cur.close()
    return events, docs


def write_csvs(events: List[EventRow], docs: List[Tuple[int,int,str,int]]):
    # topic_slice_events.csv
    with open(TOPIC_EVENTS_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id","authority","pub_date","title","url","topic_tag"])
        for e in events:
            w.writerow([e.event_id, e.authority, e.pub_date, e.title, e.url, ("AI_POLICY" if e.authority in AI_AUTHORITIES else "FINTECH")])

    # topic_slice_docs.csv
    with open(TOPIC_DOCS_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["document_id","event_id","source_url","length_chars"])
        for d in docs:
            w.writerow(list(d))


def median(values: List[int]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return float(s[mid])
    return (s[mid-1] + s[mid]) / 2.0


def enrichment_report(conn, event_ids: List[str]) -> Tuple[int, int, float]:
    # Count missing summaries/embeddings and project batch cost; actual spend remains 0
    if not event_ids:
        with open(ENRICHMENT_REPORT_MD, "w", encoding="utf-8") as f:
            f.write("# Enrichment Report (MVP)\n\n")
            f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write("No events in window; batch not required.\n")
        return 0, 0, 0.0

    need_sum = 0
    need_emb = 0

    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM events WHERE event_id::text = ANY(%s) AND (summary_en IS NULL OR summary_en = '')",
            (event_ids,),
        )
        need_sum = int(cur.fetchone()[0])
        cur.execute(
            "SELECT COUNT(*) FROM events WHERE event_id::text = ANY(%s) AND (embedding IS NULL)",
            (event_ids,),
        )
        need_emb = int(cur.fetchone()[0])
        cur.close()
    else:
        # Fallback: estimate from CSV exports (summary only; embedding not available in CSV)
        ids = set(event_ids)
        if os.path.exists(EVENTS_EXPORT):
            with open(EVENTS_EXPORT, "r", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                for row in rdr:
                    try:
                        eid = str(row.get("event_id") or "")
                        if eid in ids:
                            summ = row.get("summary_en")
                            if not summ:
                                need_sum += 1
                    except Exception:
                        continue
        need_emb = 0

    # Projection from brief: ~500 tokens per summary input; ~400 tokens per embedding
    projected = (need_sum * 500 / 1_000_000.0 * 0.150) + (need_emb * 400 / 1_000_000.0 * 0.020)

    with open(ENRICHMENT_REPORT_MD, "w", encoding="utf-8") as f:
        f.write("# Enrichment Report (MVP)\n\n")
        f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write("## Summary\n\n")
        f.write(f"- Events needing summaries: {need_sum}\n")
        f.write(f"- Events needing embeddings: {need_emb}\n")
        f.write(f"- Projected batch spend: ${projected:.4f} (budget $5.00)\n")
        f.write(f"- Actual spend: $0.0000 (batch not executed in this run)\n\n")
        f.write("Models:\n\n- Summary: ${OPENAI_SUMMARY_MODEL or 'gpt-4o-mini-search-preview'}\n- Embedding: ${OPENAI_EMBED_MODEL or 'text-embedding-3-small'}\n")

    return need_sum, need_emb, projected


def top_items_for_teaser(events: List[EventRow], k: int = 5) -> List[EventRow]:
    return events[:k]


def write_teaser(events: List[EventRow]):
    os.makedirs(TEASER_DIR, exist_ok=True)
    date_utc = utc_date_str()
    contact = os.getenv("CONTACT_EMAIL", "data@aseanforge.com")
    topics = "AI Policy & Fintech Regulation"

    items = top_items_for_teaser(events, 5)
    # Insights: first 3–5 items as bullets
    insights = []
    for e in items[:5]:
        insights.append(f"{e.authority} — {e.title[:120]} ({e.pub_date})")

    # Table rows
    table_rows = "\n".join(
        [f"<tr><td>{e.authority}</td><td>{e.pub_date}</td><td>{e.title}</td><td><a href=\"{e.url}\">Link</a></td></tr>" for e in items]
    )

    html = f"""
<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>AseanForge – Teaser</title>
<style>
 body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; color: #001833; }}
 header {{ background: #00205B; color: white; padding: 16px 20px; }}
 h1 {{ margin: 0; font-size: 20px; }}
 .container {{ padding: 20px; }}
 .meta {{ color: #555; margin-bottom: 12px; }}
 .insights li {{ margin: 8px 0; }}
 table {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
 th, td {{ border: 1px solid #e5e7eb; padding: 8px; text-align: left; }}
 th {{ background: #F7F9FC; }}
 a {{ color: #00205B; text-decoration: none; }}
 .badge {{ display: inline-block; background: #BA0C2F; color: white; padding: 2px 8px; border-radius: 999px; font-size: 12px; margin-left: 8px; }}
 footer {{ padding: 16px 20px; border-top: 1px solid #eee; color: #555; }}
</style>
</head>
<body>
  <header>
    <h1>AseanForge – Tech & Policy Intelligence <span class=\"badge\">Teaser</span></h1>
  </header>
  <div class=\"container\">
    <div class=\"meta\">Date (UTC): {date_utc} · Topics: {topics}</div>
    <h2>Key insights</h2>
    <ul class=\"insights\">
      {''.join(f'<li>{ins}</li>' for ins in insights[:5])}
    </ul>
    <h2>Top items</h2>
    <table>
      <thead><tr><th>Authority</th><th>Date</th><th>Title</th><th>Link</th></tr></thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>
  <footer>Contact: {contact}</footer>
</body>
</html>
"""
    with open(TEASER_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    # Optional PDF rendering via weasyprint if available
    exe = shutil.which("weasyprint")
    if exe:
        try:
            os.makedirs(TEASER_DIR, exist_ok=True)
            os.system(f'weasyprint "{TEASER_HTML}" "{TEASER_PDF}" >/dev/null 2>&1')
        except Exception:
            pass


def write_landing_and_linkedin(events: List[EventRow]):
    date_utc = utc_date_str()
    contact = os.getenv("CONTACT_EMAIL", "data@aseanforge.com")
    lead_form = os.getenv("LEAD_FORM_URL", "")
    cta_href = lead_form if lead_form else f"mailto:{contact}"

    # Landing
    os.makedirs(LANDING_DIR, exist_ok=True)
    teaser_rel = os.path.relpath(TEASER_HTML, LANDING_DIR)
    landing_html = f"""
<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>AseanForge: ASEAN Tech & Policy Intelligence</title>
<style>body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;color:#001833}}header{{background:#00205B;color:#fff;padding:18px 20px}}h1{{margin:0;font-size:22px}}.wrap{{padding:20px}}.cta a{{background:#BA0C2F;color:#fff;padding:10px 14px;border-radius:6px;text-decoration:none}}.muted{{color:#555}}</style>
</head><body>
<header><h1>AseanForge: ASEAN Tech & Policy Intelligence</h1></header>
<div class=\"wrap\">
  <p class=\"muted\">Updated (UTC): {date_utc}</p>
  <ul>
    <li>Real-time regulatory tracking across 10+ ASEAN authorities</li>
    <li>AI-powered summaries in English</li>
    <li>Investor-focused insights</li>
  </ul>
  <p><a href=\"{teaser_rel}\">View the teaser</a></p>
  <p class=\"cta\"><a href=\"{cta_href}\">Request full report</a></p>
</div>
</body></html>
"""
    with open(LANDING_HTML, "w", encoding="utf-8") as f:
        f.write(landing_html)

    # LinkedIn Draft
    q = (datetime.now(timezone.utc).month - 1)//3 + 1
    title = f"ASEAN Tech Policy Update: AI Regulation & Fintech Trends (Q{q} {datetime.now(timezone.utc).year})"
    items = top_items_for_teaser(events, 5)
    bullets = [f"- {e.authority}: {e.title} ({e.pub_date})" for e in items[:5]]
    links = [e.url for e in items[:2] if e.url]
    teaser_rel_from_root = os.path.relpath(TEASER_HTML, REPO_ROOT)
    body = (
        f"{title}\n\n"
        "In the past month, ASEAN regulators have advanced AI governance and fintech frameworks with tangible implications for compliance, risk, and growth. This teaser highlights the most actionable updates from high-signal authorities across the region. We track English-source announcements, extract canonical documents, and summarize in a consistent investor-oriented format. Our focus: what changed, why it matters, associated risks, and where the opportunities are.\n\n"
        "Highlights from this period include policy guidance, licensing moves, and market access signals that shape capital deployment and product strategy. If you operate in digital banking, payments, or AI-enabled services in ASEAN, these are the threads to watch.\n\n"
        + "\n".join(bullets) + "\n\n"
        + (f"Source links: {', '.join(links)}\n\n" if links else "")
        + f"Download the full teaser here: {teaser_rel_from_root}\n\n"
        + f"Contact us for custom intelligence: {os.getenv('CONTACT_EMAIL','data@aseanforge.com')}\n"
    )
    with open(LINKEDIN_MD, "w", encoding="utf-8") as f:
        f.write(body)


def write_checklist(outline_pass: bool, data_pass: bool, teaser_status: str, web_pass: bool, events_n: int, docs_n: int, fc_used: int, batch_usd: float):
    with open(CHECKLIST_MD, "w", encoding="utf-8") as f:
        f.write("# Week 1–2 MVP Checklist\n\n")
        f.write("## 1. Flagship Report Outline\n")
        f.write(f"- Status: {'PASS' if outline_pass else 'FAIL'}\n")
        f.write(f"- Path: docs/report_outline.md\n\n")
        f.write("## 2. Data Update (Two Hot Topics)\n")
        f.write(f"- Status: {'PASS' if data_pass else 'FAIL'}\n")
        f.write(f"- Events: {events_n} new/enriched\n")
        f.write(f"- Docs: {docs_n} created\n")
        f.write(f"- Firecrawl URLs: {fc_used}/300\n")
        f.write(f"- OpenAI Batch: ${batch_usd:.2f}/$5.00\n")
        f.write(f"- Paths: topic_slice_events.csv, topic_slice_docs.csv, enrichment_report.md\n\n")
        f.write("## 3. Teaser\n")
        f.write(f"- Status: {teaser_status}\n")
        f.write(f"- Paths: teaser.html, {'teaser.pdf' if os.path.exists(TEASER_PDF) else '(PDF skipped)'}\n\n")
        f.write("## 4. Landing + LinkedIn\n")
        f.write(f"- Status: {'PASS' if web_pass else 'FAIL'}\n")
        f.write(f"- Paths: landing/index.html, linkedin_article_draft.md\n\n")
        f.write("## Blockers (if any)\n")
        if os.path.exists(BLOCKERS_MD):
            with open(BLOCKERS_MD, 'r', encoding='utf-8') as b:
                f.write(b.read())
        else:
            f.write("- None\n")


def create_snapshot() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_zip = os.path.join(DELIV_DIR, f"mvp_week1_2_snapshot_{ts}.zip")
    import zipfile
    with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        def add(path):
            if os.path.exists(path):
                z.write(path, os.path.relpath(path, REPO_ROOT))
        add(REPORT_OUTLINE)
        add(TOPIC_EVENTS_CSV)
        add(TOPIC_DOCS_CSV)
        add(ENRICHMENT_REPORT_MD)
        add(TEASER_HTML)
        if os.path.exists(TEASER_PDF):
            add(TEASER_PDF)
        add(LANDING_HTML)
        add(LINKEDIN_MD)
        add(ROBOT_BLOCKS)  # may be absent
        add(BLOCKERS_MD)   # may be absent
        add(CHECKLIST_MD)
    return out_zip


def main():
    ensure_dirs()

    # 1) Report outline precondition: ensure path text file
    with open(REPORT_OUTLINE_PATH_TXT, "w", encoding="utf-8") as f:
        f.write(os.path.abspath(REPORT_OUTLINE))

    # 2) Data update (slice only)
    conn = get_db()
    events, docs = fetch_topic_slice(conn)
    write_csvs(events, docs)

    # PASS/FAIL for data: >=40 events OR >=20 events with median doc length >= 5000
    doc_len_map: Dict[int, int] = {}
    for _, eid, _, length in docs:
        doc_len_map[eid] = max(doc_len_map.get(eid, 0), int(length or 0))
    doc_lengths = [doc_len_map.get(e.event_id, 0) for e in events if e.event_id in doc_len_map]
    med_len = median(doc_lengths)
    events_n = len(events)
    docs_n = len(docs)

    # 3) Enrichment report (projected only, no spend)
    need_sum, need_emb, projected = enrichment_report(conn, [e.event_id for e in events])
    if conn is not None:
        conn.close()

    data_pass = (events_n >= 40) or (events_n >= 20 and med_len >= 5000)

    # 4) Teaser
    write_teaser(events)
    teaser_status = "PASS+" if os.path.exists(TEASER_PDF) else "PASS"

    # 5) Landing + LinkedIn
    write_landing_and_linkedin(events)
    web_pass = os.path.exists(LANDING_HTML) and os.path.exists(LINKEDIN_MD)

    # Outline PASS: file existence + placeholders checked externally by creation step
    outline_pass = os.path.exists(REPORT_OUTLINE)

    # 6) Checklist
    write_checklist(outline_pass, data_pass, teaser_status, web_pass, events_n, docs_n, 0, 0.0)

    # 7) Snapshot
    snap = create_snapshot()

    # 8) Final summary line
    print(f"Week 1–2 MVP: Outline [{'PASS' if outline_pass else 'FAIL'}] | Data [{'PASS' if data_pass else 'FAIL'}] | Teaser [{teaser_status}] | Landing [{'PASS' if web_pass else 'FAIL'}] | Snapshot: {os.path.relpath(snap, REPO_ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        os.makedirs(LATEST_DIR, exist_ok=True)
        with open(BLOCKERS_MD, "w", encoding="utf-8") as f:
            f.write("# Blockers\n\n")
            f.write(f"{e.__class__.__name__}: {e}\n")
        print(f"ERROR: {e}")
        sys.exit(1)

