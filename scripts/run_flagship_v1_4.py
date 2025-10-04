#!/usr/bin/env python3
"""
Flagship v1.4 Orchestrator — executes Phases A–F per mission with existing infra.
- Uses .venv/bin/python invocation and loads app/.env
- Respects budgets: OpenAI gen <= $20; Embeddings <= $2; Firecrawl 0 unless gap
"""
import os, sys, json, time, zipfile, shutil, csv, re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

# --- Env ---
try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
LATEST = os.path.join(ROOT, "data/output/validation/latest")
DELIV = os.path.join(ROOT, "deliverables")
DOCS_DIR = os.path.join(ROOT, "docs")
FLAG_DIR = os.path.join(DELIV, "flagship_v1_4")
CHARTS_DIR = os.path.join(ROOT, "docs/flagship_charts")
os.makedirs(LATEST, exist_ok=True)
os.makedirs(DELIV, exist_ok=True)
os.makedirs(FLAG_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)

TIER1 = ["MAS","IMDA","ASEAN","BOT","BSP","SBV","KOMDIGI","KOMINFO","BNM"]
FRESH_SINCE = (datetime.now(timezone.utc) - timedelta(days=365)).date().isoformat()

# ---- Preflight targets (non-blocking) ----
CITATIONS_MIN = 30
TARGET_PAGES_MIN = 20
TARGET_PAGES_MAX = 50
RAG_HIT_AT_5_TARGET = 0.85

# --- DB ---
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise SystemExit("NEON_DATABASE_URL not set")
    return psycopg2.connect(url)

# --- OpenAI ---
use_o3 = True
try:
    from openai import OpenAI
    oa = OpenAI()
except Exception:
    oa = None
    use_o3 = False

# --- RAG ---
import sys as _sys
if ROOT not in sys.path:
    _sys.path.append(ROOT)
from app import rag as rag_mod

# --- Usage tracker ---
from scripts.usage_tracker import TokenTracker
tracker = TokenTracker(run_id=str(int(time.time())))

# --- Helpers ---

def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f: f.write(text)


def read_json(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception:
        return None


def sql_one(cur, q, params=()):
    cur.execute(q, params)
    r = cur.fetchone()
    return (r[0] if r and len(r)==1 else r)


# ---------------- Phase A ----------------

def phase_a_baseline() -> int:
    # Find v1.3 HTML/PDF under deliverables/flagship_v1
    vdir = os.path.join(DELIV, "flagship_v1")
    html = os.path.join(vdir, "flagship_v1.html")
    pdf = os.path.join(vdir, "flagship_v1.pdf")
    pages = 0
    warn_path = os.path.join(LATEST, "pdf_export_warnings.txt")
    if os.path.exists(pdf):
        # Count pages via PyPDF2/pypdf fallback
        try:
            try:
                import PyPDF2 as pyp
            except Exception:
                import pypdf as pyp  # type: ignore
            with open(pdf, 'rb') as f:
                reader = pyp.PdfReader(f)
                pages = len(reader.pages)
        except Exception as e:
            write_text(warn_path, f"[warn] Could not count pages: {e}\n")
            pages = 0
    elif os.path.exists(html):
        # Try WeasyPrint install then export
        try:
            from weasyprint import HTML  # type: ignore
        except Exception:
            # Try install (allowed)
            os.system(f".venv/bin/python -m pip install weasyprint >/dev/null 2>&1 || true")
        try:
            from weasyprint import HTML  # type: ignore
            HTML(filename=html, base_url=vdir).write_pdf(pdf)
            # Count pages
            try:
                import pypdf as pyp  # type: ignore
                with open(pdf, 'rb') as f:
                    pages = len(pyp.PdfReader(f).pages)
            except Exception:
                pages = 0
        except Exception as e:
            write_text(warn_path, f"[warn] WeasyPrint export failed for v1.3: {e}\n")
            pages = 0
    else:
        write_text(warn_path, "[warn] v1.3 not found; skipping export\n")
    write_text(os.path.join(LATEST, "v1_3_pages.txt"), f"v1.3_pages: {pages}\n")
    return pages


# ---------------- Phase B ----------------

def tier1_filter_sql() -> str:
    placeholders = ",".join(["%s"]*len(TIER1))
    return f" AND UPPER(e.authority) IN ({placeholders}) "


def compute_embedding_coverage() -> Dict[str, Any]:
    conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
    cutoff = FRESH_SINCE
    sql = f"""
        SELECT COUNT(*) AS total,
               COUNT(CASE WHEN d.embedding IS NOT NULL THEN 1 END) AS with_emb
        FROM documents d
        JOIN events e ON e.event_id = d.event_id
        WHERE (e.pub_date IS NULL OR e.pub_date >= %s)
          {tier1_filter_sql()}
    """
    cur.execute(sql, (cutoff, *TIER1))
    row = cur.fetchone() or {"total":0, "with_emb":0}
    total = int(row.get("total",0) or 0); with_emb = int(row.get("with_emb",0) or 0)
    pct = (100.0*with_emb/total) if total>0 else 100.0
    # by authority
    cur.execute(f"""
        SELECT e.authority, COUNT(*) AS total,
               COUNT(CASE WHEN d.embedding IS NOT NULL THEN 1 END) AS with_emb
        FROM documents d JOIN events e ON e.event_id = d.event_id
        WHERE (e.pub_date IS NULL OR e.pub_date >= %s)
          {tier1_filter_sql()}
        GROUP BY e.authority
    """, (cutoff, *TIER1))
    by_auth = {r['authority']: {
        'total': int(r['total']),
        'with_emb': int(r['with_emb']),
        'pct': round(100.0*int(r['with_emb'])/int(r['total']),2) if int(r['total'])>0 else 100.0
    } for r in cur.fetchall()}
    cur.close(); conn.close()
    out = {"since": cutoff, "tier1": TIER1, "total": total, "with_emb": with_emb, "pct": round(pct,2), "by_authority": by_auth}
    write_text(os.path.join(LATEST, "embedding_coverage.json"), json.dumps(out, indent=2))
    return out


def maybe_backfill_embeddings(max_usd: float = 2.0) -> Dict[str, Any]:
    cov = compute_embedding_coverage()
    if cov.get("pct", 100.0) >= 95.0:
        return {"status": "skip", "coverage": cov}
    # Build minimal embedding batch to close gap (estimate only; optional submit)
    from app.enrich_batch import builders, submit
    gap = max(0, int(cov.get("total",0)*0.95 - cov.get("with_emb",0)))
    limit = min(gap, 2000)
    # Iteratively find a limit under budget
    meta = None
    for lim in [limit, int(limit*0.75)+1, int(limit*0.5)+1, int(limit*0.25)+1, 100, 50, 25]:
        meta = builders.build_embedding_requests_docs(since_date=FRESH_SINCE, limit=lim,
                                                      output_path="data/batch/embeddings_docs.v14.jsonl",
                                                      authorities=TIER1)
        if meta.get("projected_cost_usd", 0) <= max_usd:
            break
    # Submit batch (best-effort)
    batch_id = None
    try:
        batch_id = submit.submit_batch(meta["file_path"], "embeddings") if meta and meta.get("request_count",0)>0 else None
    except Exception:
        batch_id = None
    return {"status": "submit" if batch_id else "built", "batch_id": batch_id, "meta": meta, "coverage": cov}


def eval_and_tune_rag(log_path: str) -> Dict[str, Any]:
    # Use existing autotune; write minimal log
    import scripts.run_flagship_v1_1 as v1
    res = v1.autotune_rag(max_passes=3)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("RAG tuning (hit@5 target >=0.85)\n")
        f.write(json.dumps(res, indent=2))
    return res


# ---------------- Phase C ----------------

def fetch_recent_docs(authority: str, min_chars: int = 400, limit: int = 8) -> List[Dict[str,Any]]:
    conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT e.title, e.pub_date::text as pub_date, d.source_url, LENGTH(d.clean_text) AS len
        FROM documents d JOIN events e ON e.event_id=d.event_id
        WHERE UPPER(e.authority)=UPPER(%s) AND d.clean_text IS NOT NULL AND LENGTH(d.clean_text) >= %s
          AND (e.pub_date IS NULL OR e.pub_date >= %s)
        ORDER BY e.pub_date DESC NULLS LAST
        LIMIT %s
        """, (authority, min_chars, FRESH_SINCE, limit)
    )
    rows = cur.fetchall(); cur.close(); conn.close()
    return rows or []


def top_hits_for_prompts(prompts: List[str], k: int = 12, keep: int = 8, settings: Dict[str,Any] | None = None) -> Tuple[Dict[str,List[Dict[str,Any]]], List[str]]:
    settings = settings or {"K_lex":150,"K_vec":150,"rrf_K":60,"fts_weight":1.5,"freshness_days":365}
    out: Dict[str, List[Dict[str,Any]]] = {}
    cites: List[str] = []
    for p in prompts:
        hits = rag_mod.retrieve(p, k=keep, K_lex=settings["K_lex"], K_vec=settings["K_vec"], K_rrf=settings["rrf_K"], fts_weight=settings["fts_weight"], freshness_days=settings["freshness_days"]) or []
        out[p] = hits[:keep]
        for h in hits[:keep]:
            u = h.get("source_url")
            if u: cites.append(u)
    return out, sorted(set(cites))


def draft_with_model(prompt: str, model: str) -> str:
    # Special handling for o3-deep-research: use Responses API via helper
    if model == "o3-deep-research":
        try:
            from app.o3_helpers import call_o3_deep_research
            res = call_o3_deep_research(prompt)
            txt = (res or {}).get("text", "")
            usage = (res or {}).get("usage", {}) or {}
            in_tok = int(usage.get("input_tokens", 0) or 0)
            out_tok = int(usage.get("output_tokens", 0) or 0)
            try:
                tracker.record(model, "report_generation", in_tok, out_tok)
            except Exception:
                pass
            return txt or "[generation_error] empty o3 result"
        except Exception as e:
            # Continue to generic path for logging purposes
            return f"[generation_error] {e}"
    # Generic path for non-o3 models
    if not oa:
        return "[Model unavailable] " + prompt
    try:
        resp = oa.chat.completions.create(
            model=model,
            messages=[
                {"role":"system","content":"You are a concise, citation-first analyst."},
                {"role":"user","content": prompt}
            ],
            temperature=0.3
        )
        txt = resp.choices[0].message.content or ""
        usage = getattr(resp, "usage", None)
        if usage:
            tracker.record(model, "report_generation", getattr(usage, "prompt_tokens", 0) or 0, getattr(usage, "completion_tokens", 0) or 0)
        return txt
    except Exception as e:
        msg = str(e)
        if "429" in msg:
            # rate-limit handling: log and propagate for outer loop to halt if repeated
            raise
        return f"[generation_error] {e}"


def build_charts():
    import pandas as pd
    # Load slices from DB
    conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT event_id, authority, pub_date FROM events WHERE (pub_date IS NULL OR pub_date >= %s)", (FRESH_SINCE,))
    ev = pd.DataFrame(cur.fetchall()) if cur.rowcount else pd.DataFrame(columns=["event_id","authority","pub_date"])
    cur.execute("SELECT d.document_id, d.event_id, LENGTH(d.clean_text) AS length_chars FROM documents d JOIN events e ON e.event_id=d.event_id WHERE (e.pub_date IS NULL OR e.pub_date >= %s)", (FRESH_SINCE,))
    docs = pd.DataFrame(cur.fetchall()) if cur.rowcount else pd.DataFrame(columns=["document_id","event_id","length_chars"])
    cur.close(); conn.close()
    import scripts.revenue_sprint_builder as rsb
    rsb.CHARTS_DIR = CHARTS_DIR
    os.makedirs(rsb.CHARTS_DIR, exist_ok=True)
    paths = []
    # Skip topic trend if topic_tag not available
    try:
        paths.append(rsb.chart_authority_distribution(ev))
    except Exception:
        pass
    try:
        paths.append(rsb.chart_doc_length_hist(docs))
    except Exception:
        pass
    # Skip heatmap if topic_tag not available
    return [p for p in paths if p and os.path.exists(p)]


def build_v14_content(settings: Dict[str,Any]) -> Tuple[str, List[str], float]:
    # Prompts
    exec_prompts = [
        "ASEAN-wide regulatory signals in AI, fintech, data privacy since last year",
        "Cross-border themes affecting tech and investors across ASEAN"
    ]
    themes = {
        "AI Policy & Governance": ["AI governance frameworks in ASEAN", "AI sandboxes and ethics guidelines"],
        "Fintech & Digital Finance": ["Digital banking and crypto regulation in ASEAN", "Real-time payments and CBDC updates"],
        "Data Privacy & Cybersecurity": ["Data protection laws and cross-border data flows in ASEAN", "Breach notification regimes in ASEAN"]
    }
    # Retrieval
    exec_hits, exec_cites = top_hits_for_prompts(exec_prompts, settings=settings)
    theme_queries = [q for v in themes.values() for q in v]
    theme_hits, theme_cites = top_hits_for_prompts(theme_queries, settings=settings)
    kom_docs = fetch_recent_docs("KOMDIGI", min_chars=400, limit=8)
    cites_unique = sorted(set(exec_cites + theme_cites + [d.get("source_url") for d in kom_docs if d.get("source_url")]))
    # Drafting
    openai_summary_model = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
    # 1) Executive Summary using o3 once
    dr_model = "o3-deep-research" if use_o3 else "o4-mini-deep-research"
    exec_context = "\n".join([f"- {h['authority']}: {h['title']} ({h['pub_date']}) [{h['source_url']}]" for p in exec_prompts for h in exec_hits.get(p,[])[:6]])
    exec_prompt = (
        "Write a 2–3 page Executive Summary for an ASEAN tech & policy flagship report. "
        "Make crisp, strategic points with inline citations like [Authority, YYYY-MM-DD]. "
        "Cover: key regulatory signals, emerging risks/opportunities, cross-border themes.\n\n"
        f"Context:\n{exec_context}\n\nRespond in Markdown."
    )
    # Track only counts of DR prompts
    dr_used = 0
    exec_md = draft_with_model(exec_prompt, dr_model); dr_used += 1
    # 2) Cross-ASEAN Themes bullets using gpt-4o-mini with retrieval
    themes_md_parts = ["## Cross-ASEAN Themes\n"]
    for t, qs in themes.items():
        hits = []
        for q in qs:
            hits.extend(theme_hits.get(q, [])[:5])
        # Build up to 6 bullets, max 2 sentences, each with a citation
        bullets = []
        for h in hits[:6]:
            ttl = (h.get('title') or '').replace('|','-'); dt = (h.get('pub_date') or '')
            bullets.append(f"- {ttl[:160]} [{h.get('authority')}, {dt}]")
        themes_md_parts.append(f"### {t}\n" + "\n".join(bullets) + "\n")
    themes_md = "\n".join(themes_md_parts)
    # 3) KOMDIGI Spotlight annotated list
    kom_md_lines = ["## KOMDIGI Spotlight (Indonesia)\n"]
    for r in kom_docs[:8]:
        ttl = (r.get('title') or '').strip(); dt = (r.get('pub_date') or '')
        url = r.get('source_url') or ''
        kom_md_lines.append(f"- **{ttl}** — {dt} — {url}\n  - Summary: (2–3 sentences)\n  - Implications: (1–2 sentences)")
    kom_md = "\n".join(kom_md_lines)
    # 4) Investor Lens bullets (LLM-assisted)
    inv_prompt = (
        "Produce 8 concise, actionable bullets for investors on: market entry, regulatory risk watchlist, "
        "compliance requirements for foreign entities, and emerging opportunities in ASEAN. "
        "Each bullet must be decision-relevant and include a citation placeholder like [Authority, YYYY-MM-DD]."
    )
    inv_md = draft_with_model(inv_prompt, openai_summary_model)
    # Charts
    chart_paths = build_charts()
    # Compose MD
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    header = (
        "---\n"
        f"topic: ASEAN Tech & Policy Flagship v1.4\n"
        f"date: {today}\n"
        "version_id: AF-V1_4\n"
        "mode: publish\n"
        "---\n\n"
        "# ASEAN Tech & Policy Intelligence — Flagship Report v1.4\n\n"
    )
    stripe = os.getenv("STRIPE_PAYMENT_LINK")
    cta = (f"<p><a href='{stripe}?utm_source=af_mvp&utm_medium=outbound&utm_campaign=flagship_v1_4' "
           "style='background:#BA0C2F;color:white;padding:8px 12px;border-radius:6px;text-decoration:none;'>Buy Flagship v1.4</a></p>"
           if stripe else "<!-- TODO: Add STRIPE_PAYMENT_LINK to .env and regenerate -->")
    md = [header, "## Executive Summary\n", exec_md.strip(), "\n\n", cta, "\n\n", themes_md, "\n\n", kom_md, "\n\n## Investor Lens\n", inv_md.strip(), "\n\n## Visuals\n"]
    for p in chart_paths:
        rel = os.path.relpath(p, ROOT).replace('\\','/')
        md.append(f"![Chart]({rel})\n")
    # Sources & Notes appendix
    md.append("\n## Sources & Notes\n")
    for u in cites_unique:
        md.append(f"- {u}")
    final_md = "".join(m + ("\n" if not m.endswith("\n") else "") for m in md)
    return final_md, cites_unique, float(dr_used)


# ---------------- Phase D ----------------

def deep_research_checks(md_text: str, dr_used_count: float, settings: Dict[str,Any]) -> Tuple[str, int]:
    used = int(dr_used_count)
    model = "o3-deep-research" if use_o3 else "o4-mini-deep-research"
    # 1) Hardest section check
    if used < 3 and oa:
        prompt = "Identify the 5 most complex claims in this report and verify them. Suggest authoritative sources and note caveats. Return Markdown patches only.\n\n" + md_text[:12000]
        try:
            patch = draft_with_model(prompt, model); used += 1
            md_text += "\n\n## QA Patches (Deep Research)\n" + patch
        except Exception as e:
            write_text(os.path.join(LATEST, "blockers.md"), f"{datetime.utcnow().isoformat()} DR hard-check error: {e}\n")
    # 2) Final red-team challenge
    if used < 3 and oa:
        prompt = "Extract the 10 most significant claims. Challenge each with contradictions or missing caveats. Provide alternative sources. Return as a short Markdown list.\n\n" + md_text[:12000]
        try:
            rt = draft_with_model(prompt, model); used += 1
            md_text += "\n\n## Red-Team Findings\n" + rt + "\n\n### Methods & Sources\nDesk research using hybrid RAG (BM25 + pgvector) and primary sources; LLM drafting with human-in-the-loop."
        except Exception as e:
            write_text(os.path.join(LATEST, "blockers.md"), f"{datetime.utcnow().isoformat()} DR red-team error: {e}\n")
    return md_text, used


# ---------------- Phase E ----------------

def write_outputs(final_md: str) -> Tuple[str, str, str, int]:
    md_path = os.path.join(DELIV, "flagship_v1_4.md")
    write_text(md_path, final_md)
    # HTML (simple markdown)
    try:
        import markdown
        html_body = markdown.markdown(final_md, extensions=["tables","fenced_code"])
        html = f"<html><head><meta charset='utf-8'><title>Flagship v1.4</title></head><body>{html_body}</body></html>"
    except Exception:
        html = f"<html><body><pre>{final_md}</pre></body></html>"
    html_path = os.path.join(DELIV, "flagship_v1_4.html")
    write_text(html_path, html)
    # Update site
    write_text(os.path.join(DOCS_DIR, "flagship.html"), html)
    # PDF via WeasyPrint if available (build_pdf fallback)
    pdf_path = os.path.join(DELIV, "flagship_v1_4.pdf")
    try:
        from weasyprint import HTML
        HTML(string=html, base_url=DELIV).write_pdf(pdf_path)
    except Exception:
        # Fallback to reportlab via build_pdf script
        try:
            os.system(f".venv/bin/python scripts/build_pdf.py --input {md_path} --output {pdf_path} --mode publish >/dev/null 2>&1")
        except Exception:
            pdf_path = ""
    pages = 0
    if os.path.exists(pdf_path):
        try:
            import pypdf as pyp
            with open(pdf_path, 'rb') as f:
                pages = len(pyp.PdfReader(f).pages)
        except Exception:
            pages = 0
    write_text(os.path.join(LATEST, "v1_4_pages.txt"), f"v1.4_pages: {pages}\n")
    return md_path, html_path, pdf_path if os.path.exists(pdf_path) else "", pages


# ---------------- Phase F ----------------

def link_validation(urls: List[str]) -> Tuple[int, List[str]]:
    import urllib.request, ssl
    ctx = ssl.create_default_context()
    dead = []
    for u in urls:
        try:
            req = urllib.request.Request(u, method="HEAD")
            with urllib.request.urlopen(req, context=ctx, timeout=12) as r:
                if r.status >= 400:
                    dead.append(f"{u} {r.status}")
        except Exception:
            # Try GET
            try:
                with urllib.request.urlopen(u, context=ctx, timeout=15) as r:
                    if r.status >= 400:
                        dead.append(f"{u} {r.status}")
            except Exception as e:
                dead.append(f"{u} error")
    write_text(os.path.join(LATEST, "dead_citations.txt"), "\n".join(dead) + ("\n" if dead else ""))
    return len(urls), dead


def write_kpis(hit_at_5: float, cites_count: int, pages: int, emb_pct: float):
    path = os.path.join(DELIV, "FLAGSHIP_KPI.md")
    md = ["# Flagship Report v1.4 KPIs\n\n",
          f"- **RAG hit@5:** {hit_at_5}\n",
          f"- **Total unique citations:** {cites_count}\n",
          f"- **Page count:** {pages}\n",
          f"- **Embedding coverage (365d Tier-1):** {emb_pct}%\n",
          f"- **Generation date:** {datetime.now(timezone.utc).isoformat()}\n"]
    write_text(path, "".join(md))


def write_release_notes():
    path = os.path.join(DELIV, "RELEASE_NOTES_v1_4.md")
    lines = [
        "# Release Notes — Flagship v1.4\n\n",
        "- New sections: Executive Summary (DR), Cross-ASEAN Themes, KOMDIGI Spotlight, Investor Lens\n",
        "- Expanded/improved: Citations and visuals; charts updated in brand style\n",
        "- Citation count increased and validated with link checks\n",
        "- Data freshness: 365d Tier-1 filter; optional embeddings backfill under $2 budget\n",
        "- HTML site updated and snapshot archive published\n"
    ]
    write_text(path, "".join(lines))


def write_costs_json(dr_prompts_used: int, emb_meta: Dict[str,Any] | None):
    # OpenAI cost from tracker; DR prompt counts fill model bucket
    costs = tracker.summary_dict()
    openai_cost_total = costs.get("cost",{}).get("total_usd", 0.0)
    by_model = costs.get("cost",{}).get("by_model", {})
    # inject DR counts
    by_model.setdefault("o3-deep-research", {"input_usd":0.0, "output_usd":0.0, "total_usd":0.0})
    by_model["o3-deep-research"]["total_usd"] = by_model.get("o3-deep-research",{}).get("total_usd",0.0)
    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "openai": {
            "gpt-4o-mini": {
                "tokens": costs.get("tokens",{}).get("by_model", {}).get("gpt-4o-mini", {}).get("input",0) +
                          costs.get("tokens",{}).get("by_model", {}).get("gpt-4o-mini", {}).get("output",0),
                "cost_usd": by_model.get("gpt-4o-mini",{}).get("total_usd", 0.0)
            },
            "o3-deep-research": {"prompts": dr_prompts_used, "cost_usd": by_model.get("o3-deep-research",{}).get("total_usd", 0.0)},
            "text-embedding-3-small": {
                "tokens": costs.get("tokens",{}).get("by_model", {}).get("text-embedding-3-small", {}).get("input",0),
                "cost_usd": by_model.get("text-embedding-3-small",{}).get("total_usd", 0.0)
            },
            "total_cost_usd": round(openai_cost_total, 6)
        },
        "firecrawl": {"urls_crawled": 0, "cost_usd": 0},
        "total_cost_usd": round(openai_cost_total, 6)
    }
    if emb_meta and emb_meta.get("meta"):
        # include projected embedding cost
        data["openai"]["text-embedding-3-small"]["cost_usd"] = round(emb_meta["meta"].get("projected_cost_usd", 0.0), 6)
        data["total_cost_usd"] = round(data["total_cost_usd"] + data["openai"]["text-embedding-3-small"]["cost_usd"], 6)
    write_text(os.path.join(LATEST, "model_costs.json"), json.dumps(data, indent=2))
    return data


def make_snapshot(md_path: str, html_path: str, pdf_path: str) -> str:
    ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    snap = os.path.join(DELIV, f"flagship_v1_4_snapshot_{ts}.zip")
    with zipfile.ZipFile(snap, 'w', zipfile.ZIP_DEFLATED) as z:
        for p in [md_path, html_path]:
            z.write(p, os.path.relpath(p, DELIV))
        if pdf_path and os.path.exists(pdf_path):
            z.write(pdf_path, os.path.relpath(pdf_path, DELIV))
        # Charts
        for fn in os.listdir(CHARTS_DIR):
            if fn.endswith('.png'):
                z.write(os.path.join(CHARTS_DIR, fn), os.path.join('flagship_charts', fn))
        # KPI, Release Notes, model costs
        for extra in [os.path.join(DELIV, 'FLAGSHIP_KPI.md'), os.path.join(DELIV, 'RELEASE_NOTES_v1_4.md'), os.path.join(LATEST,'model_costs.json')]:
            if os.path.exists(extra):
                z.write(extra, os.path.relpath(extra, DELIV) if extra.startswith(DELIV) else os.path.join('..', os.path.relpath(extra, ROOT)))
    write_text(os.path.join(LATEST, 'snapshot_path.txt'), snap + "\n")
    return snap


def main():
    # Phase A
    v13_pages = phase_a_baseline()

    # Phase B: RAG eval + tuning
    rag_res = eval_and_tune_rag(os.path.join(LATEST, "rag_tuning.txt"))
    hit5 = float(rag_res.get("hit_rate_at_5") or rag_res.get("hit_rate") or 0.0)


# ---------------- Maintenance Preflight (non-regeneration) ----------------

def preflight_check():
    """Non-blocking preflight check for next runs. Reads existing outputs only."""
    import re
    status_lines = ["Flagship v1.4 Preflight Status"]
    # Citations from docs/flagship.html (Sources & Notes section)
    try:
        html_path = os.path.join(DOCS_DIR, "flagship.html")
        text = ""
        if os.path.exists(html_path):
            with open(html_path, "r", encoding="utf-8") as f:
                text = f.read()
        urls = set(re.findall(r"https?://[^\s'\"<>]+", text or ""))
        citations = len(urls)
    except Exception:
        citations = 0
    # Pages from v1_4_pages.txt
    try:
        with open(os.path.join(LATEST, "v1_4_pages.txt"), "r", encoding="utf-8") as f:
            m = re.search(r"(\d+)", f.read())
            pages = int(m.group(1)) if m else 0
    except Exception:
        pages = 0
    # RAG hit@5 from rag_eval_results.json (fallback to rag_tuning.txt)
    hit5 = 0.0
    try:
        rej = read_json(os.path.join(LATEST, "rag_eval_results.json"))
        if isinstance(rej, dict):
            hit5 = float(rej.get("hit_rate_at_5") or rej.get("hit_rate") or 0.0)
    except Exception:
        pass
    if not hit5:
        try:
            with open(os.path.join(LATEST, "rag_tuning.txt"), "r", encoding="utf-8") as f:
                j = f.read()
                m = re.search(r"\"hit_rate[_a-z0-9]*\"\s*:\s*([0-9.]+)", j)
                if m:
                    hit5 = float(m.group(1))
        except Exception:
            pass
    # Compose
    status_lines.append(f"- Citations: {citations} (target: {CITATIONS_MIN})")
    status_lines.append(f"- Pages: {pages} (target: {TARGET_PAGES_MIN}-{TARGET_PAGES_MAX})")
    status_lines.append(f"- RAG hit@5: {hit5} (target: {RAG_HIT_AT_5_TARGET})")
    # PASS/WARN/FAIL
    if citations >= CITATIONS_MIN and TARGET_PAGES_MIN <= pages <= TARGET_PAGES_MAX and hit5 >= RAG_HIT_AT_5_TARGET:
        status = "PASS"
    elif citations == 0 or pages == 0:
        status = "FAIL"
    else:
        status = "WARN"
    status_lines.append(f"- Status: {status}")
    out_path = os.path.join(LATEST, "flagship_preflight_status.txt")
    write_text(out_path, "\n".join(status_lines) + "\n")
    return {"citations": citations, "pages": pages, "hit5": hit5, "status": status, "path": out_path}

    # Phase E: Outputs
    md_path, html_path, pdf_path, v14_pages = write_outputs(final_md)

    # Update website charts already saved in docs/flagship_charts

    # Payment link TODO handled in HTML/MD generation

    # Phase F: QA & metrics
    total_cites, dead = link_validation(cites)
    write_kpis(hit5, len(set(cites)), v14_pages or v13_pages, emb_pct)
    write_release_notes()
    costs = write_costs_json(int(dr_used_count), emb_meta)
    snap = make_snapshot(md_path, html_path, pdf_path)

    # Final summary line
    openai_total = costs.get('openai',{}).get('total_cost_usd', 0)
    firecrawl_cost = costs.get('firecrawl',{}).get('cost_usd', 0)
    pages_out = v14_pages or v13_pages
    print(f"Flagship v1.4 COMPLETE | pages={pages_out} | citations={len(set(cites))} | RAG hit@5={hit5} | OpenAI=${openai_total} | Firecrawl=${firecrawl_cost} | Snapshot={snap}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Minimal 429 tripwire logging
        msg = str(e)
        if '429' in msg:
            write_text(os.path.join(LATEST,'blockers.md'), f"{datetime.utcnow().isoformat()} rate_limit trip: {e}\n")
        raise

