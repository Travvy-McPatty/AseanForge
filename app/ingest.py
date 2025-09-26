#!/usr/bin/env python3
"""
ASEANForge Policy Tape - Ingestion CLI (MVP: MAS + IMDA only)

Commands:
  python app/ingest.py run --since=YYYY-MM-DD
  python app/ingest.py dry-run --since=YYYY-MM-DD

Pipeline per discovered article:
  Firecrawl (JS-rendered) -> detect content-type ->
  PDF: extract text via pdfminer.six (OCR disabled; skip if image-only)
  HTML: strip boilerplate ->
  langdetect -> summarize with gpt-4o-mini (en: 3 sentences; non-en: 5 sentences in English)
  classify via configs/rules.yaml keywords (no LLM validation) ->
  embedding via text-embedding-3-small ->
  Upsert into Neon events/documents using idempotent event_hash.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

# dotenv
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Firecrawl (optional)
try:
    from firecrawl import FirecrawlApp  # type: ignore
except Exception:
    FirecrawlApp = None  # type: ignore

# PDF text extraction (correct import; no fallbacks)
from pdfminer.high_level import extract_text  # type: ignore

# Language detection
from langdetect import detect, DetectorFactory  # type: ignore
DetectorFactory.seed = 0

import psycopg2
from openai import OpenAI
import yaml

TZ = os.getenv("TIMEZONE", "Asia/Jakarta")

COUNTRY_BY_AUTH = {
    "MAS": "SG", "IMDA": "SG", "PDPC": "SG",
    "BI": "ID", "OJK": "ID", "KOMINFO": "ID",
    "BOT": "TH",
    "BNM": "MY", "SC": "MY", "MCMC": "MY",
    "BSP": "PH", "DICT": "PH",
    "MIC": "VN", "SBV": "VN",
    "ASEAN": "ASEAN"
}

# ---------------- config loaders ----------------

def load_seed() -> Dict:
    p = os.path.join(os.path.dirname(__file__), "..", "configs", "firecrawl_seed.json")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def load_rules() -> Dict:
    p = os.path.join(os.path.dirname(__file__), "..", "configs", "rules.yaml")
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# ---------------- HTTP utils ----------------

def http_get(url: str, timeout: int = 45) -> Tuple[bytes, str]:
    req = urllib.request.Request(url, headers={
        "User-Agent": "ASEANForgePolicyTape/1.0 (+https://aseanforge.com)",
        "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        ct = resp.info().get_content_type()
        data = resp.read()
        return data, ct


def looks_like_pdf(url: str, content_type: str) -> bool:
    return content_type == "application/pdf" or url.lower().endswith(".pdf")


def pdf_text_from_bytes(content: bytes) -> str:
    with io.BytesIO(content) as bio:
        try:
            text = extract_text(bio) or ""
        except Exception:
            text = ""
    # collapse whitespace
    return re.sub(r"\s+", " ", text).strip()


def strip_html(html: str) -> str:
    txt = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    txt = re.sub(r"<style[\s\S]*?</style>", " ", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    # hyphenation fix
    txt = re.sub(r"-\s+\n", "", txt)
    return re.sub(r"\s+", " ", txt).strip()


def discover_links(base_url: str, html: str, limit: int = 12) -> List[str]:
    patt = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.I)
    raw = patt.findall(html)
    urls: List[str] = []
    for href in raw:
        if href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = urllib.parse.urljoin(base_url, href)
        if not full.startswith("http"): continue
        if re.search(r"news|press|release|media|publication|policy|guideline|notice|consult", full, re.I):
            urls.append(full)
        if len(urls) >= limit:
            break
    # de-dup by preserving order
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- LLM + Embeddings ----------------

def openai_client() -> OpenAI:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set")
    return OpenAI(api_key=key)


def detect_language(text: str) -> str:
    try:
        return detect(text)[:2]
    except Exception:
        return "en"


def summarize(client: OpenAI, text: str, source_lang: str) -> str:
    model = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
    text = text[:8000]
    n_sent = 3 if (source_lang or "en").lower().startswith("en") else 5
    sys_prompt = (
        "Summarize the following content in English with EXACTLY "
        f"{n_sent} sentences, concise and factual."
        " Reply ONLY with the summary text."
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": text},
        ],
        temperature=0.1,
    )
    return (resp.choices[0].message.content or "").strip()


def embed_text(client: OpenAI, text: str) -> List[float]:
    model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")
    resp = client.embeddings.create(model=model, input=text[:6000])
    return resp.data[0].embedding  # type: ignore

# ---------------- Classification ----------------

def classify(authority: str, title: str, body: str, rules: Dict) -> Tuple[str, str]:
    """Return (policy_area, action_type) by simple keyword search; defaults to 'other'."""
    low = (title + "\n" + body).lower()
    auth = (rules.get("rules", {}).get("authorities", {}) or {}).get(authority, {})
    area = "other"
    act = "other"
    for r in auth.get("policy_area", []) or []:
        for kw in r.get("keywords", []) or []:
            if kw.lower() in low:
                area = r.get("name", area)
                break
        if area != "other":
            break
    for r in auth.get("action_type", []) or []:
        for kw in r.get("keywords", []) or []:
            if kw.lower() in low:
                act = r.get("name", act)
                break
        if act != "other":
            break
    return area, act

# ---------------- DB ----------------

def get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL not set")
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def compute_event_hash(url: str, pub_date: dt.datetime, title: str) -> str:
    key = f"{url}|{pub_date.date().isoformat()}|{title or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def parse_published_at(html: str) -> Optional[dt.datetime]:
    m = re.search(r'property=["\']article:published_time["\']\s+content=["\']([^"\']+)["\']', html or "", re.I)
    if not m:
        m = re.search(r'name=["\']pubdate["\']\s+content=["\']([^"\']+)["\']', html or "", re.I)
    if not m:
        m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html or "", re.I)
    if m:
        try:
            val = m.group(1)
            dt_val = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt_val.astimezone(dt.timezone.utc)
        except Exception:
            return None
    return None

# ---------------- Firecrawl ----------------

def fc_fetch(app_obj, url: str) -> Optional[Dict]:
    if not app_obj:
        return None
    try:
        result = app_obj.scrape_url(url, params={
            "formats": ["text", "html", "markdown"],
            "javascript": True,
            "onlyMainContent": True,
            "pdf": {"enabled": True},
        })
        return {
            "html": result.get("html") or "",
            "text": result.get("text") or "",
            "title": result.get("title") or "",
        }
    except Exception:
        return None

# ---------------- Processing ----------------

def process_article(oa: OpenAI, authority: str, url: str, fc_app, since_date: dt.date, dry_run: bool, rules: Dict, metrics: Dict):
    html_page = ""; text = ""; title = ""; content_type = "html"
    page = fc_fetch(fc_app, url)
    if page:
        html_page = page.get("html", ""); text = page.get("text", ""); title = page.get("title", "")
    if not text:
        data, ctype = http_get(url)
        if looks_like_pdf(url, ctype):
            content_type = "pdf"
            text = pdf_text_from_bytes(data)
            if not text:
                logging.info("PDF has no extractable text and OCR disabled; skipping: %s", url)
                return
        else:
            html_page = data.decode("utf-8", errors="ignore")
            text = strip_html(html_page)
            m = re.search(r"<title>(.*?)</title>", html_page, re.I | re.S)
            title = (m.group(1).strip() if m else title)

    if not text:
        logging.info("Empty content; skipping %s", url)
        return

    published_at = parse_published_at(html_page) if html_page else None
    if not published_at:
        published_at = dt.datetime.now(dt.timezone.utc)
    if published_at.date() < since_date:
        return

    lang = detect_language(text)
    summary_en = summarize(oa, text, lang)

    policy_area, action_type = classify(authority, title or "", text, rules)
    if not policy_area: policy_area = "other"
    if not action_type: action_type = "other"

    event_hash = compute_event_hash(url, published_at, title or "")
    country = COUNTRY_BY_AUTH.get(authority, "SG")

    embedding = embed_text(oa, summary_en or text[:1000])

    evt = {
        "event_hash": event_hash,
        "pub_date": published_at,
        "country": country,
        "authority": authority,
        "policy_area": policy_area,
        "action_type": action_type,
        "title": title or f"{authority}: Document",
        "url": url,
        "source_tier": 1,
        "content_type": content_type,
        "lang": "en" if summary_en else (lang or "en"),
        "is_ocr": False,
        "ocr_quality": None,
        "source_confidence": 0.9,
        "summary_en": summary_en,
        "embedding": embedding,
    }
    doc = {
        "source": urllib.parse.urlsplit(url).netloc,
        "source_url": url,
        "title": title,
        "raw_text": text,
        "clean_text": text,
        "page_spans": None,
        "rendered": bool(page and page.get("html")),
    }

    if dry_run:
        metrics["items_fetched"] += 1
        logging.info(json.dumps({"action": "dry-run", "authority": authority, "url": url, "policy_area": policy_area, "action_type": action_type}))
        return

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (
              event_hash, pub_date, country, authority, policy_area, action_type, title, url, source_tier,
              content_type, lang, is_ocr, ocr_quality, source_confidence, summary_en, embedding
            ) VALUES (
              %(event_hash)s, %(pub_date)s, %(country)s, %(authority)s, %(policy_area)s, %(action_type)s, %(title)s, %(url)s, %(source_tier)s,
              %(content_type)s, %(lang)s, %(is_ocr)s, %(ocr_quality)s, %(source_confidence)s, %(summary_en)s, %(embedding)s
            )
            ON CONFLICT (event_hash) DO UPDATE SET
              policy_area = EXCLUDED.policy_area,
              action_type = EXCLUDED.action_type,
              title = COALESCE(EXCLUDED.title, events.title),
              summary_en = EXCLUDED.summary_en,
              lang = EXCLUDED.lang,
              embedding = EXCLUDED.embedding
            RETURNING event_id, (xmax = 0) AS inserted
            """,
            evt,
        )
        eid, inserted = cur.fetchone()

        d = doc.copy(); d["event_id"] = eid
        cur.execute(
            """
            INSERT INTO documents (event_id, source, source_url, title, raw_text, clean_text, page_spans, rendered)
            VALUES (%(event_id)s, %(source)s, %(source_url)s, %(title)s, %(raw_text)s, %(clean_text)s, %(page_spans)s, %(rendered)s)
            ON CONFLICT (source_url) DO UPDATE SET
              event_id = EXCLUDED.event_id,
              title = COALESCE(EXCLUDED.title, documents.title),
              raw_text = EXCLUDED.raw_text,
              clean_text = EXCLUDED.clean_text,
              rendered = EXCLUDED.rendered
            """,
            d,
        )

    metrics["items_fetched"] += 1
    if inserted:
        metrics["items_new"] += 1
    logging.info(json.dumps({"action": "upsert", "authority": authority, "url": url, "inserted": bool(inserted)}))


def main():
    parser = argparse.ArgumentParser(description="ASEANForge Policy Tape Ingestion (MVP)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run ingestion and write to DB")
    p_run.add_argument("--since", type=str, required=True, help="YYYY-MM-DD")

    p_dry = sub.add_parser("dry-run", help="Run ingestion without DB writes")
    p_dry.add_argument("--since", type=str, required=True, help="YYYY-MM-DD")

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    try:
        since = dt.date.fromisoformat(args.since)
    except Exception:
        print("--since must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    seed = load_seed(); rules = load_rules()
    start_urls = seed.get("startUrls", [])  # ingest all configured authorities

    oa = openai_client()

    fc_app = None
    if FirecrawlApp and os.getenv("FIRECRAWL_API_KEY"):
        try:
            fc_app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY"))  # type: ignore
        except Exception:
            logging.warning("Firecrawl init failed; falling back to urllib")

    metrics = {"items_fetched": 0, "items_new": 0, "parse_failures": 0, "start": time.time()}

    for entry in start_urls:
        base = entry.get("url"); label = entry.get("label")
        if not base: continue
        try:
            landing = fc_fetch(fc_app, base) if fc_app else None
            html = landing.get("html") if landing else ""
            if not html:
                data, ct = http_get(base)
                if ct.startswith("text"):
                    html = data.decode("utf-8", errors="ignore")
            links = discover_links(base, html, limit=8)
            # process up to 8 links per source
            for url in links[:8]:
                try:
                    process_article(oa, label, url, fc_app, since, args.cmd == "dry-run", rules, metrics)
                except Exception as e:
                    metrics["parse_failures"] += 1
                    logging.exception("Failed %s: %s", url, e)
        except Exception as e:
            logging.warning("Source scan failed %s: %s", label, e)

    duration_ms = int((time.time() - metrics["start"]) * 1000)
    out = {"metrics": {k: v for k, v in metrics.items() if k != "start"}, "duration_ms": duration_ms}
    logging.info(json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()

