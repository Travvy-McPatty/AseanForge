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
    load_dotenv("app/.env")
except Exception:
    pass

# Firecrawl (optional)
try:
    from firecrawl import Firecrawl  # type: ignore
except Exception:
    Firecrawl = None  # type: ignore

# PDF text extraction (correct import; no fallbacks)
from pdfminer.high_level import extract_text  # type: ignore

# Language detection
from langdetect import detect, DetectorFactory  # type: ignore
DetectorFactory.seed = 0

import psycopg2
from openai import OpenAI
import yaml

import csv

TZ = os.getenv("TIMEZONE", "Asia/Jakarta")

COUNTRY_BY_AUTH = {
    "MAS": "SG", "IMDA": "SG", "PDPC": "SG",
    "BI": "ID", "OJK": "ID", "KOMINFO": "ID", "KOMDIGI": "ID",
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


# ---------------- Feeds override ----------------

def load_feeds_override() -> Dict[str, Dict]:
    p = os.path.join(os.path.dirname(__file__), "..", "configs", "feeds_override.json")
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
            # support list of entries or dict
            result = {}
            if isinstance(data, list):
                for e in data:
                    if isinstance(e, dict) and e.get("authority") and e.get("feed") and e.get("enabled"):
                        result[e["authority"]] = e
            elif isinstance(data, dict):
                for k, v in data.items():
                    if isinstance(v, dict) and v.get("feed") and v.get("enabled"):
                        result[k] = {"authority": k, **v}
            return result
    except Exception:
        return {}


def harvest_feed_urls(authority: str, feed_url: str, limit: int = 5) -> List[str]:
    try:
        data, ctype = http_get(feed_url)
        text = data.decode("utf-8", errors="ignore") if data else ""
        urls: List[str] = []
        if "wp-json/wp/v2/posts" in feed_url and text.strip().startswith("["):
            try:
                arr = json.loads(text)
                for it in arr:
                    u = it.get("link") or it.get("url")
                    if u:
                        urls.append(u)
            except Exception:
                pass
        elif "xml" in (ctype or "") or "<rss" in text or "<feed" in text:
            for m in re.finditer(r"<item[\s\S]*?<link>(.*?)</link>[\s\S]*?</item>", text, re.I):
                urls.append(m.group(1).strip())
            if not urls:
                for m in re.finditer(r"<entry[\s\S]*?<link[^>]*href=\"([^\"]+)\"[\s\S]*?</entry>", text, re.I):
                    urls.append(m.group(1).strip())
        # de-dup and cap
        seen = set(); out = []
        for u in urls:
            if u and u not in seen:
                seen.add(u); out.append(u)
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []

# ---------------- HTTP utils ----------------

def http_get(url: str, timeout: int = 45) -> Tuple[bytes, str]:
    # Minimal redirect handling (301/302/307/308) without new deps; cap at 5 hops
    redirects = 0
    cur = url
    while redirects <= 5:
        req = urllib.request.Request(cur, method="GET", headers={
            "User-Agent": "ASEANForgePolicyTape/1.0 (+https://aseanforge.com)",
            "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8",
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ct = resp.info().get_content_type()
                data = resp.read()
                return data, ct
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 307, 308):
                loc = e.headers.get("Location")
                if not loc:
                    raise
                cur = urllib.parse.urljoin(cur, loc)
                redirects += 1
                continue
            raise
    raise urllib.error.HTTPError(cur, 310, "Too many redirects", hdrs=None, fp=None)


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


def discover_links(base_url: str, html: str, limit: int = 8) -> List[str]:
    # Sitemap XML support
    if base_url.lower().endswith(".xml") or ("<urlset" in (html or "")):
        locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", html or "", flags=re.I)
        uniq = []
        seen = set()
        for u in locs:
            if u.startswith("http") and u not in seen:
                seen.add(u); uniq.append(u)
                if len(uniq) >= limit:
                    break
        return uniq
    patt = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.I)
    raw = patt.findall(html or "")
    urls: List[str] = []
    base_dom = urllib.parse.urlsplit(base_url).netloc.lower()
    for href in raw:
        if href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = urllib.parse.urljoin(base_url, href)
        if not full.startswith("http"): continue
        dom = urllib.parse.urlsplit(full).netloc.lower()
        if base_dom and dom != base_dom:
            continue
        if "komdigi.go.id" in base_dom:
            if ("/produk_hukum/" in full) or ("/berita" in full):
                urls.append(full)
        else:
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
# Per-authority Firecrawl proxy/wait policy and telemetry helper
from typing import Optional, Tuple

# Rate limit state tracker
rate_limit_state = {
    'consecutive_429s': 0,
    'current_concurrency': int(os.getenv('MAX_CONCURRENCY', '2')),
    'paused_until': None
}

def resolve_fc_proxy_and_wait_ms(authority: Optional[str]) -> Tuple[str, int]:
    auth = (authority or "").upper()
    wait_default = int(os.getenv("FIRECRAWL_WAIT_MS", "2000"))
    if auth in ("BNM", "KOMINFO"):
        return ("stealth", 12000)
    if auth in ("ASEAN", "OJK", "MCMC", "DICT"):
        return ("stealth", 5000)
    # KOMDIGI defaults to auto; escalate via retry if needed
    return ("auto", wait_default)


def write_provider_event(authority: Optional[str], url: str, provider: str, status_code_or_error: str, wait_ms: int, proxy_mode: str, notes: str = "") -> None:
    try:
        out_dir = os.path.join("data", "output", "validation", "latest")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "provider_events.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["authority", "url", "provider", "status_code_or_error", "waitFor_ms", "proxy_mode", "timestamp", "notes"])
            w.writerow([authority or "", url, provider, status_code_or_error, wait_ms, proxy_mode, dt.datetime.utcnow().isoformat(), notes])
    except Exception:
        pass

def write_quality_drop(authority: Optional[str], url: str, reason: str, metric: str = "") -> None:
    try:
        out_dir = os.path.join("data", "output", "validation", "latest")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "quality_drops.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["authority", "url", "reason", "metric"])
            w.writerow([authority or "", url, reason, metric])
    except Exception:
        pass


def ascii_ratio(text: str) -> float:
    if not text:
        return 0.0
    total = sum(1 for ch in text if not ch.isspace())
    if total == 0:
        return 0.0
    ascii_count = sum(1 for ch in text if ord(ch) < 128 and not ch.isspace())
    return ascii_count / total


def is_link_farm_html(html: str) -> float:
    if not html:
        return 0.0
    nonws = ''.join(ch for ch in html if not ch.isspace())
    if not nonws:
        return 0.0
    links = re.findall(r"<a\b[\s\S]*?>([\s\S]*?)</a>", html, flags=re.I)
    link_chars = sum(len(''.join(x.split())) for x in links)
    list_items = re.findall(r"<li\b[\s\S]*?</li>", html, flags=re.I)
    li_chars = sum(len(''.join(x.split())) for x in list_items)
    return min(1.0, (link_chars + li_chars) / max(1, len(nonws)))


def contains_not_found(title: Optional[str], text_or_html: str) -> bool:
    hay = ((title or "") + "\n" + (text_or_html or "")).lower()
    phrases = ["not found", "404", "page no longer exists", "this page no longer exists"]
    return any(p in hay for p in phrases)

def write_fc_error(domain: str, url: str, status: str, error_msg: str) -> None:
    try:
        out_dir = os.path.join("data", "output", "validation", "latest")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "fc_errors.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["domain", "url", "status", "error"])
            w.writerow([domain, url, status, (error_msg or "").strip()[:500]])
    except Exception:
        pass


def check_rate_limit_pause():
    """Check if we need to pause due to rate limiting."""
    global rate_limit_state

    if rate_limit_state['paused_until'] and time.time() < rate_limit_state['paused_until']:
        pause_duration = rate_limit_state['paused_until'] - time.time()
        logging.warning(f"Rate limit pause active, sleeping {pause_duration:.1f}s...")
        time.sleep(pause_duration)
        rate_limit_state['paused_until'] = None


def handle_rate_limit_error(error_msg: str):
    """Handle rate limit error (429 or similar)."""
    global rate_limit_state

    rate_limit_state['consecutive_429s'] += 1

    if rate_limit_state['consecutive_429s'] >= 3:
        # Hard halt after 3 consecutive 429s (per guardrail)
        os.makedirs("data/output/validation/latest", exist_ok=True)
        with open('data/output/validation/latest/rate_limit_trip.txt', 'w') as f:
            f.write(f"HALTED: {rate_limit_state['consecutive_429s']} consecutive 429s at {dt.datetime.now(dt.timezone.utc).isoformat()}\n")
            f.write(f"Error: {error_msg}\n")
        raise RuntimeError("Rate limit circuit breaker tripped after 3 consecutive 429s")


def reset_rate_limit_counter():
    """Reset rate limit counter on successful request."""
    global rate_limit_state
    rate_limit_state['consecutive_429s'] = 0


# v2 helper: Firecrawl-first scrape with per-authority proxy/wait and telemetry
def fc_fetch(app_obj, url: str, authority: Optional[str], proxy_mode: str, wait_ms: int) -> Optional[Dict]:
    if not app_obj:
        return None

    # Check rate limit pause
    check_rate_limit_pause()

    delay_ms = int(os.getenv("CRAWL_DELAY_MS", "1200"))
    page_opts = {
        "onlyMainContent": True,
        "waitFor": int(wait_ms),
        "timeout": 60000,
        "includeHtml": True,
        "parsePDF": True,
    }
    api_path = "v2"
    try:
        # Preferred v2 signature
        doc = app_obj.scrape(url=url, formats=["markdown", "html", "text"], pageOptions=page_opts, parsers=["pdf"], proxy=proxy_mode, maxAge=172800000)  # type: ignore
        reset_rate_limit_counter()  # Success, reset counter
    except TypeError:
        # Legacy fallbacks
        api_path = "legacy"
        try:
            doc = app_obj.scrape(url=url, formats=["markdown", "html"])  # type: ignore
        except Exception:
            try:
                doc = app_obj.scrape({"url": url, "formats": ["markdown", "html"]})  # type: ignore[arg-type]
            except Exception:
                doc = None  # type: ignore
    except Exception as e:
        # Check for rate limit errors
        error_str = str(e).lower()
        if '429' in error_str or 'rate limit' in error_str or 'too many requests' in error_str:
            handle_rate_limit_error(str(e))
        doc = None  # type: ignore
    html = ""; text = ""; title = ""
    if doc is not None:
        data = getattr(doc, "data", {}) or {}
        html = getattr(doc, "html", "") or (data.get("html", "") if isinstance(data, dict) else "")
        text = getattr(doc, "text", "") or (data.get("text", "") if isinstance(data, dict) else "")
        md = getattr(doc, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
        if not text and md:
            text = md
        meta = (data.get("metadata") if isinstance(data, dict) else {}) or {}
        title = meta.get("title", "")
    # polite delay
    if delay_ms > 0:
        try:
            time.sleep(delay_ms / 1000.0)
        except Exception:
            pass
    try:
        write_provider_event(authority, url, "firecrawl", ("ok" if (html or text) else "empty"), int(wait_ms), proxy_mode, api_path)
    except Exception:
        pass
    if not (html or text):
        # Retry escalation for stubborn sources with higher wait, selectors, SG locale
        try:
            extra_kwargs = {"maxAge": 172800000}
            if (authority or "").upper() in ("BNM", "KOMINFO"):
                extra_kwargs["selectors"] = ["article", ".post-content", ".news-detail"]
                extra_kwargs["location"] = {"country": "SG", "languages": ["en-SG"]}
            page_opts2 = {**page_opts, "waitFor": 12000}
            doc2 = app_obj.scrape(url=url, formats=["markdown", "html", "text"], pageOptions=page_opts2, parsers=["pdf"], proxy=proxy_mode, **extra_kwargs)  # type: ignore
            data2 = getattr(doc2, "data", {}) or {}
            html = getattr(doc2, "html", "") or (data2.get("html", "") if isinstance(data2, dict) else "")
            text = getattr(doc2, "text", "") or (data2.get("text", "") if isinstance(data2, dict) else "")
            md = getattr(doc2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
            if not text and md:
                text = md
        except TypeError:
            try:
                doc2 = app_obj.scrape(url=url, formats=["markdown", "html", "text"], pageOptions={**page_opts, "waitFor": 12000}, parsers=["pdf"], proxy=proxy_mode, maxAge=172800000)  # type: ignore
                data2 = getattr(doc2, "data", {}) or {}
                html = getattr(doc2, "html", "") or (data2.get("html", "") if isinstance(data2, dict) else "")
                text = getattr(doc2, "text", "") or (data2.get("text", "") if isinstance(data2, dict) else "")
                md = getattr(doc2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
                if not text and md:
                    text = md
            except Exception as e:
                write_fc_error(urllib.parse.urlsplit(url).netloc, url, "scrape_retry_error", str(e))
        except Exception as e:
            write_fc_error(urllib.parse.urlsplit(url).netloc, url, "scrape_error", str(e))
    if html or text:
        return {"html": html, "text": text, "title": title}
    return None

# Legacy flexible helper retained for probe mode; per-request stealthProxy is not supported in v2
# and is kept here only as a no-op in practice.
def fc_fetch_with_params(app_obj, url: str, params: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    if not app_obj:
        return None, "no_firecrawl_app"
    last_err: Optional[str] = None
    for meth_name in ("scrape", "scrape_url", "scrapeUrl"):
        try:
            meth = getattr(app_obj, meth_name, None)
            if not meth:
                continue
            try:
                # Prefer v2: pass url kw with pageOptions if present
                page_opts = params.get("pageOptions") or {
                    "onlyMainContent": params.get("onlyMainContent", True),
                    "waitFor": params.get("waitFor") or ".article, .press, main, article",
                    "timeout": params.get("timeout", 60000),
                    "includeHtml": True,
                    "parsePDF": True,
                }
                result = meth(url=url, formats=params.get("formats") or ["text", "html", "markdown"], pageOptions=page_opts)  # type: ignore
            except TypeError:
                result = meth({"url": url, **params})
            page = {
                "html": (result.get("html") if isinstance(result, dict) else "") or "",
                "text": (result.get("text") if isinstance(result, dict) else "") or (result.get("markdown") if isinstance(result, dict) else "") or "",
                "title": (result.get("title") if isinstance(result, dict) else "") or "",
            }
            if page["html"] or page["text"]:
                return page, None
        except Exception as e:
            last_err = f"exc={e.__class__.__name__}:{str(e)[:200]}"
    return None, last_err or "unknown_error"

# v2 helper: crawl landing and extract candidate URLs (shallow)
def fc_crawl_links(app_obj, base_url: str, limit: int = 8, max_depth: int = 1, proxy_mode: str = "auto", wait_ms: int = 2000, authority: Optional[str] = None) -> List[str]:
    if not app_obj or not base_url:
        return []
    delay_ms = int(os.getenv("CRAWL_DELAY_MS", "1200"))
    items: List[Dict] = []
    api_path = "v2"
    # Try v2 first; fall back to legacy signatures if needed
    try:
        docs = app_obj.crawl(
            url=base_url,
            limit=limit,
            pageOptions={"waitFor": int(wait_ms), "timeout": 60000, "includeHtml": True, "parsePDF": True, "onlyMainContent": True},
            crawlerOptions={"maxDepth": int(max_depth)},
            proxy=proxy_mode,
            poll_interval=1,
            timeout=120,
            maxAge=172800000
        )  # type: ignore
    except TypeError:
        api_path = "legacy"
        try:
            docs = app_obj.crawl(base_url, limit=limit)  # type: ignore[arg-type]
        except Exception:
            try:
                docs = app_obj.crawl({"url": base_url, "limit": limit, "crawlerOptions": {"maxDepth": max_depth, "delayMs": delay_ms}})  # type: ignore[arg-type]
            except Exception:
                docs = None  # type: ignore
    except Exception:
        docs = None  # type: ignore
    # polite delay
    if delay_ms > 0:
        try:
            time.sleep(delay_ms / 1000.0)
        except Exception:
            pass
    if isinstance(docs, dict) and "data" in docs:
        items = docs.get("data") or []
    elif hasattr(docs, "data"):
        items = getattr(docs, "data") or []  # type: ignore
    elif isinstance(docs, list):
        items = docs  # type: ignore
    else:
        items = []
    try:
        write_provider_event(authority, base_url, "firecrawl", ("ok" if items else "empty"), int(wait_ms), proxy_mode, api_path)
    except Exception:
        pass
    if not items and (authority or "").upper() in ("BNM", "KOMINFO"):
        # Escalate crawl retry
        try:
            docs2 = app_obj.crawl(url=base_url, limit=limit, pageOptions={"waitFor": 12000, "timeout": 60000, "includeHtml": True, "parsePDF": True, "onlyMainContent": True}, proxy=proxy_mode, poll_interval=1, timeout=120, maxAge=172800000, location={"country": "SG", "languages": ["en-SG"]})  # type: ignore
            if isinstance(docs2, dict) and "data" in docs2:
                items = docs2.get("data") or []
            elif hasattr(docs2, "data"):
                items = getattr(docs2, "data") or []
        except TypeError:
            try:
                docs2 = app_obj.crawl(url=base_url, limit=limit, pageOptions={"waitFor": 12000, "timeout": 60000, "includeHtml": True, "parsePDF": True, "onlyMainContent": True}, proxy=proxy_mode, poll_interval=1, timeout=120, maxAge=172800000)  # type: ignore
                if isinstance(docs2, dict) and "data" in docs2:
                    items = docs2.get("data") or []
                elif hasattr(docs2, "data"):
                    items = getattr(docs2, "data") or []
            except Exception as e:
                write_fc_error(urllib.parse.urlsplit(base_url).netloc, base_url, "crawl_retry_error", str(e))
        except Exception as e:
            write_fc_error(urllib.parse.urlsplit(base_url).netloc, base_url, "crawl_error", str(e))
    urls: List[str] = []
    seen = set()
    for it in items:
        try:
            meta = (it.get("metadata") if isinstance(it, dict) else {}) or {}
            u = meta.get("sourceURL") or meta.get("ogUrl") or meta.get("url") or (it.get("url") if isinstance(it, dict) else None)
            if u and u.startswith("http") and u not in seen:
                seen.add(u)
                urls.append(u)
                if len(urls) >= limit:
                    break
        except Exception:
            continue
    return urls

# ---------------- Processing ----------------

def process_article(oa: OpenAI, authority: str, url: str, fc_app, since_date: dt.date, dry_run: bool, rules: Dict, metrics: Dict, mode: str = "full"):
    html_page = ""; text = ""; title = ""; content_type = "html"
    proxy_mode, wait_ms = resolve_fc_proxy_and_wait_ms(authority)
    page = fc_fetch(fc_app, url, authority, proxy_mode, wait_ms)
    if page:
        html_page = page.get("html", ""); text = page.get("text", ""); title = page.get("title", "")
        logging.info(f"FETCH_PROVIDER=firecrawl mode=scrape url={url} waitFor={int(wait_ms)} proxy={proxy_mode}")
        try:
            write_provider_event(authority, url, "firecrawl", "ok", int(wait_ms), proxy_mode, "")
        except Exception:
            pass
        if metrics is not None:
            metrics["prov_fc_article"] = metrics.get("prov_fc_article", 0) + 1
            try:
                if authority and isinstance(metrics.get("by_auth"), dict):
                    slot = metrics["by_auth"].setdefault(authority, {})
                    slot["fc_article"] = slot.get("fc_article", 0) + 1
            except Exception:
                pass

    if not text:
        logging.info(f"FETCH_PROVIDER=http mode=scrape url={url} waitFor={int(wait_ms)} proxy={proxy_mode}")
        try:
            write_provider_event(authority, url, "http", "fallback", int(wait_ms), proxy_mode, "")
        except Exception:
            pass
        if metrics is not None:
            metrics["prov_http_article"] = metrics.get("prov_http_article", 0) + 1
            try:
                if authority and isinstance(metrics.get("by_auth"), dict):
                    slot = metrics["by_auth"].setdefault(authority, {})
                    slot["http_article"] = slot.get("http_article", 0) + 1
            except Exception:
                pass
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

    # Quality gates — thin content, 404, link-farm, language, dedup
    total_len = len((html_page or "")) + len((text or ""))
    reasons: List[str] = []
    if total_len < 700:
        reasons.append("thin")
    if contains_not_found(title, (html_page or text)):
        reasons.append("not_found")
    lf = is_link_farm_html(html_page or "")
    if lf > 0.65:
        reasons.append(f"link_farm({lf:.2f})")
    if (authority or "").upper() in ("ASEAN","MAS","IMDA","PDPC","SC","BNM","BOT","BSP","DICT","SBV","MIC"):
        ar = ascii_ratio(text)
        if ar < 0.60:
            reasons.append(f"non_english({ar:.2f})")
    # per-run dedup hash
    norm_title = (title or "").strip().lower()
    first400 = (text or "")[:400]
    qhash = hashlib.sha256((norm_title + "|" + url + "|" + first400).encode("utf-8")).hexdigest()
    global _SEEN_QHASH
    if '_SEEN_QHASH' not in globals():
        _SEEN_QHASH = set()
    if qhash in _SEEN_QHASH:
        reasons.append("dup_hash")
    if reasons:
        try:
            write_quality_drop(authority, url, ";".join(reasons), metric=str(total_len))
        except Exception:
            pass
        return
    _SEEN_QHASH.add(qhash)

    published_at = parse_published_at(html_page) if html_page else None
    if not published_at:
        published_at = dt.datetime.now(dt.timezone.utc)
    if published_at.date() < since_date:
        return

    lang = detect_language(text)

    # HARVEST mode: skip OpenAI calls (summarization and embeddings)
    # ENRICH mode: only process if summary/embedding missing (handled separately)
    # FULL mode: legacy behavior (summarize + embed inline)
    summary_en = None
    embedding = None

    # Check environment variables for override (backward compatibility)
    enable_summarization = os.getenv("ENABLE_SUMMARIZATION", "1") != "0"
    enable_embeddings = os.getenv("ENABLE_EMBEDDINGS", "1") != "0"

    if mode == "full" and enable_summarization and enable_embeddings:
        # Legacy mode: call OpenAI inline (may hit quota limits)
        summary_en = summarize(oa, text, lang)
        embedding = embed_text(oa, summary_en or text[:1000])
    elif mode == "harvest" or not enable_summarization or not enable_embeddings:
        # Harvest mode: skip OpenAI calls entirely
        logging.info(f"HARVEST mode: skipping summarization and embedding for {url}")
    # ENRICH mode is handled by a separate batch processing function (not in this flow)

    policy_area, action_type = classify(authority, title or "", text, rules)
    if not policy_area: policy_area = "other"
    if not action_type: action_type = "other"

    event_hash = compute_event_hash(url, published_at, title or "")
    country = COUNTRY_BY_AUTH.get(authority, "SG")

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


# ---------------- Aggressive Firecrawl Probe ----------------

def _fc_params_variant(stealth: bool, timeout_ms: int) -> Dict:
    return {
        "formats": ["text", "html", "markdown"],
        "javascript": True,
        "onlyMainContent": True,
        "pdf": {"enabled": True, "parsePDF": True},
        "render": True,
        "waitUntil": "networkidle",
        "timeout": timeout_ms,
        "stealthProxy": stealth,
        "ocr_fallback": True,
    }


def _variants_matrix() -> List[Tuple[str, Dict]]:
    out: List[Tuple[str, Dict]] = []
    for stealth in (True, False):
        for timeout_ms in (30000, 60000, 90000):
            for delay in (800, 1200, 1600, 2000):
                name = f"stealth={int(stealth)}_timeout={timeout_ms}_delayMs={delay}"
                params = _fc_params_variant(stealth, timeout_ms)
                params["delayMs"] = delay
                out.append((name, params))
    return out


def run_aggressive_probe(authorities: List[str], pages_per_auth: int, seed: Dict, fc_app) -> None:
    out_dir = os.path.join("data", "output", "validation", "aggressive_probe")
    os.makedirs(out_dir, exist_ok=True)
    summary_csv = os.path.join(out_dir, "summary.csv")
    if not os.path.exists(summary_csv):
        with open(summary_csv, "w", encoding="utf-8", newline="") as w:
            csv.writer(w).writerow(["authority", "url", "variant", "provider", "status", "notes"])

    # Build base URLs per authority from seed
    bases: Dict[str, List[str]] = {}
    for e in seed.get("startUrls", []) or []:
        lab = e.get("label"); url = e.get("url")
        if not lab or not url: continue
        if lab not in authorities: continue
        bases.setdefault(lab, [])
        if url not in bases[lab]:
            bases[lab].append(url)

    for auth, base_list in bases.items():
        log_path = os.path.join(out_dir, f"{auth.lower()}_probe.log")
        with open(log_path, "a", encoding="utf-8") as logf, open(summary_csv, "a", encoding="utf-8", newline="") as wcsv:
            writer = csv.writer(wcsv)
            for base in base_list:
                # Discover candidate pages from landing
                html = ""
                try:
                    data, ct = http_get(base)
                    if (ct or "").startswith("text"):
                        html = data.decode("utf-8", errors="ignore")
                except Exception as e:
                    logf.write(f"landing_http_error {base} {e}\n")
                if not html and fc_app:
                    page, err = fc_fetch_with_params(fc_app, base, _fc_params_variant(True, 60000))
                    if page and page.get("html"):
                        html = page.get("html")
                        logging.info(f"FETCH_PROVIDER=firecrawl url={base} waitFor=na proxy=na")
                        logf.write(f"landing_firecrawl_ok {base}\n")
                    else:
                        logf.write(f"landing_firecrawl_fail {base} {err}\n")
                candidates = [base] + discover_links(base, html or "", limit=max(5, pages_per_auth*2))
                candidates = candidates[:pages_per_auth]

                # Test each candidate with variants, then fallback to HTTP
                for url in candidates:
                    success = False
                    for vname, vparams in _variants_matrix():
                        page, err = fc_fetch_with_params(fc_app, url, vparams)
                        if page and (page.get("text") or page.get("html")):
                            logging.info(f"FETCH_PROVIDER=firecrawl url={url} waitFor=na proxy={'stealth' if vparams.get('stealthProxy') else 'auto'}")
                            writer.writerow([auth, url, vname, "firecrawl", "ok", ""])
                            logf.write(f"ok_firecrawl {vname} {url}\n")
                            success = True
                            break
                        else:
                            note = err or "empty"
                            if note and ("captcha" in note.lower() or "are you a robot" in note.lower()):
                                logf.write(f"captcha_detected {vname} {url} {note}\n")
                            elif note and ("429" in note or "rate" in note.lower()):
                                logf.write(f"rate_limited {vname} {url} {note}\n")
                            elif note and ("timeout" in note.lower()):
                                logf.write(f"timeout {vname} {url} {note}\n")
                            elif note and ("403" in note or "blocked" in note.lower() or "proxy" in note.lower()):
                                logf.write(f"proxy_inadequate {vname} {url} {note}\n")
                            else:
                                logf.write(f"firecrawl_fail {vname} {url} {note}\n")
                    if success:
                        continue
                    # HTTP fallback
                    try:
                        data, ct = http_get(url)
                        text = ""
                        if looks_like_pdf(url, ct):
                            text = pdf_text_from_bytes(data)
                        elif (ct or "").startswith("text"):
                            text = strip_html(data.decode("utf-8", errors="ignore"))
                        if text:
                            writer.writerow([auth, url, "-", "http", "ok", "fallback"])
                            logf.write(f"ok_http {url}\n")
                        else:
                            writer.writerow([auth, url, "-", "http", "empty", "fallback_empty"])
                            logf.write(f"http_empty {url}\n")
                    except Exception as e:
                        writer.writerow([auth, url, "-", "http", "error", str(e)[:120]])
                        logf.write(f"http_error {url} {e}\n")


def run_enrich_auto(args):
    """
    Run automatic batch enrichment pipeline.

    Builds, submits, polls, and merges both embeddings and summaries batches.
    """
    from app.enrich_batch import builders, submit, poll, merge

    # Determine budget
    budget = args.budget or float(os.getenv("ENRICH_MAX_USD_FULL", "200"))

    print(f"=== OpenAI Batch Enrichment Pipeline ===")
    print(f"Budget: ${budget:.2f}")
    print()

    # Build embeddings requests
    print("Step 1/6: Building embedding requests...")
    emb_meta = builders.build_embedding_requests(
        since_date=getattr(args, "since", None),
        limit=None,
        output_path="data/batch/embeddings.requests.jsonl"
    )
    print()

    # Build summaries requests
    print("Step 2/6: Building summary requests...")
    sum_meta = builders.build_summary_requests(
        since_date=getattr(args, "since", None),
        limit=None,
        output_path="data/batch/summaries.requests.jsonl"
    )
    print()

    # Cost gate
    total_cost = emb_meta.get("projected_cost_usd", 0) + sum_meta.get("projected_cost_usd", 0)
    print(f"Total projected cost: ${total_cost:.4f}")

    if total_cost > budget:
        print(f"ERROR: Projected cost ${total_cost:.4f} exceeds budget ${budget:.2f}", file=sys.stderr)
        print("Aborting enrichment pipeline.", file=sys.stderr)
        sys.exit(1)

    print(f"✓ Cost check passed (${total_cost:.4f} <= ${budget:.2f})")
    print()

    # Submit embeddings batch
    print("Step 3/6: Submitting embeddings batch...")
    emb_batch_id = submit.submit_batch(emb_meta["file_path"], "embeddings")
    print()

    # Submit summaries batch
    print("Step 4/6: Submitting summaries batch...")
    sum_batch_id = submit.submit_batch(sum_meta["file_path"], "summaries")
    print()

    # Poll embeddings batch
    print("Step 5/6: Polling embeddings batch...")
    emb_result = poll.poll_batch(emb_batch_id, poll_interval_seconds=60, timeout_hours=26)

    if emb_result["status"] != "completed":
        print(f"ERROR: Embeddings batch did not complete: {emb_result['status']}", file=sys.stderr)
        sys.exit(1)
    print()

    # Poll summaries batch
    print("Polling summaries batch...")
    sum_result = poll.poll_batch(sum_batch_id, poll_interval_seconds=60, timeout_hours=26)

    if sum_result["status"] != "completed":
        print(f"ERROR: Summaries batch did not complete: {sum_result['status']}", file=sys.stderr)
        sys.exit(1)
    print()

    # Merge embeddings
    print("Step 6/6: Merging embeddings to database...")
    emb_stats = merge.merge_embeddings(emb_result["output_file_path"])
    print()

    # Merge summaries
    print("Merging summaries to database...")
    sum_stats = merge.merge_summaries(sum_result["output_file_path"])
    print()

    # Write enrichment report
    os.makedirs("data/output/validation/latest", exist_ok=True)
    report_path = "data/output/validation/latest/enrichment_report.md"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# OpenAI Batch Enrichment Report\n\n")
        f.write(f"**Timestamp**: {dt.datetime.now(dt.timezone.utc).isoformat()}\n\n")
        f.write(f"**Budget**: ${budget:.2f}\n\n")
        f.write(f"**Projected Cost**: ${total_cost:.4f}\n\n")

        f.write("## Embeddings\n\n")
        f.write(f"- **Batch ID**: {emb_batch_id}\n")
        f.write(f"- **Model**: {emb_meta['model']}\n")
        f.write(f"- **Requests**: {emb_meta['request_count']}\n")
        f.write(f"- **Estimated Tokens**: {emb_meta['estimated_tokens']:,}\n")
        f.write(f"- **Projected Cost**: ${emb_meta['projected_cost_usd']:.4f}\n")
        f.write(f"- **Upserted**: {emb_stats['upserted_count']}\n")
        f.write(f"- **Skipped**: {emb_stats['skipped_count']}\n")
        f.write(f"- **Errors**: {emb_stats['error_count']}\n\n")

        f.write("## Summaries\n\n")
        f.write(f"- **Batch ID**: {sum_batch_id}\n")
        f.write(f"- **Model**: {sum_meta['model']}\n")
        f.write(f"- **Requests**: {sum_meta['request_count']}\n")
        f.write(f"- **Estimated Input Tokens**: {sum_meta['estimated_input_tokens']:,}\n")
        f.write(f"- **Estimated Output Tokens**: {sum_meta['estimated_output_tokens']:,}\n")
        f.write(f"- **Projected Cost**: ${sum_meta['projected_cost_usd']:.4f}\n")
        f.write(f"- **Upserted**: {sum_stats['upserted_count']}\n")
        f.write(f"- **Skipped**: {sum_stats['skipped_count']}\n")
        f.write(f"- **Errors**: {sum_stats['error_count']}\n\n")

        f.write("## Summary\n\n")
        f.write(f"- **Total Rows Updated**: {emb_stats['upserted_count'] + sum_stats['upserted_count']}\n")
        f.write(f"- **Total Errors**: {emb_stats['error_count'] + sum_stats['error_count']}\n")

    print(f"✓ Enrichment report written to: {report_path}")

    # Update ROADMAP
    try:
        with open("docs/ROADMAP.md", "a", encoding="utf-8") as f:
            f.write(f"\n### Batch Enrichment Run — {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')}\n\n")
            f.write(f"- **Date**: {dt.datetime.now(dt.timezone.utc).isoformat()}\n")
            f.write(f"- **Embeddings**: {emb_stats['upserted_count']} rows updated (model: {emb_meta['model']})\n")
            f.write(f"- **Summaries**: {sum_stats['upserted_count']} rows updated (model: {sum_meta['model']})\n")
            f.write(f"- **Cost**: ${total_cost:.4f} (projected)\n")
            f.write(f"- **Report**: {report_path}\n\n")

        print(f"✓ ROADMAP updated")
    except Exception as e:
        print(f"WARNING: Failed to update ROADMAP: {e}")

    print("\n=== Enrichment Pipeline Complete ===")


def main():
    parser = argparse.ArgumentParser(description="ASEANForge Policy Tape Ingestion (MVP)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run ingestion and write to DB")
    p_run.add_argument("--since", type=str, required=False, help="YYYY-MM-DD (required for harvest/full modes)")
    p_run.add_argument("--mode", type=str, choices=["harvest", "enrich", "full"], default="full",
                      help="harvest=crawl/scrape only (no LLM); enrich=batch summarize/embed; full=both (legacy)")
    p_run.add_argument("--auto", action="store_true", help="Auto-run full pipeline (for enrich mode)")
    p_run.add_argument("--budget", type=float, help="Override cost budget in USD (for enrich mode)")
    p_run.add_argument("--use-batch-scrape", action="store_true", help="Use Firecrawl batch scrape API when available")
    p_run.add_argument("--batch-size", type=int, default=100, help="URLs per batch scrape (50-200)")
    p_run.add_argument("--limit-per-source", type=int, default=100, help="Max URLs to process per source")
    p_run.add_argument("--authorities", type=str, help="Comma-separated authorities to include (e.g., KOMDIGI)")
    p_run.add_argument("--max-depth", type=int, default=2, help="Max crawl depth")

    p_dry = sub.add_parser("dry-run", help="Run ingestion without DB writes")
    p_dry.add_argument("--since", type=str, required=True, help="YYYY-MM-DD")
    p_dry.add_argument("--mode", type=str, choices=["harvest", "enrich", "full"], default="full",
                      help="harvest=crawl/scrape only (no LLM); enrich=batch summarize/embed; full=both (legacy)")

    p_probe = sub.add_parser("probe-aggressive", help="Aggressive Firecrawl troubleshooting probe")
    p_probe.add_argument("--authorities", type=str, default="IMDA,OJK,MAS,BI,ASEAN")
    p_probe.add_argument("--pages", type=int, default=8)

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    seed = load_seed(); rules = load_rules()

    fc_app = None
    if Firecrawl and os.getenv("FIRECRAWL_API_KEY"):
        try:
            fc_app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))  # type: ignore
            logging.info("Firecrawl v2 client initialized; assuming stealth proxies enabled at account level.")
        except Exception:
            logging.warning("Firecrawl init failed; falling back to urllib")

    if args.cmd == "probe-aggressive":
        auths = [a.strip().upper() for a in (args.authorities or "").split(",") if a.strip()]
        run_aggressive_probe(auths, int(args.pages or 8), seed, fc_app)
        logging.info(json.dumps({"probe_done": True, "authorities": auths}))
        sys.exit(0)

    # Handle enrich mode
    if args.cmd == "run" and getattr(args, "mode", "full") == "enrich":
        if not getattr(args, "auto", False):
            print("ERROR: enrich mode requires --auto flag", file=sys.stderr)
            print("Usage: .venv/bin/python app/ingest.py run --mode enrich --auto", file=sys.stderr)
            sys.exit(1)

        run_enrich_auto(args)
        sys.exit(0)

    # For run/dry-run flows (harvest/full modes), parse since
    if not args.since:
        print("ERROR: --since is required for harvest/full modes", file=sys.stderr)
        sys.exit(2)

    try:
        since = dt.date.fromisoformat(args.since)
    except Exception:
        print("--since must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    start_urls = seed.get("startUrls", [])  # ingest all configured authorities

    oa = openai_client()

    # provider counters
    metrics = {
        "items_fetched": 0,
        "items_new": 0,
        "parse_failures": 0,
        "prov_fc_landing": 0,
        "prov_http_landing": 0,
        "prov_fc_article": 0,
        "prov_http_article": 0,
        "by_auth": {},  # per-authority tallies
        "start": time.time(),
    }

    feeds_override = load_feeds_override()

    # Optional authority filter
    auth_filter = set(a.strip().upper() for a in ((getattr(args, "authorities", "") or "").split(",")) if a.strip())

    for entry in start_urls:
        base = entry.get("url"); label = entry.get("label")
        if not base: continue
        if auth_filter and (label or "").upper() not in auth_filter:
            continue
        try:
            # Feed-first for enabled authorities
            used_feed = False
            fo = feeds_override.get(label) if label else None
            if fo and fo.get("enabled") and fo.get("feed"):
                urls = harvest_feed_urls(label, fo["feed"], limit=5)
                if urls:
                    for url in urls[:5]:
                        try:
                            process_article(oa, label, url, fc_app, since, args.cmd == "dry-run", rules, metrics)
                        except Exception as e:
                            metrics["parse_failures"] += 1
                            logging.exception("Failed %s: %s", url, e)
                    used_feed = True
            if used_feed:
                continue

            # Per-authority metrics bucket
            auth_key = label or base
            am = metrics.setdefault("by_auth", {}).setdefault(auth_key, {"fc_landing": 0, "http_landing": 0, "fc_article": 0, "http_article": 0})

            # Prefer Firecrawl v2 crawl for landing discovery
            links: List[str] = []
            if fc_app:
                try:
                    proxy_mode, wait_ms = resolve_fc_proxy_and_wait_ms(label)
                    max_d = entry.get("maxDepth") or (2 if (label or "").upper() in ("BNM","KOMINFO","KOMDIGI") else 1)
                    crawl_limit = int(getattr(args, "limit_per_source", 100) or 100)
                    links = fc_crawl_links(fc_app, base, limit=crawl_limit, max_depth=max_d, proxy_mode=proxy_mode, wait_ms=int(wait_ms), authority=label)
                    if links:
                        logging.info(f"FETCH_PROVIDER=firecrawl mode=crawl url={base} waitFor={int(wait_ms)} proxy={proxy_mode}")
                        metrics["prov_fc_landing"] += 1
                        am["fc_landing"] += 1
                except Exception:
                    links = []

            # Fallback: scrape landing with Firecrawl then HTTP and extract links
            if not links:
                html = ""
                if fc_app:
                    proxy_mode, wait_ms = resolve_fc_proxy_and_wait_ms(label)
                    landing = fc_fetch(fc_app, base, label, proxy_mode, int(wait_ms))
                    html = landing.get("html") if landing else ""
                    if html:
                        logging.info(f"FETCH_PROVIDER=firecrawl mode=scrape url={base} waitFor={int(wait_ms)} proxy={proxy_mode}")
                        metrics["prov_fc_landing"] += 1
                        am["fc_landing"] += 1
                if not html:
                    logging.info(f"FETCH_PROVIDER=http mode=scrape url={base} waitFor={int(wait_ms)} proxy={proxy_mode}")
                    try:
                        proxy_mode, wait_ms = resolve_fc_proxy_and_wait_ms(label)
                        write_provider_event(label, base, "http", "fallback", int(wait_ms), proxy_mode, "")
                    except Exception:
                        pass
                    metrics["prov_http_landing"] += 1
                    am["http_landing"] += 1
                    data, ct = http_get(base)
                    if ct.startswith("text"):
                        html = data.decode("utf-8", errors="ignore")
                links = discover_links(base, html or "", limit=int(getattr(args, "limit_per_source", 100) or 100))

            # process up to N links per source
            for url in links[:int(getattr(args, "limit_per_source", 100) or 100)]:
                try:
                    mode = getattr(args, "mode", "full")  # Get mode from args, default to "full"
                    process_article(oa, label, url, fc_app, since, args.cmd == "dry-run", rules, metrics, mode)
                except Exception as e:
                    metrics["parse_failures"] += 1
                    logging.exception("Failed %s: %s", url, e)
        except Exception as e:
            logging.warning("Source scan failed %s: %s", label, e)

    duration_ms = int((time.time() - metrics["start"]) * 1000)
    out = {
        "metrics": {k: v for k, v in metrics.items() if k not in ("start", "by_auth")},
        "provider_summary": {
            "landing": {"firecrawl": metrics["prov_fc_landing"], "http": metrics["prov_http_landing"]},
            "articles": {"firecrawl": metrics["prov_fc_article"], "http": metrics["prov_http_article"]},
            "by_authority": metrics.get("by_auth", {}),
        },
        "duration_ms": duration_ms,
    }
    # Write provider usage CSV snapshot
    try:
        out_dir = os.path.join("data", "output", "validation", "latest")
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, "provider_usage.csv")
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["authority", "fc_landing", "http_landing", "fc_article", "http_article"])
            for auth, row in sorted(metrics.get("by_auth", {}).items()):
                w.writerow([auth, row.get("fc_landing", 0), row.get("http_landing", 0), row.get("fc_article", 0), row.get("http_article", 0)])
        logging.info(f"wrote provider usage CSV: {csv_path}")
    except Exception as se:
        logging.warning(f"provider usage CSV write failed: {se}")

    logging.info(json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()

