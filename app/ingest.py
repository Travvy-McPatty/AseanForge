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

import csv

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
            "waitUntil": "networkidle",
            "timeout": 60000,
        })
        return {
            "html": result.get("html") or "",
            "text": result.get("text") or "",
            "title": result.get("title") or "",
        }
    except Exception:
        return None

def fc_fetch_with_params(app_obj, url: str, params: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Firecrawl fetch with explicit params; returns (page, error_note). Tries multiple SDK method names."""
    if not app_obj:
        return None, "no_firecrawl_app"
    last_err: Optional[str] = None
    for meth_name in ("scrape_url", "scrapeUrl", "scrape"):
        try:
            meth = getattr(app_obj, meth_name, None)
            if not meth:
                continue
            try:
                result = meth(url, params=params)
            except TypeError:
                # Some SDKs accept a single dict payload
                result = meth({"url": url, **params})
            page = {
                "html": (result.get("html") if isinstance(result, dict) else "") or "",
                "text": (result.get("text") if isinstance(result, dict) else "") or "",
                "title": (result.get("title") if isinstance(result, dict) else "") or "",
            }
            if page["html"] or page["text"]:
                return page, None
        except Exception as e:
            last_err = f"exc={e.__class__.__name__}:{str(e)[:200]}"
    return None, last_err or "unknown_error"

# ---------------- Processing ----------------

def process_article(oa: OpenAI, authority: str, url: str, fc_app, since_date: dt.date, dry_run: bool, rules: Dict, metrics: Dict):
    html_page = ""; text = ""; title = ""; content_type = "html"
    page = fc_fetch(fc_app, url)
    if page:
        html_page = page.get("html", ""); text = page.get("text", ""); title = page.get("title", "")
        logging.info(f"FETCH_PROVIDER=firecrawl url={url}")
        if metrics is not None:
            metrics["prov_fc_article"] = metrics.get("prov_fc_article", 0) + 1

    if not text:
        logging.info(f"FETCH_PROVIDER=http url={url}")
        if metrics is not None:
            metrics["prov_http_article"] = metrics.get("prov_http_article", 0) + 1
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
                        logging.info(f"FETCH_PROVIDER=firecrawl url={base}")
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
                            logging.info(f"FETCH_PROVIDER=firecrawl url={url}")
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


def main():
    parser = argparse.ArgumentParser(description="ASEANForge Policy Tape Ingestion (MVP)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run ingestion and write to DB")
    p_run.add_argument("--since", type=str, required=True, help="YYYY-MM-DD")

    p_dry = sub.add_parser("dry-run", help="Run ingestion without DB writes")
    p_dry.add_argument("--since", type=str, required=True, help="YYYY-MM-DD")

    p_probe = sub.add_parser("probe-aggressive", help="Aggressive Firecrawl troubleshooting probe")
    p_probe.add_argument("--authorities", type=str, default="IMDA,OJK,MAS,BI,ASEAN")
    p_probe.add_argument("--pages", type=int, default=8)

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    seed = load_seed(); rules = load_rules()

    fc_app = None
    if FirecrawlApp and os.getenv("FIRECRAWL_API_KEY"):
        try:
            fc_app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY"))  # type: ignore
        except Exception:
            logging.warning("Firecrawl init failed; falling back to urllib")

    if args.cmd == "probe-aggressive":
        auths = [a.strip().upper() for a in (args.authorities or "").split(",") if a.strip()]
        run_aggressive_probe(auths, int(args.pages or 8), seed, fc_app)
        logging.info(json.dumps({"probe_done": True, "authorities": auths}))
        sys.exit(0)

    # For run/dry-run flows, parse since
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
        "start": time.time(),
    }

    feeds_override = load_feeds_override()

    for entry in start_urls:
        base = entry.get("url"); label = entry.get("label")
        if not base: continue
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

            landing = fc_fetch(fc_app, base) if fc_app else None
            html = landing.get("html") if landing else ""
            if landing and html:
                logging.info(f"FETCH_PROVIDER=firecrawl url={base}")
                metrics["prov_fc_landing"] += 1

            if not html:
                logging.info(f"FETCH_PROVIDER=http url={base}")
                metrics["prov_http_landing"] += 1
                data, ct = http_get(base)
                if ct.startswith("text"):
                    html = data.decode("utf-8", errors="ignore")
            links = discover_links(base, html, limit=8)
            # process up to 5 links per source
            for url in links[:5]:
                try:
                    process_article(oa, label, url, fc_app, since, args.cmd == "dry-run", rules, metrics)
                except Exception as e:
                    metrics["parse_failures"] += 1
                    logging.exception("Failed %s: %s", url, e)
        except Exception as e:
            logging.warning("Source scan failed %s: %s", label, e)

    duration_ms = int((time.time() - metrics["start"]) * 1000)
    out = {
        "metrics": {k: v for k, v in metrics.items() if k != "start"},
        "provider_summary": {
            "landing": {"firecrawl": metrics["prov_fc_landing"], "http": metrics["prov_http_landing"]},
            "articles": {"firecrawl": metrics["prov_fc_article"], "http": metrics["prov_http_article"]},
        },
        "duration_ms": duration_ms,
    }
    logging.info(json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()

