import os
import argparse
import json
import time
import csv
import re
from urllib.request import urlopen, Request


from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
import yaml

# Firecrawl SDK
from firecrawl import Firecrawl
# Vector store and embeddings (optional; used when not --dry-run)
try:
    from langchain_postgres import PGVector
    from langchain_openai import OpenAIEmbeddings
except Exception:
    PGVector = None
    OpenAIEmbeddings = None


# Reuse existing SQLAlchemy models and utilities
try:
    from scripts.db_models import get_engine_from_env, SessionLocal, Source, Page, Chunk
except Exception:
    from db_models import get_engine_from_env, SessionLocal, Source, Page, Chunk


STATE_PATH = "data/ingest_state.json"
DEFAULT_CONFIG = "config/sources.yaml"

# Heuristic: ~4 chars per token; target 600 tokens -> ~2400 chars, overlap ~100 tokens
DEFAULT_CHUNK_CHARS = int(os.getenv("CHUNK_CHARS", str(600 * 4)))
DEFAULT_OVERLAP_CHARS = int(os.getenv("CHUNK_OVERLAP_CHARS", str(100 * 4)))
# Lower default min length to be more permissive; override via env
MIN_PAGE_CHARS = int(os.getenv("MIN_PAGE_CHARS", "700"))
# Crawling behavior defaults (polite & shallow)
WAIT_MS_DEFAULT = int(os.getenv("FIRECRAWL_WAIT_MS", "2000"))  # ms before parsing
CRAWL_DELAY_MS = int(os.getenv("CRAWL_DELAY_MS", "1200"))

INGEST_DEBUG = os.getenv("INGEST_DEBUG", "0").lower() in ("1","true","yes","y")
SOURCE_FILTER = os.getenv("SOURCE_FILTER")  # comma-separated substrings matched against entry name or url (case-insensitive)


def _load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f)


def chunk_text(text: str, size: int = DEFAULT_CHUNK_CHARS, overlap: int = DEFAULT_OVERLAP_CHARS) -> Iterable[str]:
    if not text:
        return []
    if size <= overlap:
        raise ValueError("chunk size must be > overlap")
    i = 0
    L = len(text)
    while i < L:
        yield text[i : i + size]
        i += (size - overlap)


def load_sources_config(path: str = DEFAULT_CONFIG) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    entries: List[Dict[str, Any]] = []
    for section, items in (data or {}).items():
        for it in items or []:
            it = dict(it)
            it["section"] = section
            entries.append(it)
    # Optional filter via env for targeted debugging
    if SOURCE_FILTER:
        needles = [s.strip().lower() for s in SOURCE_FILTER.split(",") if s.strip()]
        def _match(e: Dict[str, Any]) -> bool:
            name = (e.get("name") or "").lower(); url = (e.get("url") or "").lower()
            return any(n in name or n in url for n in needles)
        entries = [e for e in entries if _match(e)]
    return entries


def authority_from_entry(entry: Dict[str, Any]) -> Optional[str]:
    name = (entry.get("name") or "").upper()
    url = entry.get("url") or ""
    dom = urlparse(url).netloc.lower() if url else ""
    for key, markers in {
        "ASEAN": ["ASEAN", "asean.org"],
        "MAS": ["MAS", "mas.gov.sg"],
        "IMDA": ["IMDA", "imda.gov.sg"],
        "PDPC": ["PDPC", "pdpc.gov.sg"],
        "OJK": ["OJK", "ojk.go.id"],
        "BI": [" BI ", " BI-", "bi.go.id", "BANK INDONESIA"],
        "KOMINFO": ["KOMINFO", "kominfo.go.id"],
        "BOT": ["BOT", "bot.or.th"],
        "BNM": ["BNM", "bnm.gov.my"],
        "SC": [" SC ", "sc.com.my", "SECURITIES COMMISSION MALAYSIA"],
        "MCMC": ["MCMC", "mcmc.gov.my"],
        "BSP": ["BSP", "bsp.gov.ph"],
        "DICT": ["DICT", "dict.gov.ph"],
        "MIC": ["MIC", "mic.gov.vn"],
        "SBV": ["SBV", "sbv.gov.vn"],
    }.items():
        if any(m in name or (dom and m in dom) for m in markers):
            return key.strip()
    return None


def resolve_fc_proxy_and_wait_ms(entry: Dict[str, Any]) -> Tuple[str, int]:
    auth = authority_from_entry(entry) or ""
    # Escalate stubborn authorities per spec
    if auth in ("BNM", "KOMINFO"):
        return ("stealth", 12000)
    # High-security but generally OK with 5s
    if auth in ("ASEAN", "OJK", "MCMC", "DICT"):
        return ("stealth", 5000)
    # defaults for others incl. IMDA, MAS, BI, SC, PDPC, BOT, BSP, SBV, MIC
    return ("auto", WAIT_MS_DEFAULT)




def authority_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    dom = urlparse(url).netloc.lower()
    if "asean.org" in dom:
        return "ASEAN"
    if "mas.gov.sg" in dom:
        return "MAS"
    if "imda.gov.sg" in dom:
        return "IMDA"
    if "ojk.go.id" in dom:
        return "OJK"
    if "bi.go.id" in dom:
        return "BI"
    if "bnm.gov.my" in dom:
        return "BNM"
    if "kominfo.go.id" in dom:
        return "KOMINFO"
    if "sc.com.my" in dom:
        return "SC"
    if "mcmc.gov.my" in dom:
        return "MCMC"
    if "bsp.gov.ph" in dom:
        return "BSP"
    if "dict.gov.ph" in dom:
        return "DICT"
    if "mic.gov.vn" in dom:
        return "MIC"
    if "sbv.gov.vn" in dom:
        return "SBV"
    return None


def write_provider_event(authority: Optional[str], url: str, provider: str, status_code_or_error: str, wait_ms: int, proxy_mode: str, notes: str = "") -> None:
    """Append a provider event to CSV with normalized schema.
    Columns: [authority, url, provider, status_code_or_error, waitFor_ms, proxy_mode, timestamp, notes]
    """
    try:
        out_dir = os.path.join("data", "output", "validation", "latest")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "provider_events.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["authority", "url", "provider", "status_code_or_error", "waitFor_ms", "proxy_mode", "timestamp", "notes"])
            w.writerow([authority or "", url, provider, status_code_or_error, wait_ms, proxy_mode, datetime.utcnow().isoformat(), notes])
    except Exception:
        pass


def write_fc_error(domain: str, url: str, status: str, error_msg: str) -> None:
    """Append a Firecrawl error row for stubborn/empty cases.
    Columns: [domain, url, status, error]
    """
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


def write_quality_drop(authority: Optional[str], url: str, reason: str, metric: str = "") -> None:
    """Log pages skipped by quality gates for reporting.
    Columns: [authority, url, reason, metric]
    """
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


def is_link_farm_markdown(md: str) -> float:
    """Estimate fraction of non-whitespace chars within markdown links and bullets."""
    if not md:
        return 0.0
    nonws = ''.join(ch for ch in md if not ch.isspace())
    if not nonws:
        return 0.0
    # Link text portions inside [] and list markers
    link_texts = re.findall(r"\[([^\]]{1,200})\]\((http[^)]+)\)", md, flags=re.I)
    link_chars = sum(len(t[0]) for t in link_texts)
    bullet_lines = [ln for ln in md.splitlines() if ln.strip().startswith(('-','*','•'))]
    bullet_chars = sum(len(''.join(ch for ch in ln if not ch.isspace())) for ln in bullet_lines)
    return min(1.0, (link_chars + bullet_chars) / max(1, len(nonws)))


def contains_not_found(title: Optional[str], md: str) -> bool:
    hay = ((title or "") + "\n" + (md or "")).lower()
    phrases = ["not found", "404", "page no longer exists", "this page no longer exists"]
    return any(p in hay for p in phrases)

def http_fetch_markdown(url: str, timeout: int = 20) -> Tuple[str, Dict[str, Any]]:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; AseanForge/1.0)"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        text = data.decode("utf-8", errors="ignore")
        # naive tag strip to yield a markdown-like plain text
        stripped = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
        stripped = re.sub(r"<style[\s\S]*?</style>", " ", stripped, flags=re.I)
        stripped = re.sub(r"<[^>]+>", " ", stripped)
        stripped = re.sub(r"\s+", " ", stripped).strip()
        meta = {"provider": "http", "fetched_at": datetime.utcnow().isoformat()}
        return stripped, meta
    except Exception as e:
        if INGEST_DEBUG:
            print(f"      [debug] HTTP fallback failed for {url}: {e}")
        return "", {}


def extract_metadata(item: Dict[str, Any]) -> Tuple[str, str, Optional[str], Optional[datetime]]:
    """Return (url, title, domain, published_at)
    item is one entry from Firecrawl crawl results: {markdown, html?, metadata}
    """
    meta = (item.get("metadata") or {}) if isinstance(item, dict) else {}
    # URL fallbacks: metadata first, then top-level
    url = (
        meta.get("sourceURL") or meta.get("ogUrl") or meta.get("url") or
        (item.get("url") if isinstance(item, dict) else None) or ""
    )
    # Title fallbacks: metadata first, then top-level
    title = (
        meta.get("title") or meta.get("ogTitle") or
        (item.get("title") if isinstance(item, dict) else None) or ""
    )
    domain = urlparse(url).netloc if url else None
    # Best-effort publication date from common fields
    pub_raw = meta.get("date") or meta.get("article:published_time") or meta.get("published_time")
    pub_dt: Optional[datetime] = None
    if pub_raw:
        try:
            # Attempt multiple common formats
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    pub_dt = datetime.strptime(pub_raw, fmt)
                    break
                except Exception:
                    continue
        except Exception:
            pub_dt = None
    return url, title, domain, pub_dt

def ensure_url_and_markdown(fc: Firecrawl, item: Any, page_options: Dict[str, Any], proxy_mode: str) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """Resolve a URL and markdown for a crawl item using Firecrawl v2 first, then HTTP fallback.
    Returns (url, markdown, metadata_dict). metadata may include _fetched_via: fc_scrape|http
    """
    meta: Dict[str, Any] = {}
    url: Optional[str] = None
    markdown: Optional[str] = None
    try:
        if isinstance(item, dict):
            meta = item.get("metadata") or {}
            url = (
                meta.get("sourceURL") or meta.get("ogUrl") or meta.get("url") or
                item.get("url")
            )
            markdown = item.get("markdown") or item.get("content")
        elif isinstance(item, str) and item.startswith("http"):
            url = item
        # If we lack markdown but have a URL, try a direct scrape (v2-first)
        if url and not markdown:
            try:
                try:
                    scrape = fc.scrape(url=url, formats=["markdown", "html"], pageOptions=page_options, parsers=["pdf"], proxy=proxy_mode, maxAge=172800000)
                except TypeError as te:
                    if "pageOptions" in str(te) or "proxy" in str(te) or "parsers" in str(te):
                        scrape = fc.scrape(url, formats=["markdown", "html"])  # legacy
                    else:
                        raise
                data = getattr(scrape, "data", {}) or {}
                markdown = getattr(scrape, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
                md2 = (data.get("metadata") if isinstance(data, dict) else {}) or {}
                meta = {**md2, **meta}
                if markdown:
                    meta["_fetched_via"] = "fc_scrape"
            except Exception as se:
                if INGEST_DEBUG:
                    print(f"      [debug] scrape failed for {url}: {se}")
        # Retry escalation with higher wait/selectors; then HTTP fallback if still no markdown
        if url and not markdown:
            try:
                auth_lbl2 = authority_from_url(url)
                extra_kwargs = {"maxAge": 172800000}
                if auth_lbl2 in ("BNM", "KOMINFO"):
                    extra_kwargs["selectors"] = ["article", ".post-content", ".news-detail"]
                    extra_kwargs["location"] = {"country": "SG", "languages": ["en-SG"]}
                scrape2 = fc.scrape(url=url, formats=["markdown", "html"], pageOptions={**page_options, "waitFor": 12000}, parsers=["pdf"], proxy=proxy_mode, **extra_kwargs)
                data2 = getattr(scrape2, "data", {}) or {}
                md2 = getattr(scrape2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
                if md2:
                    markdown = md2
                    meta["_fetched_via"] = "fc_scrape"
            except TypeError:
                try:
                    scrape2 = fc.scrape(url=url, formats=["markdown", "html"], pageOptions={**page_options, "waitFor": 12000}, parsers=["pdf"], proxy=proxy_mode, maxAge=172800000)
                    data2 = getattr(scrape2, "data", {}) or {}
                    md2 = getattr(scrape2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
                    if md2:
                        markdown = md2
                        meta["_fetched_via"] = "fc_scrape"
                except Exception as e:
                    write_fc_error(urlparse(url).netloc, url, "scrape_retry_error", str(e))
            except Exception as e:
                write_fc_error(urlparse(url).netloc, url, "scrape_error", str(e))
        if url and not markdown:
            md_http, md_meta = http_fetch_markdown(url)
            if md_http:
                markdown = md_http
                meta = {**meta, **md_meta}
                meta["_fetched_via"] = "http"
    except Exception as e:
        if INGEST_DEBUG:
            print(f"      [debug] ensure_url_and_markdown error: {e}")
    return url, markdown, (meta or {})


def ensure_session():
    engine = get_engine_from_env()
    if engine is None:
        return None
    SessionLocal.configure(bind=engine)
    return SessionLocal()


def ingest_from_crawl_item(session, url: str, title: str, domain: Optional[str], markdown: str, published_at: Optional[datetime]) -> Tuple[int, int]:
    """Persist Source/Page/Chunk with dedup by (url, content_hash). Returns (pages_added, chunks_added)."""
    import hashlib

    pages_added = 0
    chunks_added = 0

    if not markdown:
        return (0, 0)

    content_hash = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
    base_url = None
    try:
        p = urlparse(url)
        base_url = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else None
    except Exception:
        pass

    # upsert-like Source by domain
    src = None
    if domain:
        src = session.query(Source).filter_by(domain=domain).first()
        if src is None:
            src = Source(domain=domain, base_url=base_url, discovery_method="firecrawl")
            session.add(src)
            session.flush()

    # unique by url+hash
    page = session.query(Page).filter_by(url=url, content_hash=content_hash).first()
    if page is None:
        page = Page(
            source_id=(src.id if src else None),
            url=url,
            title=(title or None),
            content_hash=content_hash,
            fetched_at=datetime.utcnow(),
            token_estimate=None,
            from_cache=False,
        )
        session.add(page)
        session.flush()
        pages_added += 1

    # chunks
    chunks = list(chunk_text(markdown))
    for i, t in enumerate(chunks):
        ch = session.query(Chunk).filter_by(page_id=page.id, chunk_index=i).first()
        if ch is None:
            ch = Chunk(
                page_id=page.id,
                chunk_index=i,
                text_len=len(t),
                token_estimate=len(t) // 4,
                embedding_model=None,
                meta={"url": url, "title": title, "content_hash": content_hash, "chunk_index": i},
            )
            session.add(ch)
            chunks_added += 1

    session.commit()
    return pages_added, chunks_added


def run_ingest(config_path: str, dry_run: bool = False, limit_per_source: int = 10, max_depth: int = 1, pdf_only: bool = False) -> None:
    load_dotenv(override=True)
    entries = load_sources_config(config_path)
    # Prepare vector store for embeddings if available and not a dry run
    vs = None
    if not dry_run and PGVector is not None and OpenAIEmbeddings is not None:
        try:
            conn = os.getenv("NEON_DATABASE_URL")
            if conn and conn.startswith("postgresql://"):
                conn = conn.replace("postgresql://", "postgresql+psycopg://", 1)
            coll = os.getenv("COLLECTION_NAME", "asean_docs")
            vs = PGVector(
                embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
                collection_name=coll,
                connection=conn,
                use_jsonb=True,
            )
        except Exception as e:
            print(f"[warn] Embedding store unavailable; skipping vector writes: {e}")

    fc = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))

    state = _load_state()
    session = None if dry_run else ensure_session()

    total_pages = 0
    total_chunks = 0

    # Per-source stats for summary output
    ingest_started = datetime.utcnow().isoformat()
    per_source: Dict[str, Dict[str, Any]] = {}

    for entry in entries:
        base = entry.get("url")
        name = entry.get("name")
        section = entry.get("section")
        lp = int(entry.get("limit", limit_per_source) or limit_per_source)
        mdp = int(entry.get("max_depth", max_depth) or max_depth)
        # Force deeper crawl for stubborn authorities
        try:
            auth_lbl = authority_from_entry(entry) or authority_from_url(base)
            if auth_lbl in ("BNM", "KOMINFO"):
                mdp = max(mdp, 2)
        except Exception:
            pass
        key = base or name or f"{section}:{lp}:{mdp}"
        if key not in per_source:
            per_source[key] = {
                "name": name,
                "authority": authority_from_entry(entry) or authority_from_url(base),
                "url": base,
                "section": section,
                "limit_used": lp,
                "max_depth_used": mdp,
                "pages_considered": 0,
                "pages_accepted": 0,
                "snippets_total": 0,
                "db_pages_inserted": 0,
                "db_chunks_inserted": 0,
                # provider tallies for diagnostics
                "provider_fc_crawl": 0,
                "provider_fc_scrape": 0,
                "provider_http": 0,
            }
        proxy_mode, wait_ms = resolve_fc_proxy_and_wait_ms(entry)
        page_options = {"waitFor": wait_ms, "timeout": 60000, "includeHtml": True, "parsePDF": True, "onlyMainContent": True}
        try:
            if (authority_from_entry(entry) or authority_from_url(base)) in ("BNM", "KOMINFO"):
                page_options["selectors"] = ["article", ".post-content", ".news__item", ".entry-content", ".press-release"]
        except Exception:
            pass

        print(f"[{datetime.utcnow().isoformat()}] Crawl: {name} ({section}) {base} limit={lp} depth={mdp}")
        try:
            # Firecrawl v2 crawl preferred; fall back to legacy signatures if needed
            api_path = "v2"
            try:
                docs = fc.crawl(
                    url=base,
                    limit=lp,
                    pageOptions=page_options,
                    proxy=proxy_mode,
                    poll_interval=1,
                    timeout=120,
                    maxAge=172800000,
                )
            except TypeError as te:
                if "pageOptions" in str(te):
                    try:
                        # legacy simpler signature
                        docs = fc.crawl(base, limit=lp)
                        api_path = "legacy"
                    except Exception:
                        # final fallback: minimal kwargs without pageOptions
                        docs = fc.crawl(url=base, limit=lp)
                        api_path = "legacy"
                else:
                    # Some SDK builds accept a dict payload
                    docs = fc.crawl({"url": base, "limit": lp})
                    api_path = "legacy"
            # SDK: may return dict with data, or object with .data
            # polite delay between API calls
            try:
                time.sleep(CRAWL_DELAY_MS / 1000.0)
            except Exception:
                pass

            items = []
            if isinstance(docs, dict) and "data" in docs:
                items = docs.get("data") or []
            elif hasattr(docs, "data"):
                items = getattr(docs, "data") or []
            elif isinstance(docs, list):
                items = docs
            else:
                items = []
            if not items:
                # Escalate retry for BNM/KOMINFO
                auth_lbl = authority_from_entry(entry) or authority_from_url(base)
                if auth_lbl in ("BNM", "KOMINFO"):
                    try:
                        docs2 = fc.crawl(url=base, limit=lp, pageOptions={**page_options, "waitFor": 12000}, proxy=proxy_mode, poll_interval=1, timeout=120, maxAge=172800000, location={"country": "SG", "languages": ["en-SG"]})
                        if hasattr(docs2, "data"):
                            items = getattr(docs2, "data") or []
                        elif isinstance(docs2, dict):
                            items = docs2.get("data") or []
                    except TypeError:
                        try:
                            docs2 = fc.crawl(url=base, limit=lp, pageOptions={**page_options, "waitFor": 12000}, proxy=proxy_mode, poll_interval=1, timeout=120, maxAge=172800000)
                            if hasattr(docs2, "data"):
                                items = getattr(docs2, "data") or []
                            elif isinstance(docs2, dict):
                                items = docs2.get("data") or []
                        except Exception as e:
                            write_fc_error(urlparse(base).netloc, base, "crawl_retry_error", str(e))
                    except Exception as e:
                        write_fc_error(urlparse(base).netloc, base, "crawl_error", str(e))
            if items:
                per_source[key]["provider_fc_crawl"] += 1
                print(f"    FETCH_PROVIDER=firecrawl mode=crawl url={base} waitFor={page_options.get('waitFor')} proxy={proxy_mode}")
                try:
                    write_provider_event(authority_from_entry(entry) or authority_from_url(base), base, "firecrawl", "ok" if items else "empty", page_options.get("waitFor", 0), proxy_mode, api_path)
                except Exception:
                    pass


            # Always attempt to include a direct scrape of the base URL as the first candidate
            if base:
                try:
                    notes = "v2"
                    try:
                        s = fc.scrape(
                            url=base,
                            formats=["markdown", "html"],
                            pageOptions=page_options,
                            parsers=["pdf"],
                            proxy=proxy_mode,
                            maxAge=172800000,
                        )
                    except TypeError as te:
                        if "pageOptions" in str(te) or "proxy" in str(te) or "parsers" in str(te):
                            s = fc.scrape(base, formats=["markdown", "html"])  # legacy
                            notes = "legacy"
                        else:
                            raise
                    data = getattr(s, "data", {}) or {}
                    md = getattr(s, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
                    meta_b = (data.get("metadata") if isinstance(data, dict) else {}) or {}
                    if md:
                        base_item = {"markdown": md, "metadata": meta_b, "url": base}
                        items = [base_item] + (items or [])
                        per_source[key]["provider_fc_scrape"] += 1
                        print(f"    FETCH_PROVIDER=firecrawl mode=scrape url={base} waitFor={page_options.get('waitFor')} proxy={proxy_mode}")
                        try:
                            write_provider_event(authority_from_entry(entry) or authority_from_url(base), base, "firecrawl", "ok", page_options.get("waitFor", 0), proxy_mode, notes)
                        except Exception:
                            pass
                    else:
                        # Retry with escalation if empty
                        auth_lbl = authority_from_entry(entry) or authority_from_url(base)
                        md = ""
                        try:
                            # Retry with higher wait and SG locale; add minimal selectors for BNM/KOMINFO
                            extra_kwargs = {"maxAge": 172800000}
                            if auth_lbl in ("BNM", "KOMINFO"):
                                extra_kwargs["selectors"] = ["article", ".post-content", ".news-detail"]
                                extra_kwargs["location"] = {"country": "SG", "languages": ["en-SG"]}
                            s2 = fc.scrape(url=base, formats=["markdown", "html"], pageOptions={**page_options, "waitFor": 12000}, parsers=["pdf"], proxy=proxy_mode, **extra_kwargs)
                            data2 = getattr(s2, "data", {}) or {}
                            md = getattr(s2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
                        except TypeError:
                            # SDK may not accept selectors/location; try without them
                            try:
                                s2 = fc.scrape(url=base, formats=["markdown", "html"], pageOptions={**page_options, "waitFor": 12000}, parsers=["pdf"], proxy=proxy_mode, maxAge=172800000)
                                data2 = getattr(s2, "data", {}) or {}
                                md = getattr(s2, "markdown", "") or (data2.get("markdown", "") if isinstance(data2, dict) else "")
                            except Exception:
                                md = ""
                        except Exception:
                            md = ""
                        if md:
                            base_item = {"markdown": md, "metadata": {}, "url": base}
                            items = [base_item] + (items or [])
                            per_source[key]["provider_fc_scrape"] += 1
                            print(f"    FETCH_PROVIDER=firecrawl mode=scrape url={base} waitFor=12000 proxy={proxy_mode}")
                            try:
                                write_provider_event(authority_from_entry(entry) or authority_from_url(base), base, "firecrawl", "ok", 12000, proxy_mode, "retry")
                            except Exception:
                                pass
                        else:
                            # HTTP fallback for base if Firecrawl returns empty
                            md_http, _ = http_fetch_markdown(base)
                            if md_http:
                                base_item = {"markdown": md_http, "metadata": {}, "url": base}
                                items = [base_item] + (items or [])
                                per_source[key]["provider_http"] += 1
                                print(f"    FETCH_PROVIDER=http mode=scrape url={base} waitFor={page_options.get('waitFor')} proxy={proxy_mode}")
                                try:
                                    write_provider_event(authority_from_entry(entry) or authority_from_url(base), base, "http", "fallback", page_options.get("waitFor", 0), proxy_mode, "")
                                except Exception:
                                    pass
                    # polite delay
                    try:
                        time.sleep(CRAWL_DELAY_MS / 1000.0)
                    except Exception:
                        pass
                except Exception as se:
                    if INGEST_DEBUG or dry_run:
                        print(f"    [debug] base scrape failed: {se}")

            for it in items:
                per_source[key]["pages_considered"] += 1
                # Resolve URL and markdown; attempt a direct scrape if crawl item lacks content
                url0, markdown, meta = ensure_url_and_markdown(fc, it, page_options, proxy_mode)
                edict = it if isinstance(it, dict) else {}
                if isinstance(edict, dict):
                    em = dict(edict.get("metadata") or {})
                    m = dict(meta or {})
                    combined_meta = {**m, **em}
                    edict["metadata"] = combined_meta
                    if markdown and not edict.get("markdown"):
                        edict["markdown"] = markdown
                    if url0 and not edict.get("url"):
                        edict["url"] = url0
                # Provider attribution for fallback fetches
                via = (meta or {}).get("_fetched_via")
                if via == "fc_scrape" and url0:
                    per_source[key]["provider_fc_scrape"] += 1
                    print(f"    FETCH_PROVIDER=firecrawl mode=scrape url={url0} waitFor={page_options.get('waitFor')} proxy={proxy_mode}")
                    try:
                        write_provider_event(authority_from_entry(entry) or authority_from_url(url0), url0, "firecrawl", "ok", page_options.get("waitFor", 0), proxy_mode, "")
                    except Exception:
                        pass
                elif via == "http" and url0:
                    per_source[key]["provider_http"] += 1
                    print(f"    FETCH_PROVIDER=http mode=scrape url={url0} waitFor={page_options.get('waitFor')} proxy={proxy_mode}")
                    try:
                        write_provider_event(authority_from_entry(entry) or authority_from_url(url0), url0, "http", "fallback", page_options.get("waitFor", 0), proxy_mode, "")
                    except Exception:
                        pass
                meta_url, meta_title, domain, published_at = extract_metadata(edict)

                # Diagnostics
                # Normalize markdown to string for safe diagnostics
                markdown = markdown or ""
                if INGEST_DEBUG or dry_run:
                    keys = list(edict.keys()) if isinstance(edict, dict) else []
                    mlen = len(markdown)
                    snippet = (markdown[:240].replace("\n"," ") + ("..." if mlen > 240 else "")) if markdown else ""
                    print(f"    item keys={keys}")
                    print(f"    meta keys={list((edict.get('metadata') or {}).keys())}")
                    print(f"    markdown_len={mlen}")
                    if snippet:
                        print(f"    markdown_snippet='{snippet}'")
                    print(f"    extracted url={meta_url} title={meta_title} domain={domain} published_at={published_at}")

                # Optional PDF-only mode: skip non-PDF URLs early
                if pdf_only:
                    check_url = (meta_url or url0 or "").split("?")[0].lower()
                    if not check_url.endswith(".pdf"):
                        try:
                            write_quality_drop(auth_lbl2 if 'auth_lbl2' in locals() else (authority_from_entry(entry) or authority_from_url(meta_url or url0 or "")), meta_url or (url0 or ""), "not_pdf", metric="0")
                        except Exception:
                            pass
                        if INGEST_DEBUG or dry_run:
                            print("    decision=SKIP reasons=['not_pdf']")
                        continue

                reasons = []
                if not markdown:
                    reasons.append("no_markdown")
                if len(markdown) < MIN_PAGE_CHARS:
                    reasons.append(f"too_short(<{MIN_PAGE_CHARS})")
                if not meta_url:
                    reasons.append("no_url")
                # 404/Not Found filter
                if contains_not_found(meta_title, markdown):
                    reasons.append("not_found")
                # Link farm filter
                lf = is_link_farm_markdown(markdown)
                if lf > 0.65:
                    reasons.append(f"link_farm({lf:.2f})")
                # Language filter: apply to English-expected authorities
                auth_lbl2 = authority_from_entry(entry) or authority_from_url(meta_url)
                if auth_lbl2 in ("ASEAN","MAS","IMDA","PDPC","SC","BNM","BOT","BSP","DICT","SBV","MIC"):
                    ar = ascii_ratio(markdown)
                    if ar < 0.60:
                        reasons.append(f"non_english({ar:.2f})")

                if reasons:
                    try:
                        write_quality_drop(auth_lbl2, meta_url or (url0 or ""), ";".join(reasons), metric=str(len(markdown)))
                    except Exception:
                        pass
                    if INGEST_DEBUG or dry_run:
                        print(f"    decision=SKIP reasons={reasons}")
                    continue

                snippets = list(chunk_text(markdown))
                snippet_count = len(snippets)
                per_source[key]["pages_accepted"] += 1
                per_source[key]["snippets_total"] += snippet_count
                print(f"  - {meta_title or meta_url} | {domain} | snippets {snippet_count}")

                # Add accepted chunks to vector store (embeddings) if available

                if vs is not None:
                    try:
                        metadatas = [{
                            "url": meta_url,
                            "title": meta_title,
                            "source": name or "",
                            "section": section or "",
                            "chunk_index": i,
                        } for i in range(snippet_count)]
                        vs.add_texts(snippets, metadatas=metadatas)
                    except Exception as ve:
                        print(f"[warn] vector add failed for {meta_url}: {ve}")


                if dry_run:
                    if INGEST_DEBUG:
                        print(f"    decision=ACCEPT would_insert: url={meta_url} title={meta_title} domain={domain} snippet_count={snippet_count}")
                    continue

                if session is None:
                    raise SystemExit("No DB session available; set NEON_DATABASE_URL or use --dry-run")

                pages_added, chunks_added = ingest_from_crawl_item(
                    session=session,
                    url=meta_url,
                    title=meta_title,
                    domain=domain,
                    markdown=markdown,
                    published_at=published_at,
                )
                total_pages += pages_added
                total_chunks += chunks_added
                per_source[key]["db_pages_inserted"] += pages_added
                per_source[key]["db_chunks_inserted"] += chunks_added
        except Exception as e:
            print(f"[warn] {base}: {e}")

    done_msg = (
        f"[{datetime.utcnow().isoformat()}] Ingest done. pages_added={total_pages} chunks_added={total_chunks} items_new={total_pages} dry_run={dry_run}"
    )
    print(done_msg)

    # Write human-readable JSON summary for this run
    try:
        os.makedirs("data/output", exist_ok=True)
        summary = {
            "ingestion_started": ingest_started,
            "ingestion_completed": datetime.utcnow().isoformat(),
            "config_path": config_path,
            "dry_run": dry_run,
            "totals": {"db_pages_inserted": total_pages, "db_chunks_inserted": total_chunks},
            "sources": list(per_source.values()),
        }
        with open("data/output/ingestion_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print("Wrote summary: data/output/ingestion_summary.json")
        # Also write a provider-usage CSV snapshot for validation
        try:
            out_dir = os.path.join("data", "output", "validation", "latest")
            os.makedirs(out_dir, exist_ok=True)
            csv_path = os.path.join(out_dir, "provider_usage_sources.csv")
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow(["authority", "url", "fc_crawl_count", "fc_scrape_count", "http_fallback_count"])
                for s in per_source.values():
                    w.writerow([
                        s.get("authority") or "", s.get("url"),
                        s.get("provider_fc_crawl", 0), s.get("provider_fc_scrape", 0), s.get("provider_http", 0)
                    ])
            print(f"Wrote provider usage CSV: {csv_path}")
        except Exception as se:
            print(f"[warn] failed to write provider usage CSV: {se}")

    except Exception as se:
        print(f"[warn] failed to write ingestion summary: {se}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Minimal ingestion pipeline using Firecrawl and SQLAlchemy models")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Path to config/sources.yaml")
    ap.add_argument("--dry-run", action="store_true", help="Fetch and parse only; do not write to DB")
    ap.add_argument("--limit-per-source", type=int, default=10, help="Max pages per source per run")
    ap.add_argument("--max-depth", type=int, default=1, help="Max crawl depth (hint to crawler; may be ignored)")
    ap.add_argument("--pdf-only", action="store_true", help="Filter to PDF documents only (by URL extension)")
    args = ap.parse_args()
    run_ingest(config_path=args.config, dry_run=args.dry_run, limit_per_source=args.limit_per_source, max_depth=args.max_depth, pdf_only=args.pdf_only)

