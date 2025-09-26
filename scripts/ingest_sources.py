import os
import argparse
import json
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
MIN_PAGE_CHARS = int(os.getenv("MIN_PAGE_CHARS", "100"))
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

def ensure_url_and_markdown(fc: Firecrawl, item: Any) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """Try to resolve a URL and markdown for a crawl item.
    Returns (url, markdown, metadata_dict)."""
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
        # If we lack markdown but have a URL, try a direct scrape
        if url and not markdown:
            try:
                scrape = fc.scrape(url=url, formats=["markdown"])
                # firecrawl-py may return an object with attributes
                data = getattr(scrape, "data", {}) or {}
                markdown = getattr(scrape, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
                md2 = (data.get("metadata") if isinstance(data, dict) else {}) or {}
                meta = {**md2, **meta}
            except Exception as se:
                if INGEST_DEBUG:
                    print(f"      [debug] scrape failed for {url}: {se}")
    except Exception:
        pass
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


def run_ingest(config_path: str, dry_run: bool = False, limit_per_source: int = 10, max_depth: int = 1) -> None:
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
        key = base or name or f"{section}:{lp}:{mdp}"
        if key not in per_source:
            per_source[key] = {
                "name": name,
                "url": base,
                "section": section,
                "limit_used": lp,
                "max_depth_used": mdp,
                "pages_considered": 0,
                "pages_accepted": 0,
                "snippets_total": 0,
                "db_pages_inserted": 0,
                "db_chunks_inserted": 0,
            }
        print(f"[{datetime.utcnow().isoformat()}] Crawl: {name} ({section}) {base} limit={lp} depth={mdp}")
        try:
            docs = fc.crawl(url=base, limit=lp)
            # SDK: may return dict with data, or object with .data
            items = []
            if isinstance(docs, dict) and "data" in docs:
                items = docs.get("data") or []
            elif hasattr(docs, "data"):
                items = getattr(docs, "data") or []
            elif isinstance(docs, list):
                items = docs
            else:
                items = []

            # Always attempt to include a direct scrape of the base URL as the first candidate
            if base:
                try:
                    s = fc.scrape(url=base, formats=["markdown"])
                    data = getattr(s, "data", {}) or {}
                    md = getattr(s, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
                    meta_b = (data.get("metadata") if isinstance(data, dict) else {}) or {}
                    if md:
                        base_item = {"markdown": md, "metadata": meta_b, "url": base}
                        items = [base_item] + (items or [])
                        if INGEST_DEBUG or dry_run:
                            print("    [debug] prepended direct scrape of base URL")
                except Exception as se:
                    if INGEST_DEBUG or dry_run:
                        print(f"    [debug] base scrape failed: {se}")

            for it in items:
                per_source[key]["pages_considered"] += 1
                # Resolve URL and markdown; attempt a direct scrape if crawl item lacks content
                url0, markdown, meta = ensure_url_and_markdown(fc, it)
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

                reasons = []
                if not markdown:
                    reasons.append("no_markdown")
                if len(markdown) < MIN_PAGE_CHARS:
                    reasons.append(f"too_short(<{MIN_PAGE_CHARS})")
                if not meta_url:
                    reasons.append("no_url")

                if reasons:
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
        f"[{datetime.utcnow().isoformat()}] Ingest done. pages_added={total_pages} chunks_added={total_chunks} dry_run={dry_run}"
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
    except Exception as se:
        print(f"[warn] failed to write ingestion summary: {se}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Minimal ingestion pipeline using Firecrawl and SQLAlchemy models")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Path to config/sources.yaml")
    ap.add_argument("--dry-run", action="store_true", help="Fetch and parse only; do not write to DB")
    ap.add_argument("--limit-per-source", type=int, default=10, help="Max pages per source per run")
    ap.add_argument("--max-depth", type=int, default=1, help="Max crawl depth (hint to crawler; may be ignored)")
    args = ap.parse_args()
    run_ingest(config_path=args.config, dry_run=args.dry_run, limit_per_source=args.limit_per_source, max_depth=args.max_depth)

