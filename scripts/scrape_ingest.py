import os, argparse, json, hashlib, time
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from firecrawl import Firecrawl
from langchain_postgres import PGVector
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.exc import SQLAlchemyError

try:
    from usage_tracker import TokenTracker
except Exception:
    class TokenTracker:
        def __init__(self, run_id: str):
            pass
        def record(self, *a, **k):
            pass
        def total_input(self):
            return 0
        def total_output(self):
            return 0
        def total_cost_usd(self):
            return 0.0
        def json_line(self):
            return "{}"
try:
    from scripts.db_models import get_engine_from_env, SessionLocal, Source, Page, Chunk
except Exception:
    from db_models import get_engine_from_env, SessionLocal, Source, Page, Chunk

SEED_URLS = [
    "https://www.techinasia.com/",
    "https://www.dealstreetasia.com/",
    "https://www.nikkei.com/asia/technology/",
    "https://www.imda.gov.sg/resources/press-releases-factsheets-and-speeches",
    "https://www.mas.gov.sg/news",
    "https://www.kominfo.go.id/",
    "https://mdec.my/resources/news/",
    "https://vietnamnet.vn/en/technology",
    "https://www.digital.go.th/en/",
    "https://www.channelnewsasia.com/business/technology",
]

CACHE_PATH = "data/ingest_cache.json"
MIN_PAGE_CHARS = int(os.getenv("MIN_PAGE_CHARS", "300"))

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f)

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def chunk(text, size=1200, overlap=200):
    for i in range(0, max(len(text), 1), size - overlap):
        yield text[i:i+size]


def main(limit: int, refresh: bool, track_metadata: bool = True):
    load_dotenv(override=True)
    fc = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    conn = os.getenv("NEON_DATABASE_URL")
    ts = int(time.time())
    tracker = TokenTracker(run_id=str(ts))

    if conn and conn.startswith("postgresql://"):
        conn = conn.replace("postgresql://", "postgresql+psycopg://", 1)
    coll = os.getenv("COLLECTION_NAME", "asean_docs")
    vs = PGVector(embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
                  collection_name=coll, connection=conn, use_jsonb=True)
    pages_inserted = 0
    chunks_inserted = 0


    # Optional DB tracking
    session = None
    if track_metadata:
        try:
            engine = get_engine_from_env()
            if engine is not None:
                SessionLocal.configure(bind=engine)
                session = SessionLocal()
        except Exception as e:
            print(f"[warn] DB tracking disabled due to error: {e}")
            session = None

    cache = load_cache()
    urls = SEED_URLS[:limit] if limit else SEED_URLS

    for url in urls:
        try:
            cached = cache.get(url) or {}
            md = None
            if cached and not refresh and cached.get("markdown"):
                # Use cached content to avoid network re-scrape
                md = cached.get("markdown")
                title = cached.get("title", "")
                content_hash = cached.get("hash")
                from_cache = True
            else:
                doc = fc.scrape(url, formats=["markdown"])  # network call
                data = getattr(doc, "data", {}) or {}
                md = getattr(doc, "markdown", "") or (data.get("markdown", "") if isinstance(data, dict) else "")
                meta_raw = (data.get("metadata") if isinstance(data, dict) else {}) or {}
                title = meta_raw.get("title", "")
                content_hash = sha256_text(md) if md else None
                from_cache = False

            if not md or len(md) < MIN_PAGE_CHARS:
                print(f"Skip (empty/short): {url} chars={len(md) if md else 0}")
                # Update cache minimally to record skip decision without ingest
                cache[url] = {
                    "hash": content_hash or "",
                    "title": title,
                    "markdown": md or "",
                    "ts": int(time.time()),
                }


            # Idempotency: if we have same URL + content hash, skip re-ingestion
            if cached and not refresh and cached.get("hash") == content_hash:
                print(f"Skip (unchanged, cached): {url}")
                continue

            texts = list(chunk(md))
            # Estimate embedding input tokens (~4 chars per token heuristic)
            est_tokens = sum(len(t) // 4 for t in texts)
            tracker.record("text-embedding-3-small", "content_processing", input_tokens=est_tokens, output_tokens=0)

            if texts:
                # Attach metadata per chunk (URL + content hash + chunk index)
                metadatas = [{
                    "url": url,
                    "title": title,
                    "source": "firecrawl",
                    "content_hash": content_hash,
                    "chunk_index": i,
                } for i in range(len(texts))]
                vs.add_texts(texts, metadatas=metadatas)

            # Persist metadata (optional)
            if session is not None:
                try:
                    parsed = urlparse(url)
                    domain = parsed.netloc
                    base_url = (parsed.scheme + "://" + parsed.netloc) if parsed.scheme and parsed.netloc else None
                    # upsert source by domain
                    src = session.query(Source).filter_by(domain=domain).first()
                    if src is None:
                        src = Source(domain=domain, base_url=base_url, discovery_method="firecrawl")
                        session.add(src)
                        session.flush()

                    # insert page (unique by url+hash)
                    page = session.query(Page).filter_by(url=url, content_hash=content_hash).first()
                    if page is None:
                        page = Page(source_id=src.id, url=url, title=title or None,
                                    content_hash=content_hash or "", fetched_at=datetime.utcnow(),
                                    token_estimate=est_tokens, from_cache=from_cache)
                        session.add(page)
                        session.flush()
                        pages_inserted += 1
                    # insert chunks
                    for i, t in enumerate(texts):
                        ch = session.query(Chunk).filter_by(page_id=page.id, chunk_index=i).first()
                        if ch is None:
                            ch = Chunk(page_id=page.id, chunk_index=i, text_len=len(t),
                                       token_estimate=len(t)//4, embedding_model="text-embedding-3-small",
                                       meta={"url": url, "title": title, "content_hash": content_hash, "chunk_index": i})
                            session.add(ch)
                            chunks_inserted += 1
                    session.commit()
                except SQLAlchemyError as dbe:
                    session.rollback()
                    print(f"[warn] DB write failed for {url}: {dbe}")


            cache[url] = {
                "hash": content_hash,
                "title": title,
                "markdown": md,  # enables true fetch avoidance next run
                "ts": int(time.time()),
                "chunks": len(texts),
            }
            save_cache(cache)
            print(f"Ingested: {url}  chunks={len(texts)}  from_cache={from_cache}")
        except Exception as e:
            print(f"[warn] {url}: {e}")


    # Structured usage/cost summary for ingestion (embeddings tokens are estimated)
    summary_msg = (
        f"Ingestion completed. Tokens used: {tracker.total_input()} input, {tracker.total_output()} output. "
        f"Estimated cost: ${tracker.total_cost_usd():.4f} (embeddings estimated)"
    )
    print(summary_msg)
    print(tracker.json_line())

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Limit number of seed URLs")
    ap.add_argument("--refresh", action="store_true", help="Force re-scrape (ignore local cache)")
    ap.add_argument("--track-metadata", dest="track_metadata", action="store_true", default=True,
                    help="Enable DB metadata persistence (default: on)")
    ap.add_argument("--no-track-metadata", dest="track_metadata", action="store_false",
                    help="Disable DB metadata persistence")
    args = ap.parse_args()
    main(limit=args.limit, refresh=args.refresh, track_metadata=args.track_metadata)


