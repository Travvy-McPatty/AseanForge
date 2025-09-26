import hashlib
from urllib.parse import urlparse

from scripts.scrape_ingest import chunk, sha256_text
from scripts.db_models import Source, Page, Chunk


def test_chunking_basic_overlap():
    text = "A" * 3000
    size = 1000
    overlap = 200
    parts = list(chunk(text, size=size, overlap=overlap))
    # expected start indices: 0, 800, 1600, 2400
    starts = [0, 800, 1600, 2400]
    assert len(parts) == len(starts)
    for idx, start in enumerate(starts):
        expected = text[start:start + size]
        assert parts[idx] == expected
    # boundary: last chunk shorter
    assert len(parts[-1]) == 600


def _persist_like_ingest(session, url: str, title: str, md: str, from_cache: bool, size=250, overlap=50):
    """Mimic the metadata persistence block in scripts/scrape_ingest.main using the
    same ORM models, without external services.
    """
    content_hash = sha256_text(md)
    texts = list(chunk(md, size=size, overlap=overlap))
    est_tokens = sum(len(t) // 4 for t in texts)

    parsed = urlparse(url)
    domain = parsed.netloc
    base_url = (parsed.scheme + "://" + parsed.netloc) if parsed.scheme and parsed.netloc else None

    src = session.query(Source).filter_by(domain=domain).first()
    if src is None:
        src = Source(domain=domain, base_url=base_url, discovery_method="firecrawl")
        session.add(src)
        session.flush()

    page = session.query(Page).filter_by(url=url, content_hash=content_hash).first()
    if page is None:
        page = Page(source_id=src.id, url=url, title=title or None,
                    content_hash=content_hash or "", token_estimate=est_tokens,
                    from_cache=from_cache)
        session.add(page)
        session.flush()

    # insert chunks if missing
    for i, t in enumerate(texts):
        ch = session.query(Chunk).filter_by(page_id=page.id, chunk_index=i).first()
        if ch is None:
            ch = Chunk(page_id=page.id, chunk_index=i, text_len=len(t),
                       token_estimate=len(t)//4, embedding_model="text-embedding-3-small",
                       meta={"url": url, "title": title, "content_hash": content_hash, "chunk_index": i})
            session.add(ch)
    session.commit()

    return {
        "source": src,
        "page": page,
        "chunks": texts,
        "est_tokens": est_tokens,
        "content_hash": content_hash,
    }


def test_metadata_persistence_and_idempotency(db_session):
    url = "https://example.com/article"
    title = "Example Article"
    md = "# Header\n" + ("Body " * 200)

    # First run persists source, page, chunks
    res1 = _persist_like_ingest(db_session, url, title, md, from_cache=False, size=300, overlap=50)

    # Verify source
    assert res1["source"].domain == "example.com"

    # Verify page
    page = db_session.query(Page).filter_by(url=url, content_hash=res1["content_hash"]).one()
    assert page.title == title
    assert page.from_cache is False
    assert page.token_estimate == res1["est_tokens"]

    # Verify chunks count and metadata
    chunk_rows = db_session.query(Chunk).filter_by(page_id=page.id).order_by(Chunk.chunk_index).all()
    assert len(chunk_rows) == len(res1["chunks"]) > 0
    for i, ch in enumerate(chunk_rows):
        assert ch.chunk_index == i
        assert ch.meta["url"] == url
        assert ch.meta["title"] == title
        assert ch.meta["content_hash"] == res1["content_hash"]

    # Second run with identical content should not create duplicates
    _persist_like_ingest(db_session, url, title, md, from_cache=True, size=300, overlap=50)

    # Counts remain the same
    assert db_session.query(Source).count() == 1
    assert db_session.query(Page).count() == 1
    assert db_session.query(Chunk).count() == len(chunk_rows)


def test_deduplication_by_url_and_content_hash(db_session):
    url = "https://example.com/dup"
    title = "Dup"
    md = "Same content repeated" * 50

    # two runs with identical md
    _persist_like_ingest(db_session, url, title, md, from_cache=False)
    _persist_like_ingest(db_session, url, title, md, from_cache=False)

    # Only one page for given (url,hash)
    assert db_session.query(Page).count() == 1
    # If content changes, a second page with same URL but different hash is allowed
    md2 = md + " extra"
    _persist_like_ingest(db_session, url, title, md2, from_cache=False)
    assert db_session.query(Page).count() == 2

