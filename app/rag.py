#!/usr/bin/env python3
"""
Lean Hybrid Retriever (BM25 + pgvector cosine) for AseanForge.

- Uses Postgres FTS (tsvector) for lexical scoring
- Uses documents.embedding (vector(1536)) for semantic scoring
- Reciprocal Rank Fusion (RRF) to combine

Environment: loads app/.env
Requirements: psycopg2, openai, python-dotenv
"""
from __future__ import annotations

import os
import time
from typing import List, Dict, Any
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

# OpenAI Embeddings for query vectors
try:
    from openai import OpenAI
    _oa_client = OpenAI()
except Exception:
    _oa_client = None


@dataclass
class Retrieved:
    id: str
    event_id: str | None
    source_url: str
    authority: str
    pub_date: str | None
    title: str | None
    snippet: str
    score: float


def _get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(url)


def ensure_db_prereqs():
    """Ensure vector extension and indices exist. Idempotent."""
    conn = _get_db(); cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    # Ensure embedding columns
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS embedding vector(1536);")
    cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding vector(1536);")
    cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding_model text;")
    cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding_ts timestamptz;")
    cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding_version text;")
    # Try to add generated FTS column; ignore if fails
    try:
        cur.execute("""
            ALTER TABLE documents
            ADD COLUMN IF NOT EXISTS documents_fts tsvector
            GENERATED ALWAYS AS (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(clean_text,''))) STORED;
        """)
    except Exception:
        pass
    # Indices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_fts ON documents USING GIN ((to_tsvector('english', coalesce(title,'') || ' ' || coalesce(clean_text,''))));")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_embedding ON documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);")
    except Exception:
        # ivfflat may not be available; skip
        pass
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_event_id ON documents(event_id);")
    conn.commit(); cur.close(); conn.close()


def _embed_query(text: str) -> List[float]:
    if _oa_client is None:
        raise RuntimeError("OpenAI SDK not available; cannot embed query")
    model = os.getenv("EMBED_MODEL", os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small"))
    resp = _oa_client.embeddings.create(model=model, input=text)
    return resp.data[0].embedding  # type: ignore


def bm25_search(query: str, k: int = 50) -> List[Retrieved]:
    q = query.replace("'", " ")
    sql = """
        WITH query AS (SELECT plainto_tsquery('english', %s) AS q)
        SELECT d.document_id::text as id, d.event_id::text as event_id, d.source_url, e.authority,
               e.pub_date::text, e.title, substring(d.clean_text from 1 for 300) AS snippet,
               ts_rank_cd(to_tsvector('english', coalesce(e.title,'') || ' ' || coalesce(d.clean_text,'')), (SELECT q FROM query)) AS fts_score
        FROM documents d
        JOIN events e ON e.event_id = d.event_id, query
        WHERE to_tsvector('english', coalesce(e.title,'') || ' ' || coalesce(d.clean_text,'')) @@ (SELECT q FROM query)
        ORDER BY fts_score DESC
        LIMIT %s;
    """
    conn = _get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, (q, k))
    rows = cur.fetchall()
    cur.close(); conn.close()
    out: List[Retrieved] = []
    for r in rows:
        out.append(Retrieved(
            id=r["id"], event_id=r.get("event_id"), source_url=r.get("source_url") or "",
            authority=r.get("authority") or "", pub_date=r.get("pub_date"), title=r.get("title"),
            snippet=r.get("snippet") or "", score=float(r.get("fts_score") or 0.0)
        ))
    return out


def vector_search(query: str, k: int = 50, freshness_days: int = 365) -> List[Retrieved]:
    vec = _embed_query(query)
    conn = _get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
    sql = """
        SELECT d.document_id::text as id, d.event_id::text as event_id, d.source_url,
               e.authority, e.pub_date::text, e.title,
               1 - (d.embedding <=> %s) AS vector_score,
               substring(d.clean_text from 1 for 300) AS snippet
        FROM documents d
        JOIN events e ON e.event_id = d.event_id
        WHERE d.embedding IS NOT NULL
          AND (e.pub_date IS NULL OR e.pub_date >= (CURRENT_DATE - (%s || ' days')::interval))
        ORDER BY d.embedding <=> %s
        LIMIT %s;
    """
    cur.execute(sql, (vec, freshness_days, vec, k))
    rows = cur.fetchall()
    cur.close(); conn.close()
    out: List[Retrieved] = []
    for r in rows:
        out.append(Retrieved(
            id=r["id"], event_id=r.get("event_id"), source_url=r.get("source_url") or "",
            authority=r.get("authority") or "", pub_date=r.get("pub_date"), title=r.get("title"),
            snippet=r.get("snippet") or "", score=float(r.get("vector_score") or 0.0)
        ))
    return out


def rrf_fuse(bm25: List[Retrieved], vec: List[Retrieved], k: int = 10, K: int = 60, fts_weight: float = 1.0) -> List[Retrieved]:
    rank_map: Dict[str, Dict[str, int]] = {}
    for i, r in enumerate(bm25):
        rank_map.setdefault(r.id, {})['bm25'] = i + 1
    for i, r in enumerate(vec):
        rank_map.setdefault(r.id, {})['vec'] = i + 1
    scored: Dict[str, float] = {}
    items: Dict[str, Retrieved] = {}
    for r in bm25 + vec:
        items[r.id] = r
    for doc_id, ranks in rank_map.items():
        s = 0.0
        if 'bm25' in ranks:
            s += fts_weight * (1.0 / (K + ranks['bm25']))
        if 'vec' in ranks:
            s += 1.0 / (K + ranks['vec'])
        scored[doc_id] = s
    top_ids = sorted(scored.keys(), key=lambda x: scored[x], reverse=True)[:k]
    return [items[i] for i in top_ids]


def retrieve(query: str, k: int = 10, K_lex: int = 50, K_vec: int = 50, K_rrf: int = 60, fts_weight: float = 1.0, freshness_days: int = 365) -> List[Dict[str, Any]]:
    t0 = time.time()
    bm = bm25_search(query, k=K_lex)
    try:
        vc = vector_search(query, k=K_vec, freshness_days=freshness_days)
    except Exception:
        vc = []
    fused = rrf_fuse(bm, vc, k=k, K=K_rrf, fts_weight=fts_weight)
    latency_ms = int((time.time() - t0) * 1000)
    out = []
    for r in fused:
        out.append({
            "snippet": r.snippet,
            "source_url": r.source_url,
            "authority": r.authority,
            "pub_date": r.pub_date,
            "title": r.title,
            "rrf_score": round(r.score, 4),
            "latency_ms": latency_ms
        })
    return out


if __name__ == "__main__":
    ensure_db_prereqs()
    res = retrieve("latest AI governance from MAS", k=5)
    for r in res:
        print(f"- {r['authority']} {r['pub_date']}: {r['title']} -> {r['source_url']}")

