from __future__ import annotations
import os
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    create_engine, Column, BigInteger, Integer, String, Text, DateTime, ForeignKey,
    UniqueConstraint, Index, JSON, Numeric, Boolean
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

# Use BigInteger for Postgres; fallback to Integer on SQLite for autoincrement PKs
from sqlalchemy import BigInteger as _BigInteger, Integer as _Integer
PKBigInt = _BigInteger().with_variant(_Integer(), "sqlite")



def get_engine_from_env(echo: bool = False):
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        return None
    # SQLAlchemy psycopg dialect for async-compatible driver
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, echo=echo, pool_pre_ping=True, future=True)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, future=True)


class Source(Base):
    __tablename__ = "sources"
    id = Column(PKBigInt, primary_key=True, autoincrement=True)
    domain = Column(String(255), nullable=False)
    base_url = Column(Text, nullable=True)
    discovery_method = Column(String(50), nullable=True, default="seed")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    pages = relationship("Page", back_populates="source", cascade="all,delete-orphan")

    __table_args__ = (
        UniqueConstraint("domain", name="uq_sources_domain"),
        Index("ix_sources_domain", "domain"),
    )


class Page(Base):
    __tablename__ = "pages"
    id = Column(PKBigInt, primary_key=True, autoincrement=True)
    source_id = Column(PKBigInt, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False)
    url = Column(Text, nullable=False)
    title = Column(Text, nullable=True)
    content_hash = Column(String(64), nullable=False)
    fetched_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    token_estimate = Column(Integer, nullable=True)
    from_cache = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    source = relationship("Source", back_populates="pages")
    chunks = relationship("Chunk", back_populates="page", cascade="all,delete-orphan")

    __table_args__ = (
        UniqueConstraint("url", "content_hash", name="uq_pages_url_hash"),
        Index("ix_pages_url", "url"),
        Index("ix_pages_hash", "content_hash"),
    )


class Chunk(Base):
    __tablename__ = "chunks"
    id = Column(PKBigInt, primary_key=True, autoincrement=True)
    page_id = Column(PKBigInt, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text_len = Column(Integer, nullable=True)
    token_estimate = Column(Integer, nullable=True)
    embedding_model = Column(String(64), nullable=True)
    meta = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    page = relationship("Page", back_populates="chunks")

    __table_args__ = (
        UniqueConstraint("page_id", "chunk_index", name="uq_chunks_page_idx"),
        Index("ix_chunks_page", "page_id"),
    )



