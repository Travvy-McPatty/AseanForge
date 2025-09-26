import os
import pytest
from sqlalchemy import create_engine

from scripts.db_models import Base, SessionLocal


@pytest.fixture(scope="function")
def db_session():
    """Provide a fresh in-memory SQLite session for each test.
    Binds the project's SessionLocal to a new SQLite engine and creates schema.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    # Bind the global SessionLocal used by scripts to this test engine
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        # Not strictly needed for in-memory, but keep tidy
        Base.metadata.drop_all(engine)
        engine.dispose()

