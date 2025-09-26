import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from db_models import Base, get_engine_from_env, SessionLocal


def main(echo: bool = False):
    load_dotenv(override=True)
    engine = get_engine_from_env(echo=echo)
    if engine is None:
        raise SystemExit("NEON_DATABASE_URL not set; cannot initialize DB")
    try:
        Base.metadata.create_all(engine)
        print("Database schema created/verified.")
    except SQLAlchemyError as e:
        raise SystemExit(f"Failed to create schema: {e}")


if __name__ == "__main__":
    # Usage: python scripts/init_db.py
    main()

