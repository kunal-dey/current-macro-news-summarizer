"""Shared Base and init_db: single table macro_events + pgvector."""

import logging

from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base

from app.config.db import engine

Base = declarative_base()


def init_db():
    """Create pgvector extension and macro_events table (extracted news use temp CSV)."""
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.commit()
    from app.models.macro_event import MacroEvent  # noqa: F401
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS macro_events_embedding_idx "
                "ON macro_events USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.getLogger(__name__).warning(
                "Could not create ivfflat index (retry after macro_events has rows): %s", e
            )
