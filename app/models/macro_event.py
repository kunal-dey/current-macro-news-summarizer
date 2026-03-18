"""Macro events table with pgvector for semantic deduplication."""

import uuid
from datetime import UTC, datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, DateTime, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.models.base import Base


class MacroEvent(Base):
    """Evolving macro event: similar headlines grouped by embedding similarity."""

    __tablename__ = "macro_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_title = Column(Text, nullable=True)
    embedding = Column(Vector(1536), nullable=False)
    first_seen = Column(DateTime(timezone=True), nullable=False)
    last_updated = Column(DateTime(timezone=True), nullable=False)
    event_updates = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))

    def __repr__(self):
        return f"<MacroEvent(id={self.id}, title={self.event_title[:50] if self.event_title else None!r})>"
