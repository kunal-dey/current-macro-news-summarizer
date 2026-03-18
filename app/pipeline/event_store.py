"""Extracted news: CSV snapshot for filter. Macro_events: event dedup and timeline."""

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, List, Set
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.base import init_db
from app.models.macro_event import MacroEvent
from app.schemas.news_article import NewsArticle
from app.utils.db_client import DBClient
from app.utils.logger import get_logger

logger = get_logger(__name__)

EXTRACTED_CSV_PATH = Path("temp/extracted_news.csv")
CSV_FIELDS = ["heading", "content", "source", "timestamp", "url"]


def get_headlines_last_n_days(days: int = 2) -> List[dict[str, Any]]:
    """Headlines and content from macro_events whose event_updates fall within the last N days."""
    cutoff = datetime.now(UTC) - timedelta(days=days)
    out: List[dict[str, Any]] = []
    try:
        init_db()
        with DBClient() as db:
            stmt = select(MacroEvent.event_updates).where(MacroEvent.last_updated >= cutoff)
            rows = db.session.execute(stmt).scalars().all()
            for updates in rows:
                if not updates:
                    continue
                for u in updates:
                    ts_str = u.get("timestamp") or ""
                    try:
                        if ts_str.endswith("Z"):
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        else:
                            ts = datetime.fromisoformat(ts_str)
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=UTC)
                    except Exception:
                        ts = cutoff
                    if ts >= cutoff:
                        out.append({
                            "headline": u.get("headline") or "",
                            "content": u.get("content") or "",
                            "timestamp": ts_str,
                        })
            # Sort by timestamp descending (newest first)
            out.sort(key=lambda x: x["timestamp"], reverse=True)
    except Exception as e:
        logger.warning("Could not load headlines from DB: %s", e)
    return out


def get_extracted_headings() -> Set[str]:
    """Headlines from current extracted CSV (previous run). Filter on these before any LLM."""
    out: Set[str] = set()
    if not EXTRACTED_CSV_PATH.exists():
        return out
    try:
        with open(EXTRACTED_CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=CSV_FIELDS)
            for row in reader:
                # skip header row when we wrote it with writeheader()
                if row.get("heading") == "heading":
                    continue
                h = (row.get("heading") or "").strip()
                if h:
                    out.add(h.lower())
    except Exception as e:
        logger.warning("Could not load extracted headings from CSV: %s", e)
    return out


def save_extracted_articles(articles: List[NewsArticle]) -> int:
    """Overwrite CSV with current extracted articles (no DB, no delete step)."""
    if not articles:
        return 0
    try:
        EXTRACTED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(EXTRACTED_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
            for article in articles:
                w.writerow({
                    "heading": article.heading or "",
                    "content": article.content or "",
                    "source": article.source or "",
                    "timestamp": article.timestamp or "",
                    "url": article.url or "",
                })
        n = len(articles)
        logger.info("Wrote %d extracted articles to CSV (replaced previous)", n)
        return n
    except Exception as e:
        logger.error("Save extracted CSV error: %s", e, exc_info=True)
        return 0


def _update_row(session: Session, event: MacroEvent, headline: str, content: str) -> None:
    now = datetime.now(UTC)
    updates = list(event.event_updates or [])
    updates.append({
        "timestamp": now.isoformat(),
        "headline": headline,
        "content": content or "",
    })
    event.event_updates = updates
    event.last_updated = now
    session.add(event)


def update_event_timeline(session: Session, event_id: uuid.UUID, headline: str, content: str) -> None:
    """Append headline/content to event_updates and set last_updated."""
    event = session.get(MacroEvent, event_id)
    if event:
        _update_row(session, event, headline, content)


def insert_new_event(
    session: Session,
    event_title: str,
    embedding: List[float],
    headline: str,
    content: str,
) -> MacroEvent:
    """Create a new macro event with one update in event_updates."""
    now = datetime.now(UTC)
    event = MacroEvent(
        id=uuid.uuid4(),
        event_title=event_title or headline,
        embedding=embedding,
        first_seen=now,
        last_updated=now,
        event_updates=[{
            "timestamp": now.isoformat(),
            "headline": headline,
            "content": content or "",
        }],
    )
    session.add(event)
    return event
