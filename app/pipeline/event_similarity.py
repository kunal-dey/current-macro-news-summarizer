"""pgvector similarity search for macro events (cosine distance)."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config.settings import DISTANCE_SAME_EVENT, EVENT_WINDOW_HOURS
from app.models.macro_event import MacroEvent
from app.utils.logger import get_logger

logger = get_logger(__name__)

UTC = timezone.utc


def find_similar_events(
    session: Session,
    embedding: List[float],
    limit: int = 5,
) -> List[Tuple[MacroEvent, float]]:
    """
    Return events ordered by cosine distance (ascending).
    Each element is (MacroEvent, distance).
    """
    distance_col = MacroEvent.embedding.cosine_distance(embedding).label("distance")
    stmt = select(MacroEvent, distance_col).order_by(distance_col).limit(limit)
    rows = session.execute(stmt).all()
    return [(row[0], float(row[1])) for row in rows]


def get_same_event_if_recent(
    session: Session,
    embedding: List[float],
) -> Optional[MacroEvent]:
    """
    If a similar event exists (distance < DISTANCE_SAME_EVENT, e.g. 0.18) and was
    first_seen within EVENT_WINDOW_HOURS (e.g. 7 days), return it; else None.
    """
    candidates = find_similar_events(session, embedding, limit=5)
    cutoff = datetime.now(UTC) - timedelta(hours=EVENT_WINDOW_HOURS)
    for event, distance in candidates:
        if distance >= DISTANCE_SAME_EVENT:
            continue
        first_seen = event.first_seen
        if first_seen is None:
            continue
        if getattr(first_seen, "tzinfo", None) is None:
            first_seen = first_seen.replace(tzinfo=UTC)
        if first_seen >= cutoff:
            return event
    return None
