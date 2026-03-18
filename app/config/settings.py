"""App configuration (constants and env-driven settings)."""

import re

URL = "https://pulse.zerodha.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}
MIN_HEADING_LENGTH = 15
EXCLUDED_HEADINGS = {"pulse", "trending", "menu", "home", "about"}
SOURCE_PATTERN = re.compile(r"—\s*([^—\n]+?)(?:\s+\d+\s+(?:minutes?|hours?|days?)\s+ago|$)", re.I)
TIME_PATTERN = re.compile(r"(\d+\s+(?:minutes?|hours?|days?)\s+ago)", re.I)

REASONING_MODEL = "gpt-4o-mini"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536

# pgvector similarity thresholds (cosine distance)
# Use 0.18 so semantically same story (e.g. "Rupee at record low" phrased differently) groups as one event
DISTANCE_SAME_EVENT = 0.18
DISTANCE_RELATED = 0.22
# Match events within 7 days so follow-up headlines on the same theme merge (e.g. rupee record low over days)
EVENT_WINDOW_HOURS = 24 * 7