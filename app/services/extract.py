"""Extract news from Pulse.zerodha.com."""

import re
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from app.config.settings import (
    EXCLUDED_HEADINGS,
    HEADERS,
    MIN_HEADING_LENGTH,
    SOURCE_PATTERN,
    TIME_PATTERN,
    URL,
)
from app.schemas.news_article import NewsArticle
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _extract_article_data(element) -> Optional[NewsArticle]:
    heading_elem = element.find(["h2", "h3", "h4", "h5", "a"])
    if not heading_elem:
        return None
    heading_text = heading_elem.get_text(strip=True)
    if len(heading_text) < MIN_HEADING_LENGTH or heading_text.lower() in EXCLUDED_HEADINGS:
        return None
    text = element.get_text()
    source_match = SOURCE_PATTERN.search(text)
    source = source_match.group(1).strip() if source_match else None
    time_match = TIME_PATTERN.search(text)
    timestamp = time_match.group(1) if time_match else None
    content = ""
    content_elem = element.find("p")
    if content_elem:
        content = content_elem.get_text(strip=True)
    if not content:
        content_divs = element.find_all(
            ["div", "span"], class_=re.compile(r"content|description|summary|excerpt|text", re.I)
        )
        for div in content_divs:
            div_text = div.get_text(strip=True)
            if div_text and div_text != heading_text and len(div_text) > 20:
                if source and source in div_text:
                    div_text = re.sub(rf"{re.escape(source)}.*", "", div_text, flags=re.I)
                if timestamp and timestamp in div_text:
                    div_text = re.sub(rf"{re.escape(timestamp)}.*", "", div_text, flags=re.I)
                content = div_text.strip()
                if content:
                    break
    if not content:
        all_text = element.get_text(separator=" ", strip=True)
        content = all_text.replace(heading_text, "", 1).strip()
        if source and source in content:
            content = re.sub(rf"{re.escape(source)}.*", "", content, flags=re.I).strip()
        if timestamp and timestamp in content:
            content = re.sub(rf"{re.escape(timestamp)}.*", "", content, flags=re.I).strip()
        content = re.sub(r"^\s*[—–-]\s*", "", content).strip()
    if content:
        content = re.sub(r"\d+\s+(?:minutes?|hours?|days?)\s+ago.*", "", content, flags=re.I).strip()
        content = re.sub(r"—\s*[^—]+$", "", content).strip()
    link = element.find("a", href=True)
    article_url = None
    if link:
        article_url = link.get("href")
        if article_url and not article_url.startswith("http"):
            article_url = f"{URL.rstrip('/')}{article_url}"
    return NewsArticle(
        heading=heading_text,
        content=content[:500] if content else "",
        source=source,
        timestamp=timestamp,
        url=article_url,
    )


def extract_news_from_pulse() -> List[NewsArticle]:
    try:
        response = requests.get(URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        logger.info("Fetched webpage: %d bytes", len(response.content))
        articles = []
        selectors = [
            ("li", {}),
            (["div", "article"], {"class": re.compile(r"item|article|news|story|card", re.I)}),
            (["h2", "h3", "h4"], {}),
        ]
        for selector, attrs in selectors:
            elements = soup.find_all(selector, attrs) if attrs else soup.find_all(selector)
            for elem in elements:
                if isinstance(selector, list) and all(tag in ["h2", "h3", "h4"] for tag in selector):
                    elem = elem.parent or elem
                article = _extract_article_data(elem)
                if article:
                    articles.append(article)
            if len(articles) >= 10:
                break
        seen = set()
        unique_articles = []
        for a in articles:
            if a.heading.lower() not in seen:
                seen.add(a.heading.lower())
                unique_articles.append(a)
        logger.info("Extracted %d unique news articles", len(unique_articles))
        return unique_articles
    except requests.RequestException as e:
        logger.error("Error fetching webpage: %s", e)
        return []
    except Exception as e:
        logger.error("Error parsing webpage: %s", e, exc_info=True)
        return []
