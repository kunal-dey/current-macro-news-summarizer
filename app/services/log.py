"""Log article details."""

from app.schemas.news_article import NewsArticle
from app.utils.logger import get_logger

logger = get_logger(__name__)


def log_article_details(article: NewsArticle, idx: int) -> None:
    logger.info("")
    logger.info("Article %d:", idx)
    logger.info("-" * 80)
    logger.info("HEADING: %s", article.heading)
    logger.info("")
    logger.info("CONTENT: %s", article.content or "[No content available]")
    if article.source:
        logger.info("SOURCE: %s", article.source)
    if article.timestamp:
        logger.info("TIMESTAMP: %s", article.timestamp)
    if article.url:
        logger.info("URL: %s", article.url)
    if article.classification:
        logger.info("AFFECTS MACRO: %s", article.classification.affects_macro_indicator)
    logger.info("-" * 80)
