"""Classify articles via OpenAI (true/false macro relevance)."""

from typing import Any, Dict, List

from langchain_core.runnables import RunnableLambda, RunnableParallel
from openai import OpenAI

from app.config.settings import REASONING_MODEL
from app.prompts.macro_classifier import MACRO_CLASSIFIER_TEMPLATE
from app.schemas.news_article import MacroClassification, NewsArticle
from app.utils.logger import get_logger
from app.utils.secrets_manager import get_env_or_secret

logger = get_logger(__name__)


def _get_llm():
    api_key = get_env_or_secret("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI API key is required")
    return OpenAI(api_key=api_key)


def _parse_true_false(response) -> bool:
    text = (response.content if hasattr(response, "content") else str(response)).strip().lower()
    return text.startswith("true") or text == "true"


def classify_one_article(article: NewsArticle) -> NewsArticle:
    """Classify a single article (for use in parallel)."""
    try:
        prompt = MACRO_CLASSIFIER_TEMPLATE.format(
            heading=article.heading,
            content=article.content or "",
        )
        client = _get_llm()
        resp = client.chat.completions.create(
            model=REASONING_MODEL,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.choices[0].message.content or ""
        affects = _parse_true_false(text)
        article.classification = MacroClassification(
            affects_macro_indicator=affects,
            impact_level="None",
            affected_category="None",
            impact_frequency="Not Applicable",
            geographical_scope="None",
            reasoning="",
        )
        return article
    except Exception as e:
        logger.error("Error classifying article %r: %s", article.heading[:50], e, exc_info=True)
        return article


def classify_articles_parallel(articles: List[NewsArticle]) -> List[NewsArticle]:
    """Classify multiple articles in parallel using LCEL RunnableParallel."""
    if not articles:
        return []
    logger.info("Classifying %d articles in parallel...", len(articles))
    parallel = RunnableParallel(
        **{
            str(i): RunnableLambda(
                lambda state, idx=i: classify_one_article(state["articles"][idx])
            )
            for i in range(len(articles))
        }
    )
    result: Dict[str, Any] = parallel.invoke({"articles": articles})
    classified = [result[str(i)] for i in range(len(articles))]
    for idx, a in enumerate(classified, 1):
        if a.classification:
            logger.info("Article %d/%d: macro=%s", idx, len(classified), a.classification.affects_macro_indicator)
    return classified