"""Generate aggregate_macro_environment JSON from DB headlines using macro signals prompt."""

import json
import re
from typing import Any

from openai import OpenAI

from app.config.settings import REASONING_MODEL
from app.pipeline.event_store import get_headlines_last_n_days
from app.prompts.macro_signals import MACRO_SIGNALS_TEMPLATE
from app.utils.logger import get_logger
from app.utils.secrets_manager import get_env_or_secret

logger = get_logger(__name__)


def _extract_json_from_response(text: str) -> str:
    """Strip markdown code block if present and return JSON string."""
    text = (text or "").strip()
    # Match ```json ... ``` or ``` ... ```
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if m:
        return m.group(1).strip()
    return text


def generate_aggregate_macro_environment(days: int = 2) -> dict[str, Any] | None:
    """
    Fetch headlines from DB (last N days), run macro strategist prompt, return aggregate_macro_environment dict.
    Returns None if no headlines or on error.
    """
    news_list = get_headlines_last_n_days(days=days)
    if not news_list:
        logger.warning("No headlines in DB for the past %d days; skipping aggregate macro generation", days)
        return None

    news_json = json.dumps(news_list, indent=2)
    prompt = MACRO_SIGNALS_TEMPLATE.replace("{NEWS_JSON}", news_json)

    api_key = get_env_or_secret("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not set; cannot generate aggregate macro environment")
        return None

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=REASONING_MODEL,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.choices[0].message.content or ""
        json_str = _extract_json_from_response(raw)
        data = json.loads(json_str)
        if "aggregate_macro_environment" not in data:
            data = {"aggregate_macro_environment": data}
        env = data["aggregate_macro_environment"]
        logger.info(
            "Generated aggregate_macro_environment with %d dominant signals",
            len(env.get("dominant_signals", [])),
        )
        return data
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response as JSON: %s", e, exc_info=True)
        return None
    except Exception as e:
        logger.error("Error generating aggregate macro environment: %s", e, exc_info=True)
        return None
