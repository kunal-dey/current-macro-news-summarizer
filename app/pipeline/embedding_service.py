"""Generate OpenAI embeddings for headlines (text-embedding-3-small, 1536 dim)."""

from typing import List

from openai import OpenAI

from app.config.settings import EMBEDDING_MODEL
from app.utils.secrets_manager import get_env_or_secret


def _get_openai_client() -> OpenAI:
    api_key = get_env_or_secret("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required for embeddings")
    return OpenAI(api_key=api_key)


def embed_headline(headline: str) -> List[float]:
    """Return 1536-dim embedding for the headline only."""
    client = _get_openai_client()
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=headline)
    # OpenAI returns a list of embeddings in the same order as inputs.
    return list(resp.data[0].embedding)
