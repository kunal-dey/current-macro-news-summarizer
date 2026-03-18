"""Generate OpenAI embeddings for headlines (text-embedding-3-small, 1536 dim)."""

from typing import List

from langchain_openai import OpenAIEmbeddings

from app.config.settings import EMBEDDING_MODEL
from app.utils.secrets_manager import get_env_or_secret


def get_embedding_model() -> OpenAIEmbeddings:
    api_key = get_env_or_secret("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required for embeddings")
    return OpenAIEmbeddings(model=EMBEDDING_MODEL, openai_api_key=api_key)


def embed_headline(headline: str) -> List[float]:
    """Return 1536-dim embedding for the headline only."""
    model = get_embedding_model()
    return model.embed_query(headline)
