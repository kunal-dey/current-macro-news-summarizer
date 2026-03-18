"""Database engine and session factory (PostgreSQL on EC2)."""

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.utils.secrets_manager import get_env_or_secret

load_dotenv()


def _build_engine():
    # Prefer EC2_DB_* keys, then DB_* as fallbacks; each is resolved from
    # environment first and then from Secrets Manager JSON payload.
    # If nothing is configured, fall back to localhost so the app can still
    # start and you'll see a clear connection error instead of a config error.
    host = (
        get_env_or_secret("EC2_DB_CONNECTION")
        or get_env_or_secret("DB_HOST")
        or "localhost"
    )
    port = get_env_or_secret("EC2_DB_PORT") or get_env_or_secret("DB_PORT", "5432")
    database = get_env_or_secret("EC2_DB_NAME") or get_env_or_secret("DB_NAME", "newsdb")
    user = get_env_or_secret("EC2_DB_USERNAME") or get_env_or_secret("DB_USER", "postgres")
    # Allow password to be missing at import time so the app can still start.
    # If the DB really requires a password, connection attempts will fail later
    # with a clear authentication error instead.
    password = get_env_or_secret("EC2_DB_PASSWORD") or get_env_or_secret("DB_PASSWORD") or ""
    url = (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}"
    )
    connect_args = {}
    if os.getenv("EC2_DB_SSL", "").lower() in ("1", "true", "yes"):
        connect_args["sslmode"] = "require"
    return create_engine(url, echo=False, connect_args=connect_args)


engine = _build_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
