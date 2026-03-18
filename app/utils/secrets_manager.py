"""Helpers to read application credentials from .env and AWS Secrets Manager."""

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import boto3
from dotenv import load_dotenv


@lru_cache(maxsize=1)
def _load_secret_payload() -> dict[str, Any]:
    """
    Load JSON payload from AWS Secrets Manager using APP_SECRET_NAME.

    Returns an empty dict when APP_SECRET_NAME is not configured or when
    the secret has no SecretString payload.
    """
    secret_name = "agent_config"
    if not secret_name:
        return {}

    region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
    # If AWS static credentials are provided, pass them explicitly.
    # Otherwise boto3 will fall back to its normal credential chain
    # (instance role, shared config/profile, etc.).
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    client_kwargs: dict[str, Any] = {"region_name": region}
    if access_key and secret_key:
        client_kwargs["aws_access_key_id"] = access_key
        client_kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            client_kwargs["aws_session_token"] = session_token

    client = boto3.client("secretsmanager", **client_kwargs)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except Exception:
        # If the secret name/region is wrong locally (or IAM lacks permission),
        # don't crash at import-time; fall back to env/defaults.
        return {}

    secret_string = response.get("SecretString")
    if not secret_string:
        return {}

    payload = json.loads(secret_string)
    if not isinstance(payload, dict):
        return {}

    # Some Secrets Manager setups store nested JSON like:
    # {
    #   "agents-env": { "EC2_DB_CONNECTION": "...", ... },
    #   "OPENAI_API_KEY": "..."
    # }
    #
    # Our code expects top-level keys. If we detect that pattern, flatten it.
    if secret_name in payload and isinstance(payload.get(secret_name), dict):
        nested = payload.get(secret_name) or {}
        flat: dict[str, Any] = dict(nested)
        # Preserve any top-level scalar keys too (e.g. OPENAI_API_KEY).
        for k, v in payload.items():
            if k == secret_name:
                continue
            if not isinstance(v, dict) and k not in flat:
                flat[k] = v
        payload = flat

    return payload


def get_env_or_secret(key: str, default: str | None = None) -> str | None:
    """
    Resolve config value from, in order:
    1. Process environment (after ensuring project .env is loaded)
    2. Secrets Manager JSON payload (APP_SECRET_NAME)
    3. Provided default
    """
    # Ensure project .env is loaded for local/dev runs, regardless of CWD.
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=project_root / ".env", override=False)

    value = os.getenv(key)
    if value is not None and str(value).strip() != "":
        return value

    payload = _load_secret_payload()
    secret_value = payload.get(key, default)
    if secret_value is None:
        return None
    return str(secret_value)
