"""Upload log and extracted CSV to S3; optionally download CSV for next run; delete temp after upload."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.utils.logger import get_logger
from app.utils.secrets_manager import get_env_or_secret

logger = get_logger(__name__)

TEMP_DIR = Path(os.getenv("TEMP_DIR", "temp"))
LOG_FILE = TEMP_DIR / "macro_news_summarizer.log"
EXTRACTED_CSV_PATH = TEMP_DIR / "extracted_news.csv"
S3_KEY_PREFIX = "logs"
RECENT_NEWS_FOLDER = "recent_news_folder"
AGGREGATE_MACRO_FOLDER = "aggregate_macro_environment"
AGGREGATE_MACRO_KEY = "aggregate_macro_environment.json"


def s3_client():
    import boto3
    access_key = get_env_or_secret("AWS_ACCESS_KEY_ID")
    secret_key = get_env_or_secret("AWS_SECRET_ACCESS_KEY")
    region = get_env_or_secret("AWS_DEFAULT_REGION", "ap-south-1")
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def flush_log_handlers() -> None:
    """Flush all file handlers so log file is written to disk before upload."""
    try:
        manager = getattr(logging.Logger, "manager", None) or getattr(logging.root, "manager", None)
        if manager and getattr(manager, "loggerDict", None):
            for _name, log in manager.loggerDict.items():
                if isinstance(log, logging.Logger):
                    for h in log.handlers:
                        if getattr(h, "flush", None):
                            h.flush()
        for h in logging.root.handlers:
            if getattr(h, "flush", None):
                h.flush()
    except Exception:
        pass


def _close_log_file_handlers() -> None:
    """Close file handlers that write to LOG_FILE so it can be deleted."""
    try:
        log_path = LOG_FILE.resolve()
        manager = getattr(logging.Logger, "manager", None) or getattr(logging.root, "manager", None)
        if manager and getattr(manager, "loggerDict", None):
            for _name, log in manager.loggerDict.items():
                if isinstance(log, logging.Logger):
                    for h in list(log.handlers):
                        base = getattr(h, "baseFilename", None)
                        if base and Path(base).resolve() == log_path:
                            h.close()
                            log.removeHandler(h)
        for h in list(logging.root.handlers):
            base = getattr(h, "baseFilename", None)
            if base and Path(base).resolve() == log_path:
                h.close()
                logging.root.removeHandler(h)
    except Exception:
        pass


def upload_log_to_s3() -> bool:
    """
    Upload TEMP_DIR/macro_news_summarizer.log to the S3 bucket. Uses S3_BUCKET from env.
    Key: logs/macro_news_summarizer_<date>_<time>.log
    Returns True if uploaded, False if skipped (no bucket set or file missing).
    """
    bucket = get_env_or_secret("S3_BUCKET")
    if not bucket or not bucket.strip():
        return False
    if not LOG_FILE.exists():
        logger.warning("Log file not found: %s", LOG_FILE)
        return False
    try:
        flush_log_handlers()
        now = datetime.now(timezone.utc)
        key = f"{S3_KEY_PREFIX}/macro_news_summarizer_{now.strftime('%Y-%m-%d_%H-%M-%S')}.log"
        client = s3_client()
        client.upload_file(str(LOG_FILE), bucket, key, ExtraArgs={"ContentType": "text/plain"})
        logger.info("Log file uploaded to s3://%s/%s", bucket, key)
        _close_log_file_handlers()
        # Keep log file in temp; next run will replace its contents (logger uses mode='w').
        return True
    except Exception as e:
        logger.error("Failed to upload log to S3: %s", e, exc_info=True)
        return False


def upload_extracted_csv_to_s3() -> bool:
    """
    Upload temp/extracted_news.csv to s3://bucket/recent_news_folder/extracted_news.csv (replaced each run).
    Returns True if uploaded, False if skipped.
    """
    bucket = get_env_or_secret("S3_BUCKET")
    if not bucket or not bucket.strip():
        return False
    if not EXTRACTED_CSV_PATH.exists():
        logger.warning("Extracted CSV not found: %s", EXTRACTED_CSV_PATH)
        return False
    try:
        key = f"{RECENT_NEWS_FOLDER}/extracted_news.csv"
        client = s3_client()
        client.upload_file(
            str(EXTRACTED_CSV_PATH), bucket, key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        logger.info("Extracted CSV uploaded to s3://%s/%s", bucket, key)
        return True
    except Exception as e:
        logger.error("Failed to upload extracted CSV to S3: %s", e, exc_info=True)
        return False


def upload_aggregate_macro_to_s3(aggregate_payload: dict) -> bool:
    """
    Upload aggregate_macro_environment JSON to s3://bucket/aggregate_macro_environment/aggregate_macro_environment.json.
    aggregate_payload should be the full output, e.g. {"aggregate_macro_environment": {...}}.
    Returns True if uploaded, False if skipped.
    """
    import json
    bucket = get_env_or_secret("S3_BUCKET")
    if not bucket or not bucket.strip():
        return False
    try:
        key = f"{AGGREGATE_MACRO_FOLDER}/{AGGREGATE_MACRO_KEY}"
        body = json.dumps(aggregate_payload, indent=2, ensure_ascii=False)
        client = s3_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Aggregate macro environment uploaded to s3://%s/%s", bucket, key)
        return True
    except Exception as e:
        logger.error("Failed to upload aggregate macro to S3: %s", e, exc_info=True)
        return False


def delete_temp_files() -> None:
    """Delete all files in temp/ (after uploads)."""
    if not TEMP_DIR.exists():
        return
    for f in TEMP_DIR.iterdir():
        if f.is_file():
            try:
                f.unlink()
                logger.info("Deleted temp file: %s", f)
            except Exception as e:
                logger.warning("Could not delete %s: %s", f, e)
