"""CLI entry point."""

from dotenv import load_dotenv

load_dotenv()


def main():
    from app.pipeline.run import run_pipeline
    from app.utils.logger import get_logger
    from app.utils.s3_log import (
        LOG_FILE,
        upload_log_to_s3,
        delete_temp_files,
    )

    # Replace log content in temp each run (file is kept after S3 upload).
    if LOG_FILE.exists():
        LOG_FILE.write_text("", encoding="utf-8")

    logger = get_logger(__name__)
    logger.info("Starting news pipeline...")
    try:
        run_pipeline()
    except Exception as e:
        logger.error("Pipeline error: %s", e, exc_info=True)
        raise
    finally:
        upload_log_to_s3()
        delete_temp_files()


if __name__ == "__main__":
    main()
