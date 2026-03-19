from pathlib import Path
import gc
import os
from langchain_core.runnables import RunnableLambda, RunnableParallel, RunnableBranch

from app.utils.logger import get_logger
from app.utils.s3_log import s3_client, upload_extracted_csv_to_s3, upload_aggregate_macro_to_s3, flush_log_handlers

from app.services.extract import extract_news_from_pulse
from app.pipeline.event_store import get_extracted_headings, save_extracted_articles
from app.services.classify import classify_articles_parallel
from app.models import init_db
from app.utils.db_client import DBClient
from app.pipeline.embedding_service import embed_headline
from app.pipeline.event_store import update_event_timeline, insert_new_event
from app.services.log import log_article_details
from app.services.aggregate_macro import generate_aggregate_macro_environment
from app.pipeline.event_similarity import get_same_event_if_recent


logger = get_logger(__name__)

# Paths and S3 key for previous-run CSV download (used by _download_previous_run_csv)
_TEMP_DIR = Path(os.getenv("TEMP_DIR", "/tmp/macro_temp"))
_PREVIOUS_RUN_CSV_PATH = _TEMP_DIR / "extracted_news.csv"
_RECENT_NEWS_FOLDER = "recent_news_folder"

def _download_previous_run_csv(_state: dict):
    """
    Step 1a: Download previous run's extracted CSV from S3 to temp when local file is missing.

    Runs in parallel with _extract. Writes to _PREVIOUS_RUN_CSV_PATH so _filter_new can
    read it. Returns state fragment {"csv_restored": True/False}. Does not depend on input state.
    """
    bucket = os.getenv("S3_BUCKET")
    if not bucket or not bucket.strip() or _PREVIOUS_RUN_CSV_PATH.exists():
        return {"csv_restored": False}
    try:
        key = f"{_RECENT_NEWS_FOLDER}/extracted_news.csv"
        client = s3_client()
        _TEMP_DIR.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(_PREVIOUS_RUN_CSV_PATH))
        logger.info("Downloaded previous run CSV from s3://%s/%s", bucket, key)
        return {"csv_restored": True}
    except Exception as e:
        logger.debug("Could not download previous run CSV from S3 (optional): %s", e)
        return {"csv_restored": False}

def _extract_new_news(_state: dict):
    """
    Step 1b: Scrape news from Pulse.zerodha.com and populate state with raw articles.

    Does not depend on incoming state. On success, returns state containing the full
    list of extracted articles. On failure, returns empty articles list and appends
    the error message to state["errors"].

    Returns:
        dict: State with "articles" (list of NewsArticle). May include "errors" on exception.
    """
    try:
        articles = extract_news_from_pulse()
        logger.info("Extracted %d articles", len(articles))
        return {"articles": articles}
    except Exception as e:
        logger.error("Extract error: %s", e, exc_info=True)
        return {"articles": [], "errors": [str(e)]}

def _merge_parallel_state(parallel_output: dict) -> dict:
    """
    Step 1: Merge the output of RunnableParallel(download=..., extract=...) into a single state dict.

    parallel_output has keys "download" and "extract". Extract supplies "articles" (and
    possibly "errors"); download supplies "csv_restored". Combined state is passed to _filter_new.
    """
    extract_state = parallel_output.get("extract") or {}
    download_state = parallel_output.get("download") or {}
    return {**extract_state, **download_state}

def _filter_news(state: dict):
    """
    Step 2: Keep only articles whose headline is not already in the extracted CSV.

    Reads headings from temp/extracted_news.csv (or S3-restored copy) and filters
    state["articles"] so that only new headlines are passed to the LLM in later steps.
    This avoids re-classifying articles we have already processed.

    Returns:
        dict: Updated state with "new_articles" (subset of state["articles"]).
    """
    articles = state.get("articles", [])
    csv_restored = bool(state.get("csv_restored"))
    existing = get_extracted_headings()
    errors = state.get("errors") or []

    # If CSV could not be restored from S3
    if not csv_restored:
        # If this is effectively the first run (no prior errors and no local headings),
        # allow the pipeline to continue by treating all extracted articles as "new".
        if not existing and not errors:
            new_articles = list(articles)
            logger.warning(
                "CSV not restored from S3 and no local headings found. "
                "Treating all %d articles as new for this run (first-run behaviour, no dedup).",
                len(new_articles),
            )
            return {**state, "new_articles": new_articles, "terminate": False}

        # If there were prior errors, we still terminate early to avoid unsafe state.
        if errors:
            logger.error(
                "Cannot safely determine new articles due to prior errors before filter; terminating pipeline early."
            )
            return {**state, "new_articles": [], "terminate": True}

        # Degraded but acceptable: CSV not restored but local headings exist – treat all as new.
        new_articles = list(articles)
        logger.warning(
            "CSV not restored from S3, but local headings exist. Treating all %d articles as new (no dedup).",
            len(new_articles),
        )
        return {**state, "new_articles": new_articles, "terminate": False}

    # Normal path: CSV restored and headings loaded – perform deduplication.
    new_articles = [a for a in articles if a.heading.lower() not in existing]
    logger.info(
        "Filtered to %d new articles (skipped %d already in CSV)",
        len(new_articles),
        len(articles) - len(new_articles),
    )
    return {**state, "new_articles": new_articles, "terminate": False}

def _save_extracted_news(state: dict) -> dict:
    """
    Step 3: Overwrite the extracted-news CSV with the current run's full article list.

    Writes state["articles"] to temp/extracted_news.csv (replacing the file). The next
    pipeline run will use this file in _filter_new to decide which articles are "new".
    No database or delete step—the file is simply replaced each run.

    Returns:
        dict: Updated state with "saved_extracted" (number of rows written to CSV).
    """
    articles = state.get("articles", [])
    n = save_extracted_articles(articles)
    return {**state, "saved_extracted": n}

def _classify_parallel_news(state: dict):
    """
    Step 4: Run LLM classification on each new article in parallel.

    Uses OpenAI to determine macro relevance (e.g. India-focused, impact level) for
    each item in state["new_articles"]. Each article gets a classification attached;
    the list is returned as state["classified_articles"]. If there are no new
    articles, returns an empty classified list without calling the LLM.

    Returns:
        dict: Updated state with "classified_articles" (NewsArticle list with classification set).
    """
    new_articles = state.get("new_articles", [])
    if not new_articles:
        return {**state, "classified_articles": []}
    classified = classify_articles_parallel(new_articles)
    return {**state, "classified_articles": classified}

def _filter_macro(state: dict) -> dict:
    """
    Step 5: Keep only articles that are macro-relevant according to classification.

    Filters state["classified_articles"] to those that affect macro indicators
    (e.g. impact level, India link). Non-macro items are dropped before embedding
    and event storage.

    Returns:
        dict: Updated state with "macro_articles" (subset of classified articles).
    """
    classified = state.get("classified_articles", [])
    macro_articles = [
        a
        for a in classified
        if getattr(a, "classification", None)
        and getattr(a.classification, "affects_macro_indicator", False)
    ]
    discarded = len(classified) - len(macro_articles)
    if discarded:
        logger.info("Discarded %d non-macro articles", discarded)
    return {**state, "macro_articles": macro_articles}

def _process_events(state: dict) -> dict:
    """
    Step 6: Embed headlines, find similar events via pgvector, then update or create macro events.

    For each item in state["macro_articles"]:
    - Compute an embedding for the headline (OpenAI).
    - Query macro_events for a nearby embedding within the configured time window and distance.
    - If a match is found: append headline/content to that event's event_updates (timeline).
    - If no match: insert a new macro_event with the headline as first update.

    All DB work runs in a single session. On error, partial counts are still returned
    and the error is appended to state["errors"].

    Returns:
        dict: Updated state with "events_created", "events_updated" (counts). May add "errors".
    """
    macro_articles = state.get("macro_articles", [])
    if not macro_articles:
        return {**state, "events_created": 0, "events_updated": 0}
    created, updated = 0, 0
    errors = list(state.get("errors") or [])
    try:
        init_db()
        with DBClient() as db:
            total = len(macro_articles)
            for i, article in enumerate(macro_articles):
                headline = article.heading
                content = article.content or ""
                logger.info("Processing macro article %d/%d: %s", i + 1, total, headline[:60])
                flush_log_handlers()
                try:
                    embedding = embed_headline(headline)
                    event = get_same_event_if_recent(db.session, embedding)
                    if event:
                        update_event_timeline(db.session, event.id, headline, content)
                        updated += 1
                        logger.info("Updated event %s with headline: %s", event.id, headline[:50])
                    else:
                        insert_new_event(db.session, headline, embedding, headline, content)
                        created += 1
                        logger.info("Created new event for: %s", headline[:50])
                except Exception as e:
                    logger.error("Skipping article %d/%d due to error: %s", i + 1, total, e, exc_info=True)
                    errors.append(f"Article '{headline[:50]}...': {e}")
                gc.collect()  # release memory between articles to reduce OOM risk on low-RAM instances
            logger.info("Events: %d created, %d updated", created, updated)
    except Exception as e:
        logger.error("Event processing error: %s", e, exc_info=True)
        return {**state, "events_created": created, "events_updated": updated, "errors": errors + [str(e)]}
    return {**state, "events_created": created, "events_updated": updated, "errors": errors if errors else state.get("errors")}

def _log_success(state: dict) -> dict:
    """
    Step 7a (success path): Log the list of macro articles to the application logger (temp/stock_action.log).

    Writes a section header and then each macro article's details (heading, content,
    classification, etc.) via log_article_details. Does not modify state.

    Returns:
        dict: State unchanged.
    """
    to_log = state.get("macro_articles", [])
    if to_log:
        logger.info("=" * 80)
        logger.info("MACRO NEWS ARTICLES")
        logger.info("=" * 80)
        for i, article in enumerate(to_log, 1):
            log_article_details(article, i)
        logger.info("=" * 80)
    else:
        logger.info("No macro articles to log for this run.")
    return state

def _log_failure(state: dict) -> dict:
    """
    Step 7b (failure path): Log why the pipeline terminated early after _filter_new.

    Used when CSV could not be restored and we also have no local headings or prior
    errors, so we cannot safely distinguish new vs existing articles.

    Returns:
        dict: State unchanged.
    """
    articles = state.get("articles", [])
    errors = state.get("errors") or []
    logger.error("=" * 80)
    logger.error("PIPELINE TERMINATED AFTER FILTER_NEW (UNSAFE STATE)")
    logger.error(
        "Reason: CSV not restored from S3 and no reliable local headings or prior errors prevent safe deduplication."
    )
    logger.error("Articles in current run: %d", len(articles))
    if errors:
        logger.error("Errors present before termination:")
        for err in errors:
            logger.error("  - %s", err)
    logger.error("=" * 80)
    return state


def _upload_extracted_csv(state: dict) -> dict:
    """
    Step 7c (success path): upload the current run's extracted CSV to S3.

    This mirrors what main.py used to do in the finally block, but only for successful
    runs where we didn't terminate early in _filter_new.

    Returns:
        dict: State unchanged.
    """
    if state.get("terminate"):
        logger.info("Skipping extracted CSV upload because pipeline terminated early.")
        return state
    uploaded = upload_extracted_csv_to_s3()
    if uploaded:
        logger.info("Extracted CSV successfully uploaded to S3 in pipeline success chain.")
    else:
        logger.info("Extracted CSV upload skipped or failed (see earlier logs).")
    return state


def _summarize_and_aggregate(state: dict) -> dict:
    """
    Final step on success path: log pipeline summary and generate/upload aggregate macro environment.

    Logs high-level counts from the final state, then calls generate_aggregate_macro_environment
    for the last 2 days and uploads the result to S3 as aggregate_macro_environment.json.

    Returns:
        dict: State unchanged.
    """
    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETED")
    logger.info("=" * 80)
    logger.info("Extracted: %d", len(state.get("articles", [])))
    logger.info("New (after filter): %d", len(state.get("new_articles", [])))
    logger.info("Saved to CSV (current extraction): %d", state.get("saved_extracted", 0))
    logger.info("Classified: %d", len(state.get("classified_articles", [])))
    logger.info("Macro articles: %d", len(state.get("macro_articles", [])))
    logger.info(
        "Events created: %d, updated: %d",
        state.get("events_created", 0),
        state.get("events_updated", 0),
    )
    for err in state.get("errors") or []:
        logger.warning("  - %s", err)

    # Only generate aggregate macro environment on successful runs
    if state.get("terminate"):
        logger.info("Skipping aggregate macro environment generation because pipeline terminated early.")
    else:
        aggregate = generate_aggregate_macro_environment(days=2)
        if aggregate:
            upload_aggregate_macro_to_s3(aggregate)
        else:
            logger.info("No aggregate macro environment generated for this run.")
    return state


def _branch_after_filter(state: dict) -> dict:
    """
    Branch after _filter_news: if terminate=True, run failure chain, else success chain.
    """
    if state.get("terminate"):
        return _failure_chain.invoke(state)
    return _success_chain.invoke(state)


# LCEL chain: download and extract run in parallel, then merge → filter → ... → log.
_success_chain = (
    RunnableLambda(_save_extracted_news)
    | RunnableLambda(_classify_parallel_news)
    | RunnableLambda(_filter_macro)
    | RunnableLambda(_process_events)
    | RunnableLambda(_log_success)
    | RunnableLambda(_upload_extracted_csv)
    | RunnableLambda(_summarize_and_aggregate)
)

_failure_chain = (
    RunnableLambda(_log_failure)
    | RunnableLambda(_summarize_and_aggregate)
)

pipeline = (
    RunnableParallel(
        download=RunnableLambda(_download_previous_run_csv),
        extract=RunnableLambda(_extract_new_news),
    )
    | RunnableLambda(_merge_parallel_state)
    | RunnableLambda(_filter_news)
    | RunnableLambda(_branch_after_filter)
)

def run_pipeline() -> dict:
    """
    Run the full pipeline and return the final state dict.

    The first step runs in parallel: (1) download previous run's extracted CSV from S3
    when local temp file is missing, (2) extract articles from Pulse. Results are merged,
    then filter_new → save_extracted → classify_parallel → filter_macro → process_events
    → log. Caller is responsible for S3 uploads and temp cleanup (e.g. in main.py finally block).

    Returns:
        dict: Final state with keys such as articles, new_articles, saved_extracted,
              classified_articles, macro_articles, events_created, events_updated, errors.
    """
    return pipeline.invoke({})



