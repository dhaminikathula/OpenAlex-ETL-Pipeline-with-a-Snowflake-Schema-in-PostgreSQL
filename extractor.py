"""
extractor.py - OpenAlex API extraction with cursor pagination and exponential backoff.

Provides a generator that yields batches of raw work dictionaries from the
OpenAlex /works endpoint. Handles:

  - Cursor-based pagination (required for > 10,000 results)
  - 429 Too Many Requests with exponential backoff + jitter
  - Connection errors with automatic retry
  - Polite pool identification via User-Agent / mailto param
"""

import logging
import random
import time
from typing import Generator, Optional

import requests

import config

logger = logging.getLogger(__name__)

# Build a descriptive User-Agent string (OpenAlex polite pool best practice)
_USER_AGENT = (
    "OpenAlexETL/1.0 "
    f"(mailto:{config.OPENALEX_EMAIL}; "
    "Python-ETL-Pipeline; "
    "Snowflake-Schema-PostgreSQL; "
    "https://github.com/dhaminikathula/OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL)"
)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": _USER_AGENT})


def _build_params(cursor: str) -> dict:
    """Construct query parameters for a works request."""
    params: dict = {
        "per-page": config.BATCH_SIZE,
        "cursor": cursor,
        "select": (
            "id,title,publication_year,publication_date,"
            "cited_by_count,type,authorships,concepts,referenced_works"
        ),
    }
    if config.OPENALEX_EMAIL:
        params["mailto"] = config.OPENALEX_EMAIL
    return params


def _fetch_with_backoff(url: str, params: dict) -> Optional[dict]:
    """
    Perform a single GET request with exponential backoff on 429 errors.

    Returns the parsed JSON dict on success, or None if max retries exceeded.
    Raises requests.RequestException for non-recoverable HTTP errors.
    """
    base_wait = config.BASE_WAIT_SECONDS
    max_retries = config.MAX_RETRIES

    for attempt in range(max_retries):
        try:
            response = _SESSION.get(url, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                # Exponential backoff: wait = base * 2^attempt + jitter
                wait_time = base_wait * (2 ** attempt) + random.uniform(0.0, 1.0)
                logger.warning(
                    "Rate limited (429) on attempt %d/%d. "
                    "Retrying in %.2fs...",
                    attempt + 1,
                    max_retries,
                    wait_time,
                )
                time.sleep(wait_time)

            elif response.status_code == 503:
                # Service temporarily unavailable — short retry
                wait_time = base_wait * (2 ** attempt) + random.uniform(0.0, 0.5)
                logger.warning(
                    "Service unavailable (503) on attempt %d/%d. "
                    "Retrying in %.2fs...",
                    attempt + 1,
                    max_retries,
                    wait_time,
                )
                time.sleep(wait_time)

            elif response.status_code == 500:
                # Internal server error — retry with backoff
                wait_time = base_wait * (2 ** attempt) + random.uniform(0.0, 1.0)
                logger.warning(
                    "Server error (500) on attempt %d/%d. "
                    "Retrying in %.2fs...",
                    attempt + 1,
                    max_retries,
                    wait_time,
                )
                time.sleep(wait_time)

            else:
                # Non-recoverable HTTP error
                logger.error(
                    "HTTP %d received for URL %s. Aborting request.",
                    response.status_code,
                    url,
                )
                response.raise_for_status()

        except requests.exceptions.ConnectionError as exc:
            wait_time = base_wait * (2 ** attempt) + random.uniform(0.0, 1.0)
            logger.warning(
                "Connection error on attempt %d/%d: %s. Retrying in %.2fs...",
                attempt + 1,
                max_retries,
                exc,
                wait_time,
            )
            time.sleep(wait_time)

        except requests.exceptions.Timeout as exc:
            wait_time = base_wait * (2 ** attempt) + random.uniform(0.0, 1.0)
            logger.warning(
                "Timeout on attempt %d/%d: %s. Retrying in %.2fs...",
                attempt + 1,
                max_retries,
                exc,
                wait_time,
            )
            time.sleep(wait_time)

    logger.error("Max retries (%d) exceeded. Giving up on this batch.", max_retries)
    return None


def fetch_works_pages(
    start_cursor: str = "*",
    target: int = 500_000,
) -> Generator[tuple[list[dict], int], None, None]:
    """
    Generator that yields (batch, total_fetched_so_far) tuples.

    Iterates through OpenAlex /works using cursor-based pagination until
    `target` works have been fetched or the API signals no more pages.

    Args:
        start_cursor: The initial cursor value ('*' for the first page).
        target:       Stop after this many works have been yielded.

    Yields:
        A tuple of (list_of_raw_work_dicts, cumulative_total).
    """
    url = f"{config.OPENALEX_BASE_URL}/works"
    cursor: Optional[str] = start_cursor
    total_fetched = 0

    logger.info(
        "Starting extraction from OpenAlex /works (target=%d, batch_size=%d).",
        target,
        config.BATCH_SIZE,
    )

    while cursor and total_fetched < target:
        params = _build_params(cursor)
        data = _fetch_with_backoff(url, params)

        if data is None:
            logger.error("Failed to fetch batch at cursor=%s. Stopping.", cursor)
            break

        results: list[dict] = data.get("results", [])
        meta: dict = data.get("meta", {})
        next_cursor: Optional[str] = meta.get("next_cursor")

        if not results:
            logger.info("Empty results page received. Extraction complete.")
            break

        # Trim the last batch if it would exceed the target
        remaining = target - total_fetched
        if len(results) > remaining:
            results = results[:remaining]

        total_fetched += len(results)
        logger.info(
            "Fetched batch of %d works | Total so far: %d/%d | cursor: %s...",
            len(results),
            total_fetched,
            target,
            (next_cursor or "")[:30],
        )

        yield results, total_fetched

        # Advance cursor
        cursor = next_cursor
        if not cursor:
            logger.info("No next_cursor returned. API has no more pages.")
            break

    logger.info("Extraction complete. Total works fetched: %d.", total_fetched)
