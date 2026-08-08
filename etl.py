"""
etl.py - Main ETL orchestrator for the OpenAlex Academic Publications Pipeline.

Usage:
    python etl.py

Environment:
    Copy .env.example → .env and fill in your PostgreSQL credentials.

This script:
  1. Creates the snowflake schema tables (idempotent).
  2. Fetches works from the OpenAlex API using cursor-based pagination.
  3. Transforms raw JSON into structured records.
  4. Loads all entities into PostgreSQL with SCD Type 2 for authors.
  5. Continues until TARGET_WORKS unique works have been loaded.
"""

import logging
import sys
import time
from datetime import datetime

import config
import db
import schema
import extractor
import transformer
import loader

# ─── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("etl.main")


def print_banner() -> None:
    """Print a startup banner with configuration summary."""
    banner = f"""
╔══════════════════════════════════════════════════════════════════╗
║         OpenAlex ETL Pipeline — Snowflake Schema in PostgreSQL  ║
╠══════════════════════════════════════════════════════════════════╣
║  Target works  : {config.TARGET_WORKS:<47,d}║
║  Batch size    : {config.BATCH_SIZE:<47,d}║
║  Database      : {config.DB_NAME + '@' + config.DB_HOST:<47s}║
║  Max retries   : {config.MAX_RETRIES:<47d}║
║  Started at    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<47s}║
╚══════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def process_batch(raw_works: list[dict]) -> int:
    """
    Transform and load a single batch of raw works.

    Returns the number of works successfully loaded in this batch.
    """
    transformed = transformer.transform_batch(raw_works)
    if not transformed:
        return 0
    return loader.load_batch(transformed)


def main() -> None:
    """
    Main ETL orchestration function.

    Flow:
      create_tables → loop(fetch → transform → load) → report
    """
    print_banner()
    pipeline_start = time.time()

    # ── Step 1: Schema creation ────────────────────────────────────────────────
    logger.info("=== PHASE 1: Schema Setup ===")
    # Verify DB connectivity before proceeding
    if not db.ping_connection():
        logger.error("Cannot reach the database. Check your .env credentials.")
        sys.exit(1)
    try:
        schema.create_tables()
    except Exception:
        logger.exception("Fatal error during schema creation. Aborting.")
        sys.exit(1)

    # ── Step 2: Check how many works are already loaded (resume support) ───────
    existing_count = loader.get_current_work_count()
    logger.info("Current fact_works count: %d", existing_count)

    if existing_count >= config.TARGET_WORKS:
        logger.info(
            "Target of %d already met (%d rows in fact_works). "
            "Nothing to do — pipeline is idempotent.",
            config.TARGET_WORKS,
            existing_count,
        )
        db.close_connection()
        return

    remaining = config.TARGET_WORKS - existing_count
    logger.info(
        "Need to load %d more works to reach target of %d.",
        remaining,
        config.TARGET_WORKS,
    )

    # ── Step 3: Extract → Transform → Load loop ───────────────────────────────
    logger.info("=== PHASE 2: Extract → Transform → Load ===")

    total_loaded_this_run = 0
    batch_num = 0

    try:
        for raw_batch, api_total in extractor.fetch_works_pages(
            start_cursor="*",
            target=remaining,
        ):
            batch_num += 1
            batch_start = time.time()

            loaded_this_batch = process_batch(raw_batch)

            total_loaded_this_run += loaded_this_batch
            elapsed_batch = time.time() - batch_start
            elapsed_total = time.time() - pipeline_start

            grand_total = existing_count + total_loaded_this_run
            pct = (grand_total / config.TARGET_WORKS) * 100

            logger.info(
                "Batch %4d | +%3d loaded | Total: %7d / %7d (%5.1f%%) "
                "| Batch: %.1fs | Elapsed: %.0fs",
                batch_num,
                loaded_this_batch,
                grand_total,
                config.TARGET_WORKS,
                pct,
                elapsed_batch,
                elapsed_total,
            )

            if grand_total >= config.TARGET_WORKS:
                logger.info("Target of %d works reached!", config.TARGET_WORKS)
                break

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user. Progress has been saved.")
    except Exception:
        logger.exception("Unexpected error in ETL loop. Partial data has been saved.")
        raise
    finally:
        db.close_connection()

    # ── Step 4: Final report ──────────────────────────────────────────────────
    total_elapsed = time.time() - pipeline_start
    final_count = existing_count + total_loaded_this_run

    logger.info("=== PHASE 3: Final Report ===")
    logger.info("Works loaded this run : %d", total_loaded_this_run)
    logger.info("Total works in DB     : %d", final_count)
    logger.info("Target                : %d", config.TARGET_WORKS)
    logger.info("Total elapsed time    : %.1f seconds (%.1f minutes)",
                total_elapsed, total_elapsed / 60)

    if final_count >= config.TARGET_WORKS:
        logger.info("✓ SUCCESS — Target of %d works achieved!", config.TARGET_WORKS)
    else:
        logger.warning(
            "Target not fully reached. %d / %d works loaded. "
            "Re-run the pipeline to continue.",
            final_count,
            config.TARGET_WORKS,
        )


if __name__ == "__main__":
    main()
