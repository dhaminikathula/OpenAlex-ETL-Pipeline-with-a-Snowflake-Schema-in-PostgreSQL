"""
demo_run.py - Quick demo runner for video recording purposes.

Loads only 1,000 works (about 5 minutes) so you can record the pipeline
working in real-time. After recording, run the full pipeline with etl.py.

Usage:
    python demo_run.py
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

# Override target for demo
DEMO_TARGET = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("demo")


def main():
    print("""
╔══════════════════════════════════════════════════╗
║     OpenAlex ETL Pipeline — DEMO MODE (1,000)   ║
╚══════════════════════════════════════════════════╝
""")
    start = time.time()

    # Step 1: Create tables
    logger.info("Creating snowflake schema tables...")
    schema.create_tables()
    logger.info("All 7 tables ready ✓")

    # Step 2: ETL loop
    logger.info("Starting extraction (demo: %d works)...", DEMO_TARGET)
    total = 0

    for raw_batch, _ in extractor.fetch_works_pages(
        start_cursor="*", target=DEMO_TARGET
    ):
        transformed = transformer.transform_batch(raw_batch)
        loaded = loader.load_batch(transformed)
        total += loaded
        pct = (total / DEMO_TARGET) * 100
        elapsed = time.time() - start
        logger.info(
            "  Progress: %4d / %4d works (%5.1f%%) | Elapsed: %.0fs",
            total, DEMO_TARGET, pct, elapsed
        )
        if total >= DEMO_TARGET:
            break

    db.close_connection()

    elapsed = time.time() - start
    print(f"""
╔══════════════════════════════════════════════════╗
║                DEMO COMPLETE ✓                   ║
╠══════════════════════════════════════════════════╣
║  Works loaded : {total:<33d}║
║  Time taken   : {elapsed:<30.1f}s║
╚══════════════════════════════════════════════════╝

Now run these SQL queries to verify:

  SELECT COUNT(*) FROM fact_works;
  SELECT COUNT(*) FROM dim_institutions;
  SELECT COUNT(*) FROM dim_authors;
  SELECT COUNT(*) FROM dim_concepts;
  SELECT COUNT(*) FROM bridge_work_authors;
  SELECT COUNT(*) FROM bridge_work_concepts;
  SELECT COUNT(*) FROM bridge_work_citations;

  -- SCD Type 2 check:
  SELECT author_id, COUNT(*) as versions
  FROM dim_authors
  GROUP BY author_id HAVING COUNT(*) > 1
  LIMIT 5;
""")


if __name__ == "__main__":
    main()
