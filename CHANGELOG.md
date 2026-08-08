# Changelog

All notable changes to the OpenAlex ETL Pipeline are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-08-08

### Added
- Full ETL pipeline ingesting 500,000+ academic works from OpenAlex API
- Snowflake schema with 7 tables: `dim_institutions`, `dim_authors`, `dim_concepts`,
  `fact_works`, `bridge_work_authors`, `bridge_work_concepts`, `bridge_work_citations`
- SCD Type 2 implementation on `dim_authors` for historical institution tracking
- Cursor-based pagination for OpenAlex `/works` endpoint (supports >10,000 records)
- Exponential backoff with jitter for 429/503 rate-limit handling
- Idempotent `INSERT ... ON CONFLICT` throughout all loaders
- In-memory author cache per batch for O(1) SCD lookups
- Auto-creates all tables and indexes on first run (no manual DDL)
- Configurable via `.env` file — no hardcoded credentials
- Dual logging: stdout + rotating log file (`etl_pipeline.log`)
- `demo_run.py` for quick test runs without modifying config
- `verify.py` for post-run data integrity checks

### Configuration
- `TARGET_WORKS` — number of works to load (default: 500,000)
- `BATCH_SIZE` — API batch size, capped at 200 (default: 200)
- `MAX_RETRIES` — retries on rate-limit errors (default: 7)
- `BASE_WAIT_SECONDS` — exponential backoff base (default: 1s)
- `LOG_LEVEL` — logging verbosity (default: INFO)
- `DB_CONNECT_TIMEOUT` — PostgreSQL connection timeout in seconds (default: 10)
