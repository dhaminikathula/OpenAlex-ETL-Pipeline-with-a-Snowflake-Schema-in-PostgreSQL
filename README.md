# OpenAlex ETL Pipeline with a Snowflake Schema in PostgreSQL

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-316192?logo=postgresql)](https://postgresql.org)
[![psycopg2](https://img.shields.io/badge/psycopg2-2.9%2B-green)](https://pypi.org/project/psycopg2-binary/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade Python ETL pipeline that ingests **500,000+ academic works** from the [OpenAlex API](https://openalex.org) into a PostgreSQL data warehouse modelled as a **Snowflake Schema** with **SCD Type 2** historical tracking for author affiliations.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Model — Snowflake Schema](#data-model--snowflake-schema)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Core Concepts](#core-concepts)
  - [Cursor-based Pagination](#cursor-based-pagination)
  - [Exponential Backoff](#exponential-backoff)
  - [SCD Type 2 for Authors](#scd-type-2-for-authors)
  - [Idempotency](#idempotency)
- [Performance Notes](#performance-notes)
- [Verification](#verification)
- [FAQ](#faq)

---

## Overview

The OpenAlex ETL Pipeline fetches scholarly works data from the public [OpenAlex REST API](https://api.openalex.org/works), transforms it into a normalised relational model, and loads it into a PostgreSQL data warehouse. Key engineering features:

| Feature | Implementation |
|---|---|
| **Data Volume** | 500,000+ unique academic works |
| **API Handling** | Cursor pagination + exponential backoff (429 retry) |
| **Schema** | Snowflake Schema (7 tables) |
| **Historical Tracking** | SCD Type 2 on `dim_authors` |
| **Idempotency** | `INSERT ... ON CONFLICT` throughout |
| **Self-contained** | Auto-creates all tables on first run |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ETL Orchestrator (etl.py)                │
│                                                                 │
│   ┌─────────────┐   ┌─────────────────┐   ┌─────────────────┐  │
│   │ extractor.py│──▶│ transformer.py  │──▶│   loader.py     │  │
│   │             │   │                 │   │                 │  │
│   │ • Cursor    │   │ • Parse JSON    │   │ • SCD Type 2    │  │
│   │   pagination│   │ • Clean IDs     │   │   (authors)     │  │
│   │ • Exp.      │   │ • Handle nulls  │   │ • Bulk upserts  │  │
│   │   backoff   │   │ • Dedup entities│   │ • FK-safe       │  │
│   └──────┬──────┘   └─────────────────┘   └────────┬────────┘  │
│          │                                          │           │
└──────────│──────────────────────────────────────────│───────────┘
           │                                          │
    ┌──────▼──────┐                         ┌────────▼────────┐
    │ OpenAlex API│                         │   PostgreSQL    │
    │             │                         │                 │
    │ /works      │                         │ Snowflake Schema│
    │ cursor=*    │                         │ (7 tables)      │
    └─────────────┘                         └─────────────────┘
```

**Data flow per batch:**
1. `extractor.py` fetches 200 works per API call using cursor pagination
2. `transformer.py` parses raw JSON → typed dicts, normalising all entity IDs
3. `loader.py` writes to all 7 tables in dependency order within one transaction

---

## Data Model — Snowflake Schema

The schema follows a **snowflake pattern**: `dim_authors` is normalised off `dim_institutions` rather than embedding institution columns inline. This eliminates redundancy and reflects real-world affiliation changes via SCD Type 2.

```
                        ┌───────────────────────┐
                        │    dim_institutions    │
                        │───────────────────────│
                        │ institution_id (PK)   │
                        │ display_name          │
                        │ ror                   │
                        │ country_code          │
                        │ type                  │
                        └────────────┬──────────┘
                                     │ FK
                        ┌────────────▼──────────┐       ┌──────────────────────────┐
                        │      dim_authors       │       │       dim_concepts        │
                        │───────────────────────│       │──────────────────────────│
                        │ author_key (SERIAL PK)│       │ concept_id (PK)          │
                        │ author_id (natural key│       │ display_name             │
                        │ display_name          │       │ level                    │
                        │ institution_id (FK)   │       └────────────┬─────────────┘
                        │ start_date  ◄─ SCD2   │                    │
                        │ end_date    ◄─ SCD2   │                    │
                        │ is_current  ◄─ SCD2   │                    │
                        └────────────┬──────────┘                    │
                                     │                               │
              ┌──────────────────────▼──────────────────────────────▼─────┐
              │                        fact_works                          │
              │────────────────────────────────────────────────────────────│
              │ work_id (PK)  │ title │ publication_year │ cited_by_count │ type │
              └──────────┬────┘       └──────┬───────────┘                 │
                         │                   │                             │
          ┌──────────────▼────┐  ┌───────────▼──────────┐  ┌─────────────▼──────────────┐
          │ bridge_work_author│  │ bridge_work_concepts  │  │  bridge_work_citations      │
          │───────────────────│  │──────────────────────│  │────────────────────────────│
          │ work_id (FK, PK)  │  │ work_id (FK, PK)     │  │ citing_work_id (FK, PK)    │
          │ author_key (FK,PK)│  │ concept_id (FK, PK)  │  │ cited_work_id (FK, PK)     │
          └───────────────────┘  └──────────────────────┘  └────────────────────────────┘
```

### Table Descriptions

| Table | Type | Description |
|---|---|---|
| `dim_institutions` | Dimension | Academic institutions with ROR identifiers |
| `dim_authors` | Dimension (SCD2) | Authors with historical institution tracking |
| `dim_concepts` | Dimension | Academic topics and disciplines |
| `fact_works` | Fact | Central metrics for each academic publication |
| `bridge_work_authors` | Bridge | Many-to-many: works ↔ authors |
| `bridge_work_concepts` | Bridge | Many-to-many: works ↔ concepts |
| `bridge_work_citations` | Bridge | Many-to-many: works ↔ cited works |

---

## Project Structure

```
OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL/
├── etl.py            # 🚀 Main entry point — run this
├── config.py         # Environment variable management
├── db.py             # PostgreSQL connection management
├── schema.py         # CREATE TABLE DDL + schema setup
├── extractor.py      # OpenAlex API client with pagination & backoff
├── transformer.py    # Raw JSON → structured Python dicts
├── loader.py         # DB inserts, upserts, SCD Type 2 logic
├── requirements.txt  # Python dependencies
├── .env.example      # Environment variable template
├── etl_pipeline.log  # Runtime log (auto-generated)
└── README.md
```

---

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| Python | 3.10+ |
| PostgreSQL | 14+ |
| pip | 21+ |

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL.git
cd OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL
```

### 2. Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the PostgreSQL database

```sql
-- Connect to PostgreSQL as superuser
CREATE DATABASE openalex_dw;
CREATE USER openalex_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE openalex_dw TO openalex_user;
```

### 5. Configure environment variables

```bash
# Copy the example file
cp .env.example .env

# Edit .env with your actual values
notepad .env        # Windows
nano .env           # Linux / macOS
```

---

## Configuration

All configuration is managed via environment variables. Copy `.env.example` to `.env` and fill in your values:

| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_HOST` | No | `localhost` | PostgreSQL server hostname |
| `DB_PORT` | No | `5432` | PostgreSQL server port |
| `DB_NAME` | **Yes** | — | Target database name |
| `DB_USER` | **Yes** | — | PostgreSQL username |
| `DB_PASSWORD` | **Yes** | — | PostgreSQL password |
| `OPENALEX_EMAIL` | Recommended | — | Your email for OpenAlex polite pool |
| `TARGET_WORKS` | No | `500000` | Number of works to load |
| `BATCH_SIZE` | No | `200` | Works per API request (max 200) |
| `MAX_RETRIES` | No | `7` | Max retries on 429/503 errors |
| `BASE_WAIT_SECONDS` | No | `1` | Base wait for exponential backoff |

> **Note on `OPENALEX_EMAIL`**: Providing your email identifies your pipeline to OpenAlex and places you in the "polite pool," which has more generous rate limits. It is strongly recommended.

---

## Running the Pipeline

```bash
# Activate virtual environment (if not already active)
venv\Scripts\activate   # Windows
source venv/bin/activate # Linux/macOS

# Run the full ETL pipeline
python etl.py
```

### Expected output

```
╔══════════════════════════════════════════════════════════════════╗
║         OpenAlex ETL Pipeline — Snowflake Schema in PostgreSQL  ║
╠══════════════════════════════════════════════════════════════════╣
║  Target works  : 500,000                                        ║
║  Batch size    : 200                                            ║
║  Database      : openalex_dw@localhost                          ║
╚══════════════════════════════════════════════════════════════════╝

2026-01-01 10:00:00 [INFO    ] etl.main - === PHASE 1: Schema Setup ===
2026-01-01 10:00:01 [INFO    ] schema - Schema setup complete — all 7 tables and indexes are ready.
2026-01-01 10:00:01 [INFO    ] etl.main - === PHASE 2: Extract → Transform → Load ===
2026-01-01 10:00:03 [INFO    ] etl.main - Batch    1 |  +200 loaded | Total:     200 / 500,000 ( 0.0%) | ...
2026-01-01 10:00:05 [INFO    ] etl.main - Batch    2 |  +200 loaded | Total:     400 / 500,000 ( 0.1%) | ...
...
2026-01-01 XX:XX:XX [INFO    ] etl.main - ✓ SUCCESS — Target of 500,000 works achieved!
```

A log file `etl_pipeline.log` is also written to the current directory for post-run analysis.

### Resuming a stopped run

The pipeline is **idempotent**. If interrupted, simply re-run `python etl.py`. It will check the current `fact_works` count and continue from where it left off (though it cannot resume mid-cursor; it will restart the API cursor from the beginning, skipping already-loaded works via `ON CONFLICT DO NOTHING`).

---

## Core Concepts

### Cursor-based Pagination

The OpenAlex API uses cursor pagination for large result sets. Unlike page-number pagination, each response returns a `next_cursor` token that must be used in the next request. This is required for datasets > 10,000 records.

```
Request 1:  GET /works?per-page=200&cursor=*
Response 1: { "meta": { "next_cursor": "abc123..." }, "results": [...] }

Request 2:  GET /works?per-page=200&cursor=abc123...
Response 2: { "meta": { "next_cursor": "def456..." }, "results": [...] }

...continues until next_cursor is null
```

### Exponential Backoff

When a `429 Too Many Requests` response is received, the pipeline automatically retries with increasing wait times:

```
Attempt 1: wait = 1s  × 2⁰ + jitter ≈ 1.3s
Attempt 2: wait = 1s  × 2¹ + jitter ≈ 2.7s
Attempt 3: wait = 1s  × 2² + jitter ≈ 4.5s
Attempt 4: wait = 1s  × 2³ + jitter ≈ 8.8s
...up to MAX_RETRIES
```

Random jitter prevents the "thundering herd" problem if multiple pipeline instances run simultaneously.

### SCD Type 2 for Authors

The `dim_authors` table implements **Slowly Changing Dimension Type 2** to track historical changes in an author's institutional affiliation.

**When an author changes institution:**

```
Before:
┌────────────┬───────────┬─────────────────────┬────────────┬──────────┬────────────┐
│ author_key │ author_id │ institution_id       │ start_date │ end_date │ is_current │
├────────────┼───────────┼─────────────────────┼────────────┼──────────┼────────────┤
│     42     │  A123     │ MIT                 │ 2020-01-01 │  NULL    │   TRUE     │
└────────────┴───────────┴─────────────────────┴────────────┴──────────┴────────────┘

After (author moved to Stanford):
┌────────────┬───────────┬─────────────────────┬────────────┬────────────┬────────────┐
│ author_key │ author_id │ institution_id       │ start_date │ end_date   │ is_current │
├────────────┼───────────┼─────────────────────┼────────────┼────────────┼────────────┤
│     42     │  A123     │ MIT                 │ 2020-01-01 │ 2023-06-15 │  FALSE     │  ← expired
│     99     │  A123     │ Stanford            │ 2023-06-15 │ NULL       │  TRUE      │  ← new current
└────────────┴───────────┴─────────────────────┴────────────┴────────────┴────────────┘
```

This enables queries like: *"Which papers did author A publish while at MIT?"*

### Idempotency

Every insert uses `ON CONFLICT DO NOTHING` (dimensions) or `ON CONFLICT DO UPDATE` (fact_works, to refresh `cited_by_count`). Running the pipeline multiple times produces no duplicate rows. The SCD Type 2 logic is also idempotent: if the same author+institution combination is seen again, no new record is created.

---

## Performance Notes

- **Batch size of 200** is the API maximum — maximises throughput per HTTP round trip.
- **Single connection** with `executemany()` for bulk inserts minimises DB overhead.
- **In-memory author cache** per batch avoids N+1 SELECT queries for SCD lookups.
- **Index on `dim_authors(author_id, is_current)`** makes SCD lookups O(log n).
- **Main bottleneck** is API rate limits (~3-10 req/s in polite pool). Expect 8–24 hours for 500,000 works depending on API conditions.

---

## Verification

After the pipeline completes, run these SQL queries to verify correctness:

```sql
-- 1. Target volume
SELECT COUNT(*) AS total_works FROM fact_works;
-- Must be >= 500,000

-- 2. Dimensions populated
SELECT COUNT(*) FROM dim_institutions;   -- Must be > 0
SELECT COUNT(*) FROM dim_authors;        -- Must be > 0
SELECT COUNT(*) FROM dim_concepts;       -- Must be > 0

-- 3. Bridge tables populated
SELECT COUNT(*) FROM bridge_work_authors;   -- Must be > 0
SELECT COUNT(*) FROM bridge_work_concepts;  -- Must be > 0
SELECT COUNT(*) FROM bridge_work_citations; -- Must be > 0

-- 4. SCD Type 2 integrity: each author has at most one current record
SELECT author_id, COUNT(*) AS current_count
FROM   dim_authors
WHERE  is_current = TRUE
GROUP  BY author_id
HAVING COUNT(*) > 1;
-- Must return 0 rows

-- 5. SCD authors with history
SELECT author_id, COUNT(*) AS total_versions
FROM   dim_authors
GROUP  BY author_id
HAVING COUNT(*) > 1
LIMIT  10;
-- Shows authors who changed institutions

-- 6. FK integrity: all dim_authors.institution_id exists in dim_institutions
SELECT COUNT(*)
FROM   dim_authors da
LEFT JOIN dim_institutions di ON da.institution_id = di.institution_id
WHERE  da.institution_id IS NOT NULL
  AND  di.institution_id IS NULL;
-- Must return 0
```

---

## FAQ

**Q: How long does it take to load 500,000 works?**

It depends on API rate limits. Expect 8–24 hours. Providing `OPENALEX_EMAIL` in your `.env` grants polite pool access with better limits. The pipeline logs progress continuously and can be safely interrupted and resumed.

**Q: Can I run this again without creating duplicates?**

Yes. The pipeline is fully idempotent. Re-running it will skip already-loaded works and correctly apply SCD Type 2 logic to any authors whose institution has changed since the last run.

**Q: Some works have no title / no publication year — is that okay?**

Works without a `work_id` or `title` are skipped (title is `NOT NULL` in the schema). All other fields gracefully accept `NULL`.

**Q: How do I target a subset (e.g., only 1,000 works for testing)?**

Set `TARGET_WORKS=1000` in your `.env` file.

**Q: What if my PostgreSQL connection drops mid-run?**

Each batch is wrapped in a transaction. A dropped connection will roll back the current batch only. Re-run `python etl.py` to continue; the pipeline will resume from the last committed state.

---

## License

MIT — see [LICENSE](LICENSE) for details.