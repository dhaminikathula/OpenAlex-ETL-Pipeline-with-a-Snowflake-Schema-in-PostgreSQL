"""
schema.py - Snowflake schema DDL and table creation for the OpenAlex ETL Pipeline.

All seven tables are created via CREATE TABLE IF NOT EXISTS so the pipeline
is fully self-contained and safe to re-run. The schema implements:

  - dim_institutions  : Institution dimension
  - dim_authors       : Author dimension with SCD Type 2 columns
  - dim_concepts      : Concept/topic dimension
  - fact_works        : Central fact table for academic works
  - bridge_work_authors   : Work ↔ Author many-to-many
  - bridge_work_concepts  : Work ↔ Concept many-to-many
  - bridge_work_citations : Work ↔ Work citation many-to-many
"""

import logging
import db

logger = logging.getLogger(__name__)

# ─── DDL Statements ───────────────────────────────────────────────────────────

DDL_DIM_INSTITUTIONS = """
CREATE TABLE IF NOT EXISTS dim_institutions (
    institution_id  VARCHAR(255) PRIMARY KEY,
    display_name    TEXT,
    ror             VARCHAR(255),
    country_code    VARCHAR(2),
    type            VARCHAR(255)
);
"""

DDL_DIM_AUTHORS = """
CREATE TABLE IF NOT EXISTS dim_authors (
    author_key      SERIAL          PRIMARY KEY,
    author_id       VARCHAR(255)    NOT NULL,
    display_name    TEXT,
    institution_id  VARCHAR(255)    REFERENCES dim_institutions(institution_id),
    start_date      DATE            NOT NULL,
    end_date        DATE,
    is_current      BOOLEAN         NOT NULL
);
"""

# Index for fast SCD lookups by natural key + currency flag
DDL_IDX_AUTHORS_CURRENT = """
CREATE INDEX IF NOT EXISTS idx_dim_authors_current
    ON dim_authors (author_id, is_current);
"""

DDL_DIM_CONCEPTS = """
CREATE TABLE IF NOT EXISTS dim_concepts (
    concept_id      VARCHAR(255) PRIMARY KEY,
    display_name    TEXT,
    level           INTEGER
);
"""

DDL_FACT_WORKS = """
CREATE TABLE IF NOT EXISTS fact_works (
    work_id             VARCHAR(255)    PRIMARY KEY,
    title               TEXT            NOT NULL,
    publication_year    INTEGER,
    cited_by_count      INTEGER         NOT NULL DEFAULT 0,
    type                VARCHAR(255)
);
"""

DDL_BRIDGE_WORK_AUTHORS = """
CREATE TABLE IF NOT EXISTS bridge_work_authors (
    work_id     VARCHAR(255)    REFERENCES fact_works(work_id)    ON DELETE CASCADE,
    author_key  INTEGER         REFERENCES dim_authors(author_key) ON DELETE CASCADE,
    PRIMARY KEY (work_id, author_key)
);
"""

DDL_BRIDGE_WORK_CONCEPTS = """
CREATE TABLE IF NOT EXISTS bridge_work_concepts (
    work_id     VARCHAR(255)    REFERENCES fact_works(work_id)   ON DELETE CASCADE,
    concept_id  VARCHAR(255)    REFERENCES dim_concepts(concept_id) ON DELETE CASCADE,
    PRIMARY KEY (work_id, concept_id)
);
"""

DDL_BRIDGE_WORK_CITATIONS = """
CREATE TABLE IF NOT EXISTS bridge_work_citations (
    citing_work_id  VARCHAR(255)    REFERENCES fact_works(work_id) ON DELETE CASCADE,
    cited_work_id   VARCHAR(255)    REFERENCES fact_works(work_id) ON DELETE CASCADE,
    PRIMARY KEY (citing_work_id, cited_work_id)
);
"""

# Ordered so that FK dependencies are always created first
_ALL_DDL = [
    ("dim_institutions",     DDL_DIM_INSTITUTIONS),
    ("dim_authors",          DDL_DIM_AUTHORS),
    ("idx_dim_authors",      DDL_IDX_AUTHORS_CURRENT),
    ("dim_concepts",         DDL_DIM_CONCEPTS),
    ("fact_works",           DDL_FACT_WORKS),
    ("bridge_work_authors",  DDL_BRIDGE_WORK_AUTHORS),
    ("bridge_work_concepts", DDL_BRIDGE_WORK_CONCEPTS),
    ("bridge_work_citations",DDL_BRIDGE_WORK_CITATIONS),
]


def create_tables() -> None:
    """
    Execute all DDL statements to create the snowflake schema tables and
    supporting indexes if they do not already exist.

    This function is idempotent; it is safe to call on every pipeline run.
    """
    logger.info("Creating snowflake schema tables (IF NOT EXISTS)...")
    with db.get_standard_cursor() as cur:
        for name, ddl in _ALL_DDL:
            logger.debug("Executing DDL for: %s", name)
            cur.execute(ddl)
    logger.info("Schema setup complete — all 7 tables and indexes are ready.")
