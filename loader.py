"""
loader.py - Database loading logic for the OpenAlex ETL Pipeline.

Implements all INSERT / UPSERT / SCD Type 2 operations for the seven
snowflake schema tables.  Every function is designed to be:

  - Idempotent : Safe to call multiple times with the same data.
  - Atomic     : SCD Type 2 UPDATE + INSERT happen in one transaction.
  - Efficient  : Batch operations via executemany() where possible.
                 Author SCD logic uses an in-memory cache to minimise
                 per-author SELECT queries.

Public API
----------
load_batch(transformed_works)  ← call this for each batch from extractor
"""

import logging
from datetime import date
from typing import Optional

import psycopg2.extras

import db

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_institutions(cur, records: list[dict]) -> None:
    """
    Insert institutions, ignoring conflicts on the primary key.
    A single executemany call handles the entire batch.
    """
    if not records:
        return

    sql = """
        INSERT INTO dim_institutions
            (institution_id, display_name, ror, country_code, type)
        VALUES
            (%(institution_id)s, %(display_name)s, %(ror)s,
             %(country_code)s, %(type)s)
        ON CONFLICT (institution_id) DO NOTHING
    """
    cur.executemany(sql, records)


def _upsert_concepts(cur, records: list[dict]) -> None:
    """
    Insert concepts, ignoring conflicts on the primary key.
    """
    if not records:
        return

    sql = """
        INSERT INTO dim_concepts
            (concept_id, display_name, level)
        VALUES
            (%(concept_id)s, %(display_name)s, %(level)s)
        ON CONFLICT (concept_id) DO NOTHING
    """
    cur.executemany(sql, records)


def _fetch_current_authors(cur, author_ids: list[str]) -> dict[str, dict]:
    """
    Fetch the current dim_authors record for each author_id in the list.

    Returns a dict mapping author_id → current record dict with keys:
        author_key, author_id, institution_id, start_date, end_date, is_current
    """
    if not author_ids:
        return {}

    sql = """
        SELECT author_key, author_id, institution_id, start_date, end_date, is_current
        FROM   dim_authors
        WHERE  author_id = ANY(%s)
          AND  is_current = TRUE
    """
    cur.execute(sql, (list(author_ids),))
    rows = cur.fetchall()
    # rows are RealDictRow when cursor is RealDictCursor; convert to plain dict
    return {row["author_id"]: dict(row) for row in rows}


def _apply_scd2_authors(cur, author_records: list[dict]) -> dict[str, int]:
    """
    Apply SCD Type 2 logic for a batch of author records.

    For each author:
      - NEW author       → INSERT with is_current=True
      - Same institution → no-op (return existing author_key)
      - New institution  → expire old record, INSERT new record

    Returns a dict mapping author_id → author_key (the current surrogate key
    after all SCD operations for this batch).

    NOTE: The caller must commit the transaction after this function returns.
    """
    if not author_records:
        return {}

    # Collect all unique author_ids we need to process
    unique_ids = list({r["author_id"] for r in author_records})

    # Single query to fetch all current records we care about
    existing = _fetch_current_authors(cur, unique_ids)

    # Result mapping: author_id → author_key
    author_key_map: dict[str, int] = {}

    # Deduplicate: if the same author appears twice in a batch (different works),
    # only the first occurrence drives SCD logic; subsequent ones reuse the key.
    processed: dict[str, int] = {}

    for rec in author_records:
        aid = rec["author_id"]

        # If already processed in this batch, just reuse the key
        if aid in processed:
            author_key_map[aid] = processed[aid]
            continue

        new_inst   = rec["institution_id"]
        pub_date   = rec["pub_date"]

        if aid not in existing:
            # ── Case 1: New author ──────────────────────────────────────────
            cur.execute(
                """
                INSERT INTO dim_authors
                    (author_id, display_name, institution_id,
                     start_date, end_date, is_current)
                VALUES (%s, %s, %s, %s, NULL, TRUE)
                RETURNING author_key
                """,
                (aid, rec["display_name"], new_inst, pub_date),
            )
            author_key = cur.fetchone()["author_key"]

        else:
            current = existing[aid]
            current_inst = current["institution_id"]

            if current_inst == new_inst:
                # ── Case 2: Institution unchanged → no-op ───────────────────
                author_key = current["author_key"]

            else:
                # ── Case 3: Institution changed → SCD Type 2 ────────────────
                # Step 1: Expire the old record
                cur.execute(
                    """
                    UPDATE dim_authors
                    SET    end_date   = %s,
                           is_current = FALSE
                    WHERE  author_key = %s
                    """,
                    (pub_date, current["author_key"]),
                )

                # Step 2: Insert new current record
                cur.execute(
                    """
                    INSERT INTO dim_authors
                        (author_id, display_name, institution_id,
                         start_date, end_date, is_current)
                    VALUES (%s, %s, %s, %s, NULL, TRUE)
                    RETURNING author_key
                    """,
                    (aid, rec["display_name"], new_inst, pub_date),
                )
                author_key = cur.fetchone()["author_key"]

                # Update our local cache so later records in this batch
                # see the new state without re-querying
                existing[aid] = {
                    "author_key":     author_key,
                    "author_id":      aid,
                    "institution_id": new_inst,
                    "start_date":     pub_date,
                    "end_date":       None,
                    "is_current":     True,
                }

        author_key_map[aid] = author_key
        processed[aid]      = author_key

    return author_key_map


def _upsert_works(cur, work_records: list[dict]) -> None:
    """
    Insert or update fact_works records (upsert on work_id).

    On conflict, we update cited_by_count (which can change over time)
    while preserving stable columns.
    """
    if not work_records:
        return

    sql = """
        INSERT INTO fact_works
            (work_id, title, publication_year, cited_by_count, type)
        VALUES
            (%(work_id)s, %(title)s, %(publication_year)s,
             %(cited_by_count)s, %(type)s)
        ON CONFLICT (work_id) DO UPDATE
            SET cited_by_count = EXCLUDED.cited_by_count,
                type           = COALESCE(EXCLUDED.type, fact_works.type)
    """
    cur.executemany(sql, work_records)


def _upsert_bridge_work_authors(
    cur, work_id: str, author_keys: list[int]
) -> None:
    """Insert work-author bridge rows, ignoring duplicates."""
    if not author_keys:
        return

    records = [{"work_id": work_id, "author_key": k} for k in author_keys]
    sql = """
        INSERT INTO bridge_work_authors (work_id, author_key)
        VALUES (%(work_id)s, %(author_key)s)
        ON CONFLICT DO NOTHING
    """
    cur.executemany(sql, records)


def _upsert_bridge_work_concepts(
    cur, work_id: str, concept_ids: list[str]
) -> None:
    """Insert work-concept bridge rows, ignoring duplicates."""
    if not concept_ids:
        return

    records = [{"work_id": work_id, "concept_id": cid} for cid in concept_ids]
    sql = """
        INSERT INTO bridge_work_concepts (work_id, concept_id)
        VALUES (%(work_id)s, %(concept_id)s)
        ON CONFLICT DO NOTHING
    """
    cur.executemany(sql, records)


def _upsert_bridge_work_citations(
    cur, work_id: str, cited_ids: list[str], known_work_ids: set[str]
) -> None:
    """
    Insert work-citation bridge rows only for cited works that already
    exist in fact_works (to satisfy the FK constraint).
    """
    valid_citations = [
        {"citing_work_id": work_id, "cited_work_id": cid}
        for cid in cited_ids
        if cid in known_work_ids
    ]
    if not valid_citations:
        return

    sql = """
        INSERT INTO bridge_work_citations (citing_work_id, cited_work_id)
        VALUES (%(citing_work_id)s, %(cited_work_id)s)
        ON CONFLICT DO NOTHING
    """
    cur.executemany(sql, valid_citations)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load_batch(transformed_works: list[dict]) -> int:
    """
    Load a batch of transformed work records into the snowflake schema.

    Processing order:
      1. dim_institutions  (no FK deps)
      2. dim_concepts      (no FK deps)
      3. dim_authors       (FK → dim_institutions, SCD Type 2)
      4. fact_works        (no FK deps on dims)
      5. bridge_work_authors  (FK → fact_works, dim_authors)
      6. bridge_work_concepts (FK → fact_works, dim_concepts)
      7. bridge_work_citations (FK → fact_works self-join; only valid refs)

    Returns the number of works successfully upserted.
    """
    if not transformed_works:
        return 0

    # ── Collect all entities for bulk operations ──────────────────────────────
    all_institutions: list[dict] = []
    all_concepts:     list[dict] = []
    all_authors:      list[dict] = []
    all_works:        list[dict] = []

    # Map work_id → (author_ids, concept_ids, cited_ids) for bridge tables
    work_details: dict[str, dict] = {}

    seen_insts    = set()
    seen_concepts = set()
    seen_authors  = set()

    for tw in transformed_works:
        # Institutions
        for inst in tw["institutions"]:
            if inst["institution_id"] not in seen_insts:
                all_institutions.append(inst)
                seen_insts.add(inst["institution_id"])

        # Concepts
        for concept in tw["concepts"]:
            if concept["concept_id"] not in seen_concepts:
                all_concepts.append(concept)
                seen_concepts.add(concept["concept_id"])

        # Authors
        for author in tw["authors"]:
            # Allow repeated author_ids (SCD logic deduplicates internally)
            all_authors.append(author)

        # Works
        all_works.append(tw["work"])

        # Bridge data per work
        work_details[tw["work"]["work_id"]] = {
            "author_ids":  tw["author_ids"],
            "concept_ids": tw["concept_ids"],
            "cited_ids":   tw["cited_ids"],
        }

    conn = db.get_connection()

    # Use a RealDictCursor for the entire batch transaction
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            # Step 1: Dimensions with no intra-batch dependencies
            _upsert_institutions(cur, all_institutions)
            _upsert_concepts(cur, all_concepts)

            # Step 2: SCD Type 2 authors — returns author_id → author_key map
            author_key_map = _apply_scd2_authors(cur, all_authors)

            # Step 3: Fact table
            _upsert_works(cur, all_works)

            # Step 4: Bridge tables
            loaded_work_ids = {w["work_id"] for w in all_works}

            for work_id, details in work_details.items():
                # Bridge: work → authors
                author_keys = [
                    author_key_map[aid]
                    for aid in details["author_ids"]
                    if aid in author_key_map
                ]
                _upsert_bridge_work_authors(cur, work_id, author_keys)

                # Bridge: work → concepts
                _upsert_bridge_work_concepts(cur, work_id, details["concept_ids"])

                # Bridge: work → cited works (only those already in fact_works)
                _upsert_bridge_work_citations(
                    cur, work_id, details["cited_ids"], loaded_work_ids
                )

            conn.commit()

        except Exception:
            conn.rollback()
            logger.exception("Error loading batch — transaction rolled back.")
            raise

    return len(all_works)


def get_current_work_count() -> int:
    """Return the current number of rows in fact_works."""
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM fact_works")
        row = cur.fetchone()
        return int(row["cnt"]) if row else 0
