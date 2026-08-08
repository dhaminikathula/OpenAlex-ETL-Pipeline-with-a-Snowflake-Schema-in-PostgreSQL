"""
transformer.py - Data transformation for the OpenAlex ETL Pipeline.

Parses raw OpenAlex work JSON objects into clean, structured Python dicts
ready for database insertion. All None / missing fields are handled
gracefully so the pipeline never crashes on sparse API data.

Each public function accepts a single raw work dict and returns a
collection of typed records for one entity type.
"""

import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Default date used when a work has no publication_date or publication_year
_FALLBACK_DATE = date(1900, 1, 1)

# Maximum title length stored in the database
_TITLE_MAX_LENGTH = 5000


def _parse_date(raw: Optional[str], year: Optional[int]) -> date:
    """
    Convert an ISO date string (YYYY-MM-DD) to a Python date object.
    Falls back to Jan 1 of publication_year, then to _FALLBACK_DATE.
    """
    if raw:
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass
    if year:
        try:
            return date(int(year), 1, 1)
        except ValueError:
            pass
    return _FALLBACK_DATE


def _clean_id(raw_id: Optional[str]) -> Optional[str]:
    """
    Normalise an OpenAlex entity ID URL to a short identifier.
    'https://openalex.org/W12345' → 'W12345'
    Also accepts already-short IDs.
    """
    if not raw_id:
        return None
    # Strip trailing whitespace/newlines
    raw_id = raw_id.strip()
    # Return just the path component if it looks like a full URL
    if raw_id.startswith("https://openalex.org/"):
        return raw_id.split("/")[-1]
    return raw_id


# ─── Entity extractors ────────────────────────────────────────────────────────

def extract_institution(authorship: dict) -> Optional[dict]:
    """
    Extract institution data from a single authorship entry.

    Returns a dict suitable for inserting into dim_institutions, or None
    if no institution is present in the authorship.
    """
    institutions = authorship.get("institutions") or []
    if not institutions:
        return None

    # Use the first listed institution (primary affiliation)
    inst = institutions[0]
    inst_id = _clean_id(inst.get("id"))
    if not inst_id:
        return None

    return {
        "institution_id": inst_id,
        "display_name":   inst.get("display_name"),
        "ror":            inst.get("ror"),
        "country_code":   (inst.get("country_code") or "")[:2] or None,
        "type":           inst.get("type"),
    }


def extract_author(authorship: dict, pub_date: date) -> Optional[dict]:
    """
    Extract author data from a single authorship entry.

    Returns a dict with keys:
      author_id, display_name, institution_id, pub_date

    Used by the loader to drive SCD Type 2 logic.
    Returns None if the authorship has no author ID.
    """
    author_info = authorship.get("author") or {}
    author_id = _clean_id(author_info.get("id"))
    if not author_id:
        return None

    # Resolve institution_id (may be None if author has no affiliation)
    institution_id: Optional[str] = None
    institutions = authorship.get("institutions") or []
    if institutions:
        institution_id = _clean_id((institutions[0] or {}).get("id"))

    return {
        "author_id":      author_id,
        "display_name":   author_info.get("display_name"),
        "institution_id": institution_id,
        "pub_date":       pub_date,
    }


def extract_concept(concept: dict) -> Optional[dict]:
    """
    Extract concept data from a single concept entry on a work.

    Returns a dict suitable for inserting into dim_concepts, or None
    if the concept has no ID.
    """
    concept_id = _clean_id(concept.get("id"))
    if not concept_id:
        return None

    return {
        "concept_id":    concept_id,
        "display_name":  concept.get("display_name"),
        "level":         concept.get("level"),
    }


def transform_work(raw_work: dict) -> Optional[dict]:
    """
    Transform a single raw OpenAlex work dict into all entity records.

    Returns a dict with keys:
      work       : fact_works record dict (or None if no valid work_id/title)
      institutions: list of dim_institutions dicts
      authors    : list of author dicts (for SCD logic)
      concepts   : list of dim_concepts dicts
      author_ids : ordered list of author_id strings (natural keys)
      concept_ids: list of concept_id strings
      cited_ids  : list of work_id strings (referenced_works)

    Returns None if the raw work cannot be parsed.
    """
    work_id = _clean_id(raw_work.get("id"))
    title   = raw_work.get("title") or raw_work.get("display_name")

    if not work_id or not title:
        logger.debug("Skipping work with missing id or title: %s", raw_work.get("id"))
        return None

    pub_year  = raw_work.get("publication_year")
    pub_date  = _parse_date(raw_work.get("publication_date"), pub_year)
    cited_by  = raw_work.get("cited_by_count") or 0
    work_type = raw_work.get("type")

    # ── Fact record ──────────────────────────────────────────────────────────
    work_record = {
        "work_id":          work_id,
        "title":            str(title)[:_TITLE_MAX_LENGTH],
        "publication_year": pub_year,
        "cited_by_count":   cited_by,
        "type":             work_type,
    }

    # ── Authorships ──────────────────────────────────────────────────────────
    authorships = raw_work.get("authorships") or []
    institutions: list[dict] = []
    authors:      list[dict] = []
    author_ids:   list[str]  = []

    seen_authors = set()
    seen_institutions = set()

    for authorship in authorships:
        inst_rec = extract_institution(authorship)
        if inst_rec and inst_rec["institution_id"] not in seen_institutions:
            institutions.append(inst_rec)
            seen_institutions.add(inst_rec["institution_id"])

        author_rec = extract_author(authorship, pub_date)
        if author_rec and author_rec["author_id"] not in seen_authors:
            authors.append(author_rec)
            author_ids.append(author_rec["author_id"])
            seen_authors.add(author_rec["author_id"])

    # ── Concepts ─────────────────────────────────────────────────────────────
    raw_concepts = raw_work.get("concepts") or []
    concepts:    list[dict] = []
    concept_ids: list[str]  = []
    seen_concepts = set()

    for raw_concept in raw_concepts:
        concept_rec = extract_concept(raw_concept)
        if concept_rec and concept_rec["concept_id"] not in seen_concepts:
            concepts.append(concept_rec)
            concept_ids.append(concept_rec["concept_id"])
            seen_concepts.add(concept_rec["concept_id"])

    # ── Referenced works (citations) ─────────────────────────────────────────
    raw_cited = raw_work.get("referenced_works") or []
    cited_ids: list[str] = []
    seen_cited = set()
    for ref_url in raw_cited:
        cited_id = _clean_id(ref_url)
        if cited_id and cited_id not in seen_cited:
            cited_ids.append(cited_id)
            seen_cited.add(cited_id)

    return {
        "work":         work_record,
        "institutions": institutions,
        "authors":      authors,
        "author_ids":   author_ids,
        "concepts":     concepts,
        "concept_ids":  concept_ids,
        "cited_ids":    cited_ids,
    }


def transform_batch(raw_works: list[dict]) -> list[dict]:
    """
    Transform a list of raw work dicts, skipping any that cannot be parsed.

    Returns a list of transformed work dicts (see transform_work for shape).
    """
    results = []
    skipped = 0
    for raw in raw_works:
        try:
            transformed = transform_work(raw)
            if transformed is not None:
                results.append(transformed)
            else:
                skipped += 1
        except Exception as exc:
            skipped += 1
            logger.warning(
                "Unexpected error transforming work %s: %s",
                raw.get("id", "<unknown>"),
                exc,
                exc_info=True,
            )
    if skipped:
        logger.debug(
            "Batch transform: %d accepted, %d skipped (no id/title or parse error).",
            len(results), skipped,
        )
    return results
