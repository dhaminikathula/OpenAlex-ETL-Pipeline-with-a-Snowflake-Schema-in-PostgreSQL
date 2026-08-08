-- =============================================================================
-- analysis_queries.sql
-- Exploratory SQL queries for the OpenAlex ETL Pipeline data warehouse.
-- Run these in psql after the ETL pipeline has loaded data.
-- Connect: psql -U postgres -d openalex_dw
-- =============================================================================


-- =============================================================================
-- 1. OVERVIEW STATISTICS
-- =============================================================================

-- Total records across all tables
SELECT
    'fact_works'            AS table_name, COUNT(*) AS row_count FROM fact_works
UNION ALL SELECT 'dim_institutions',        COUNT(*) FROM dim_institutions
UNION ALL SELECT 'dim_authors',             COUNT(*) FROM dim_authors
UNION ALL SELECT 'dim_concepts',            COUNT(*) FROM dim_concepts
UNION ALL SELECT 'bridge_work_authors',     COUNT(*) FROM bridge_work_authors
UNION ALL SELECT 'bridge_work_concepts',    COUNT(*) FROM bridge_work_concepts
UNION ALL SELECT 'bridge_work_citations',   COUNT(*) FROM bridge_work_citations
ORDER BY row_count DESC;


-- =============================================================================
-- 2. PUBLICATION TRENDS
-- =============================================================================

-- Works published per year (last 20 years)
SELECT
    publication_year,
    COUNT(*)                   AS total_works,
    SUM(cited_by_count)        AS total_citations,
    ROUND(AVG(cited_by_count), 2) AS avg_citations_per_work
FROM   fact_works
WHERE  publication_year >= EXTRACT(YEAR FROM NOW()) - 20
  AND  publication_year IS NOT NULL
GROUP  BY publication_year
ORDER  BY publication_year DESC;


-- =============================================================================
-- 3. TOP WORKS
-- =============================================================================

-- Top 10 most cited academic works
SELECT
    work_id,
    LEFT(title, 80)  AS title_preview,
    publication_year,
    type,
    cited_by_count
FROM   fact_works
ORDER  BY cited_by_count DESC
LIMIT  10;

-- Works with no citations (uncited works)
SELECT COUNT(*) AS uncited_works
FROM   fact_works
WHERE  cited_by_count = 0 OR cited_by_count IS NULL;


-- =============================================================================
-- 4. CONCEPTS / TOPICS ANALYSIS
-- =============================================================================

-- Top 20 most common research topics
SELECT
    dc.display_name,
    dc.level,
    COUNT(*)         AS usage_count
FROM   bridge_work_concepts bwc
JOIN   dim_concepts dc ON bwc.concept_id = dc.concept_id
GROUP  BY dc.display_name, dc.level
ORDER  BY usage_count DESC
LIMIT  20;

-- Distribution of concept levels (0=broad, 5=specific)
SELECT
    level,
    COUNT(DISTINCT concept_id) AS unique_concepts,
    COUNT(*)                   AS total_assignments
FROM   bridge_work_concepts bwc
JOIN   dim_concepts dc ON bwc.concept_id = dc.concept_id
GROUP  BY level
ORDER  BY level;


-- =============================================================================
-- 5. AUTHOR & INSTITUTION ANALYSIS
-- =============================================================================

-- Top 10 most prolific institutions (by number of authors)
SELECT
    di.display_name,
    di.country_code,
    di.type,
    COUNT(DISTINCT da.author_key) AS author_count
FROM   dim_authors da
JOIN   dim_institutions di ON da.institution_id = di.institution_id
WHERE  da.is_current = TRUE
GROUP  BY di.display_name, di.country_code, di.type
ORDER  BY author_count DESC
LIMIT  10;

-- Country distribution of institutions
SELECT
    country_code,
    COUNT(*) AS institution_count
FROM   dim_institutions
WHERE  country_code IS NOT NULL
GROUP  BY country_code
ORDER  BY institution_count DESC
LIMIT  20;

-- Authors who changed institutions (SCD Type 2 history)
SELECT
    author_id,
    COUNT(*)  AS version_count,
    MIN(start_date) AS first_seen,
    MAX(start_date) AS latest_change
FROM   dim_authors
GROUP  BY author_id
HAVING COUNT(*) > 1
ORDER  BY version_count DESC
LIMIT  10;


-- =============================================================================
-- 6. COLLABORATION NETWORK
-- =============================================================================

-- Works with the most co-authors
SELECT
    fw.work_id,
    LEFT(fw.title, 60) AS title_preview,
    fw.publication_year,
    COUNT(bwa.author_key) AS co_author_count
FROM   fact_works fw
JOIN   bridge_work_authors bwa ON fw.work_id = bwa.work_id
GROUP  BY fw.work_id, fw.title, fw.publication_year
ORDER  BY co_author_count DESC
LIMIT  10;

-- Average co-authors per work
SELECT ROUND(AVG(author_count), 2) AS avg_authors_per_work
FROM (
    SELECT work_id, COUNT(author_key) AS author_count
    FROM   bridge_work_authors
    GROUP  BY work_id
) sub;


-- =============================================================================
-- 7. CITATION NETWORK
-- =============================================================================

-- Most cited works (via bridge_work_citations)
SELECT
    fw.work_id,
    LEFT(fw.title, 60) AS title_preview,
    COUNT(bwc.citing_work_id) AS inbound_citations
FROM   fact_works fw
JOIN   bridge_work_citations bwc ON fw.work_id = bwc.cited_work_id
GROUP  BY fw.work_id, fw.title
ORDER  BY inbound_citations DESC
LIMIT  10;


-- =============================================================================
-- 8. DATA QUALITY CHECKS
-- =============================================================================

-- Works missing title (should be 0 — title is NOT NULL in schema)
SELECT COUNT(*) AS works_missing_title
FROM   fact_works
WHERE  title IS NULL OR title = '';

-- Authors without any institution affiliation
SELECT COUNT(*) AS unaffiliated_authors
FROM   dim_authors
WHERE  institution_id IS NULL AND is_current = TRUE;

-- Works with no associated authors
SELECT COUNT(*) AS works_without_authors
FROM   fact_works fw
LEFT   JOIN bridge_work_authors bwa ON fw.work_id = bwa.work_id
WHERE  bwa.work_id IS NULL;

-- Works with no associated concepts
SELECT COUNT(*) AS works_without_concepts
FROM   fact_works fw
LEFT   JOIN bridge_work_concepts bwc ON fw.work_id = bwc.work_id
WHERE  bwc.work_id IS NULL;
