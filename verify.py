"""
verify.py - Post-run data integrity verification for the OpenAlex ETL Pipeline.

Run this script after `python etl.py` to confirm all 7 tables have been
correctly populated and that SCD Type 2 integrity is maintained.

Usage:
    python verify.py
"""

import sys
import logging

import db
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_count_query(cursor, table: str) -> int:
    """Return the row count for a given table."""
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0]


def verify_pipeline() -> None:
    """Run all verification queries and report results."""
    print("\n" + "=" * 60)
    print("  OpenAlex ETL Pipeline — Verification Report")
    print("=" * 60)

    checks_passed = 0
    checks_failed = 0

    with db.get_cursor() as cur:
        # ── Table row counts ────────────────────────────────────────
        tables = [
            ("fact_works",             "Works (fact table)"),
            ("dim_institutions",       "Institutions dimension"),
            ("dim_authors",            "Authors dimension (SCD2)"),
            ("dim_concepts",           "Concepts dimension"),
            ("bridge_work_authors",    "Work-Author bridge"),
            ("bridge_work_concepts",   "Work-Concept bridge"),
            ("bridge_work_citations",  "Work-Citation bridge"),
        ]

        print("\n📊 Table Row Counts:")
        print(f"  {'Table':<30} {'Count':>10}  {'Status'}")
        print(f"  {'-'*30} {'-'*10}  {'-'*6}")

        for table, label in tables:
            count = run_count_query(cur, table)
            status = "✅ OK" if count > 0 else "❌ EMPTY"
            if count > 0:
                checks_passed += 1
            else:
                checks_failed += 1
            print(f"  {label:<30} {count:>10,}  {status}")

        # ── Target works check ──────────────────────────────────────
        print(f"\n🎯 Target Check:")
        cur.execute("SELECT COUNT(*) FROM fact_works")
        total_works = cur.fetchone()[0]
        if total_works >= config.TARGET_WORKS:
            print(f"  ✅ Target of {config.TARGET_WORKS:,} works ACHIEVED ({total_works:,} loaded)")
            checks_passed += 1
        else:
            print(f"  ⚠️  Target: {config.TARGET_WORKS:,} | Loaded: {total_works:,} ({total_works/config.TARGET_WORKS*100:.1f}%)")

        # ── SCD Type 2 integrity ────────────────────────────────────
        print(f"\n🔍 SCD Type 2 Integrity (dim_authors):")
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT author_id
                FROM dim_authors
                WHERE is_current = TRUE
                GROUP BY author_id
                HAVING COUNT(*) > 1
            ) violations
        """)
        violations = cur.fetchone()[0]
        if violations == 0:
            print(f"  ✅ No duplicate current records found (0 violations)")
            checks_passed += 1
        else:
            print(f"  ❌ {violations} authors have multiple is_current=TRUE records!")
            checks_failed += 1

        # ── FK integrity check ──────────────────────────────────────
        print(f"\n🔗 Foreign Key Integrity:")
        cur.execute("""
            SELECT COUNT(*)
            FROM dim_authors da
            LEFT JOIN dim_institutions di ON da.institution_id = di.institution_id
            WHERE da.institution_id IS NOT NULL
              AND di.institution_id IS NULL
        """)
        fk_violations = cur.fetchone()[0]
        if fk_violations == 0:
            print(f"  ✅ All dim_authors.institution_id FK references are valid")
            checks_passed += 1
        else:
            print(f"  ❌ {fk_violations} orphaned institution_id references found!")
            checks_failed += 1

    # ── Summary ────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  Results: {checks_passed} passed, {checks_failed} failed")
    if checks_failed == 0:
        print("  ✅ All checks PASSED — pipeline output is valid!")
    else:
        print("  ❌ Some checks FAILED — review output above.")
    print("=" * 60 + "\n")

    db.close_connection()
    sys.exit(0 if checks_failed == 0 else 1)


if __name__ == "__main__":
    verify_pipeline()
