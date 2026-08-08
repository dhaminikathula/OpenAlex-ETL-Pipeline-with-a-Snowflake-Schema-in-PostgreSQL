"""
db.py - Database connection management for the OpenAlex ETL Pipeline.

Provides a simple context manager for obtaining and releasing psycopg2
connections. Uses a single persistent connection for the single-threaded
ETL process to maximise throughput.
"""

import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Generator

import config

logger = logging.getLogger(__name__)

# Module-level connection singleton (reused across the entire ETL run)
_connection: psycopg2.extensions.connection | None = None


def get_connection() -> psycopg2.extensions.connection:
    """
    Return the module-level persistent database connection.
    Creates a new one if none exists or the existing one is closed/broken.
    """
    global _connection
    if _connection is None or _connection.closed:
        logger.info(
            "Opening PostgreSQL connection to %s:%s/%s as %s",
            config.DB_HOST,
            config.DB_PORT,
            config.DB_NAME,
            config.DB_USER,
        )
        _connection = psycopg2.connect(config.DB_DSN)
        # Use autocommit=False so we control transactions explicitly
        _connection.autocommit = False
    return _connection


def close_connection() -> None:
    """Close the module-level connection if it is open."""
    global _connection
    if _connection is not None and not _connection.closed:
        _connection.close()
        logger.info("PostgreSQL connection closed.")
    _connection = None


@contextmanager
def get_cursor(
    named: bool = False,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Context manager that yields a psycopg2 cursor and handles
    commit/rollback automatically.

    Args:
        named: If True, returns a server-side (named) cursor for large
               result sets. Not typically needed here.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


@contextmanager
def get_standard_cursor() -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Context manager yielding a plain (non-RealDict) cursor.
    Useful for executemany / copy operations.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
