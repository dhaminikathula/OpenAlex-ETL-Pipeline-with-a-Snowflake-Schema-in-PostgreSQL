"""
config.py - Configuration management for the OpenAlex ETL Pipeline.

Reads all required environment variables from a .env file or the system
environment. Raises clear errors if required variables are missing.
"""

import os
from dotenv import load_dotenv

# Load .env file if it exists (won't override existing system env vars)
load_dotenv()


def _require(key: str) -> str:
    """Get a required environment variable or raise a clear error."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Please copy .env.example to .env and fill in your values."
        )
    return value


# ─── Database connection parameters ───────────────────────────────────────────
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
DB_NAME: str = _require("DB_NAME")
DB_USER: str = _require("DB_USER")
DB_PASSWORD: str = _require("DB_PASSWORD")

# ─── API parameters ───────────────────────────────────────────────────────────
OPENALEX_EMAIL: str = os.getenv("OPENALEX_EMAIL", "")
OPENALEX_BASE_URL: str = "https://api.openalex.org"

# ─── ETL pipeline parameters ──────────────────────────────────────────────────
TARGET_WORKS: int = int(os.getenv("TARGET_WORKS", "500000"))
BATCH_SIZE: int = min(int(os.getenv("BATCH_SIZE", "200")), 200)  # API max is 200
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "7"))
BASE_WAIT_SECONDS: float = float(os.getenv("BASE_WAIT_SECONDS", "1"))

# Build the DSN (Data Source Name) string for psycopg2
DB_DSN: str = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)
