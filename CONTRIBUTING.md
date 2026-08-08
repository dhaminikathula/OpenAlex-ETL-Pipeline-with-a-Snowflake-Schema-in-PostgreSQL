# Contributing to OpenAlex ETL Pipeline

Thank you for your interest in contributing! This guide explains how to set up
the development environment and submit changes.

---

## Development Setup

```bash
git clone https://github.com/dhaminikathula/OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL.git
cd OpenAlex-ETL-Pipeline-with-a-Snowflake-Schema-in-PostgreSQL

python -m venv venv
# Windows
.\venv\Scripts\Activate.ps1
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# Edit .env with your local PostgreSQL credentials
```

---

## Project Structure

| File | Purpose |
|---|---|
| `etl.py` | Main entry point — orchestrates the pipeline |
| `config.py` | Reads all settings from environment variables |
| `db.py` | PostgreSQL connection and cursor management |
| `schema.py` | `CREATE TABLE` DDL — idempotent schema setup |
| `extractor.py` | OpenAlex API client with cursor pagination + backoff |
| `transformer.py` | Raw JSON → typed Python dicts |
| `loader.py` | DB inserts, upserts, and SCD Type 2 logic |
| `verify.py` | Post-run integrity checks |
| `demo_run.py` | Quick test run with minimal data |

---

## Commit Convention

This project follows [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | When to use |
|---|---|
| `feat:` | New feature or capability |
| `fix:` | Bug fix |
| `docs:` | Documentation changes |
| `refactor:` | Code restructuring without behaviour change |
| `perf:` | Performance improvement |
| `chore:` | Tooling, config, dependency updates |
| `test:` | Adding or fixing tests |

Examples:
```
feat(loader): add retry logic for deadlock errors
fix(extractor): handle empty results page gracefully
docs: update README with verification queries
```

---

## Testing

Run the pipeline against a small dataset first:
```bash
# Set TARGET_WORKS=1000 in .env, then:
python etl.py

# Verify output:
python verify.py
```

---

## Submitting a Pull Request

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature-name`
3. Make your changes with meaningful commits
4. Push to your fork and open a Pull Request
5. Describe what changed and why in the PR description
