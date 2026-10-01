# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Project guidance for AI agents lives in AGENTS.md.
Claude Code loads it via the import below.

@AGENTS.md

---

## Project Overview

**DevPulse** is an Engineering Intelligence Platform for Leapfrog Technology. It ingests data from GitHub, Jira, CI/CD, Monitoring, Slack, and Vyaguta (internal HR/org tool), processes it through a Medallion Architecture (Bronze → Silver → Gold) on Databricks with Delta Lake, and serves analytics to a Node.js/Express dashboard.

The repository is a **Databricks Asset Bundle (DAB)** scaffold. The ETL pipeline notebooks live in the Databricks workspace and have not yet been committed here. The current code contains deployment config, a placeholder Python package, and an integration test harness.

**Workspace:** `https://dbc-199c9ed4-1fd6.cloud.databricks.com`  
**Catalog/schema targets:** `dev` (default, per-user schema) and `prod` (schema `prod`, catalog `workspace`)

---

## Commands

**Package manager:** `uv` (not pip directly)

```bash
# Install all dependencies including dev
uv sync --dev

# Run tests (requires live Databricks connection via Databricks Connect)
uv run pytest

# Run a single test file
uv run pytest tests/sample_taxis_test.py

# Lint
uv run ruff check .

# Format check
uv run ruff format --check .
```

**Bundle deploy/run:**
```bash
databricks bundle deploy             # deploy to dev (default target)
databricks bundle deploy --target prod
databricks bundle run sample_job     # run a job by resource key
databricks bundle validate           # validate bundle config
```

---

## Architecture

### DABs Layout

- `databricks.yml` — Bundle root: workspace host, targets (`dev`/`prod`), variables (`catalog`, `schema`)
- `resources/*.yml` — Job/pipeline definitions; jobs reference notebooks or the Python package
- `src/leapfrog_pulse/` — Python package deployed to the cluster; `main.py` is the CLI entry point accepting `--catalog` and `--schema` args
- `src/sample_notebook.ipynb` — Notebook tasks referenced by jobs
- `tests/` — Integration tests using `DatabricksSession` (Databricks Connect); fixtures go in `fixtures/`

### Bundle Variables

`catalog` and `schema` are injected per target in `databricks.yml` and passed to jobs as task parameters. In `dev` mode, Databricks automatically prefixes deployed resource names with `[dev <username>]` and pauses schedules.

### Python Package

`src/leapfrog_pulse/main.py` sets the Spark catalog/database context from CLI args before running pipeline logic. Add new pipeline modules under `src/leapfrog_pulse/` and wire them here.

### Tests

`tests/conftest.py` initializes a `DatabricksSession` (falls back to serverless via `DATABRICKS_SERVERLESS_COMPUTE_ID=auto`) and provides a `spark` fixture. All tests require a live cluster — there are no offline unit tests currently.

### Planned Medallion Architecture (per README)

- **Bronze** — Raw ingestion tables per source (GitHub events, Jira issues, CI runs, etc.)
- **Silver** — Cleaned/normalized tables (developer activity, PR metrics, sprint velocity, etc.)
- **Gold** — Aggregated analytics tables consumed by the dashboard API

### Ruff Config

Line length is 120. Configuration lives in `pyproject.toml` under `[tool.ruff]`.
