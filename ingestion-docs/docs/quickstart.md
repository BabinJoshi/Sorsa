# Quickstart

## Prerequisites

- Python 3.12+
- CockroachDB instance reachable through `postgresql+asyncpg` SQLAlchemy URL
- A single Sorsa API key

## Environment variables

Required:

- `COCKROACH_DATABASE_URL`
- `SORSA_API_KEY`

Optional tuning:

- `SORSA_BASE_URL` (default: `https://api.sorsa.io/v3`)
- `SORSA_PER_KEY_RPS` (default: `20`)
- `SEARCH_SLICE_COUNT` (default: `20`)
- `SEARCH_MAX_CONCURRENCY` (default: `20`)
- `SEARCH_ORDER` (default: `latest`)
- `SORSA_MAX_RETRIES` (default: `4`)
- `SORSA_RETRY_429_SLEEP_SECONDS` (default: `1.0`)
- `SORSA_RETRY_5XX_SLEEP_SECONDS` (default: `2.0`)

Concurrency is configuration-driven:

- 72h window is split into `SEARCH_SLICE_COUNT` timeframes.
- Concurrent search workers are bounded by:
  - `min(SEARCH_SLICE_COUNT, SEARCH_MAX_CONCURRENCY, SORSA_PER_KEY_RPS)`

Examples:

- `SEARCH_SLICE_COUNT=10`, `SEARCH_MAX_CONCURRENCY=20`, `SORSA_PER_KEY_RPS=20` -> up to 10 concurrent slice workers.
- `SEARCH_SLICE_COUNT=20`, `SEARCH_MAX_CONCURRENCY=20`, `SORSA_PER_KEY_RPS=20` -> up to 20 concurrent slice workers.

## Install dependencies

```bash
cd Sorsa
pip install -e .
```

## Apply ingestion DDL

Use:

- `ddl/ddl_mindshare_ingestion.sql`

Do not edit/replace:

- `Mindshare DDL/ddl mindshare.sql`

## Run locally

```bash
python main.py --project-keyword <keyword>
```

Example:

```bash
python main.py --project-keyword Acurast
```

