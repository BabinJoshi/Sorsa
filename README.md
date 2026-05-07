# Sorsa
This repo is for testing sorsa API

## Ingestion App

New ingestion code lives under `app/` and is designed for local execution now and worker adoption later.

### Run locally

Set required env vars:

- `COCKROACH_DATABASE_URL`
- `SORSA_API_KEY` (single key)
- optional tuning: `SORSA_PER_KEY_RPS`, `SEARCH_SLICE_COUNT`, `SORSA_MAX_RETRIES`

Then run:

`python main.py --project-keyword <keyword>`

You can set these in `Sorsa/.env` (loaded via `python-dotenv`) for local runs.

Concurrency behavior:
- `SEARCH_SLICE_COUNT` controls how many timeframes the 72h window is divided into.
- `SEARCH_MAX_CONCURRENCY` controls max concurrent slice workers.
- Effective search concurrency is bounded by:
  - `min(SEARCH_SLICE_COUNT, SEARCH_MAX_CONCURRENCY, SORSA_PER_KEY_RPS)`

### DDL

The original DDL remains untouched:

- `Mindshare DDL/ddl mindshare.sql`

New ingestion-focused DDL copy:

- `ddl/ddl_mindshare_ingestion.sql`
