# Sorsa Ingestion — Documentation

This documentation covers the full implementation of the Sorsa ingestion pipeline located under `app/` in the TestSorsa repository. Every section is derived directly from the source code and DDL rather than from aspirational plans.

---

## What is this?

The Sorsa ingestion pipeline is a Python 3.12 async application that:

1. Pulls X/Twitter intelligence data from the Sorsa v3 REST API.
2. Stores normalized data into CockroachDB (`mindshare` schema).
3. Performs idempotent upserts so re-runs never produce duplicates.
4. Tracks each ingestion run's lifecycle and status in `mindshare.ingestion_run`.

It is designed to run locally via the CLI today and can be ported to any worker framework without changing business logic — all logic lives in `IngestionOrchestrator.run_project_ingestion(...)`.

---

## Repository layout

```
app/
├── __init__.py
├── cli.py                        # CLI entrypoint — parses args, loads .env, calls orchestrator
├── config.py                     # Pydantic-settings: all runtime config from env vars
├── models.py                     # Shared dataclasses: TimeSlice
├── clients/
│   └── sorsa_client.py           # Async HTTP client — 4 endpoint wrappers + rate limiter + retry
├── db/
│   ├── connection.py             # asyncpg pool factory
│   └── repository.py            # All DB reads/writes: runs, normalized posts, scores
└── pipeline/
    ├── orchestrator.py           # Top-level coordinator — wires all components, controls phase order
    ├── search_ingestor.py        # Phase 1: concurrent time-sliced search ingestion
    └── aux_ingestors.py          # Phases 2–4: comments, user timelines, user scores

main.py                           # Python entry: delegates to app.cli.main()
logs/                             # Per-day log folders (logs/YYYY-MM-DD/run_HHMMSS_<id>.log)
ddl/
└── ddl_mindshare_ingestion.sql   # Schema DDL for all ingestion tables
```

---

## Implemented API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/search-tweets` | POST | Search tweets by keyword + time window |
| `/comments` | POST | Fetch all comments on a given post |
| `/user-tweets` | POST | Fetch a user's tweet timeline |
| `/score` | GET | Fetch influence score + profile for a user (`?user_id=<x_id>`) |

---

## Implemented features

- **Configurable search window** — defaults to 72 hours, overridable via `--hours` or explicit `--since`/`--until` CLI arguments.
- **Multi-keyword OR search** — `--project-keyword "term1,term two,term3"` is converted to `term1 OR "term two" OR term3` before sending to the API. The first term is used as the DB project label.
- **Multi-key API support** — `SORSA_API_KEYS` accepts a comma-separated list of API keys. Concurrency auto-scales as `len(keys) × SORSA_PER_KEY_RPS`.
- **Per-key flat RPS limiter** — enforces the `SORSA_PER_KEY_RPS` requests-per-second cap per key. Asyncio-native, shared across all concurrent workers.
- **Retry handling** — automatic backoff for HTTP 429, HTTP 5xx, network/timeout errors, and empty/non-JSON body responses (`json.JSONDecodeError`).
- **Concurrent phases** — Phase 1 runs all time-slice workers concurrently (bounded semaphore). Phases 2, 3, and 4 run concurrently with each other, each with internal per-item concurrency.
- **Aux-phase retry pass** — after the initial concurrent pass, failed posts/users are re-queued for one retry attempt before being marked as permanently failed.
- **Batch DB writes** — tweets are accumulated in a per-slice in-memory buffer and written via `executemany` in batches of 1000 (configurable).
- **DB connection resilience** — `asyncpg` pool with `max_inactive_connection_lifetime=300s`; batch writes retry on transient connection errors.
- **Normalized persistence** — every API response is decomposed into typed columns and upserted into `mindshare.mindshare_post` / `mindshare.mindshare_user` with robust type-conversion helpers.
- **`project_keywords` merge** — a post seen by multiple keyword runs accumulates all keywords in a `TEXT[]` array rather than clobbering earlier writes.
- **Ingestion run lifecycle** — every pipeline execution creates an `ingestion_run` record: `running` → `completed` or `running` → `failed`.
- **Structured logging** — every significant event is logged at `INFO`/`WARNING`/`ERROR`. Logs go to both console and daily partitioned files (`logs/YYYY-MM-DD/run_HHMMSS_<id>.log`). Request counts per API key are logged after every phase.
- **Elapsed time tracking** — total pipeline duration (in minutes) is logged and printed at the end of every run.

---

## Database schema

One CockroachDB schema is used:

- **`mindshare`** — normalized production data: posts (`mindshare_post`), users (`mindshare_user`), run metadata (`ingestion_run`).

See [Data Model](data-model.md) for full table documentation.

---

## Quick navigation

| Topic | Page |
|---|---|
| Setup and running | [Quickstart](quickstart.md) |
| System design and module responsibilities | [Architecture](architecture.md) |
| Table schemas and upsert semantics | [Data Model](data-model.md) |
| Phase-by-phase pipeline logic | [Pipeline Behavior](pipeline-behavior.md) |
| SQL checks, tuning, known limitations | [Operations Runbook](runbook.md) |
