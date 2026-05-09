# Sorsa Ingestion — Documentation

This documentation covers the full implementation of the Sorsa ingestion pipeline located under `app/` in the TestSorsa repository. Every section is derived directly from the source code and DDL rather than from aspirational plans.

---

## What is this?

The Sorsa ingestion pipeline is a Python 3.12 async application that:

1. Pulls X/Twitter intelligence data from the Sorsa v3 REST API.
2. Stores raw and normalized data into CockroachDB.
3. Tracks each ingestion run and every paginated request window in checkpoint tables.
4. Performs idempotent upserts so re-runs never produce duplicates.

It is designed to run locally via the CLI today and can be ported to any worker framework without changing business logic — all logic lives in `IngestionOrchestrator.run_project_ingestion(...)`.

---

## Repository layout

```
app/
├── __init__.py
├── cli.py                        # CLI entrypoint — parses args, loads .env, calls orchestrator
├── config.py                     # Pydantic-settings: all runtime config from env vars
├── models.py                     # Shared dataclasses: TimeSlice, IngestionContext
├── clients/
│   └── sorsa_client.py           # Async HTTP client — 4 endpoint wrappers + rate limiter + retry
├── db/
│   ├── connection.py             # SQLAlchemy async engine + session maker factory
│   └── repository.py            # All DB reads/writes: runs, raw posts, normalized posts, scores, checkpoints
└── pipeline/
    ├── orchestrator.py           # Top-level coordinator — wires all components, controls phase order
    ├── search_ingestor.py        # Phase 1: concurrent 72-hour time-sliced search ingestion
    └── aux_ingestors.py          # Phases 2–4: comments, user timelines, user scores

main.py                           # Python entry: delegates to app.cli.main()
ddl/
└── ddl_mindshare_ingestion.sql   # Schema DDL for all ingestion tables
Mindshare DDL/
└── ddl mindshare.sql             # Original DDL — not modified
```

---

## Implemented API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/search-tweets` | POST | Search tweets by keyword + time window |
| `/comments` | POST | Fetch all comments on a given post |
| `/user-tweets` | POST | Fetch a user's recent tweet timeline |
| `/score-id/{x-id}` | GET | Fetch influence score + profile for a user |

---

## Implemented features

- **72-hour sliding window search** with configurable time-slicing for parallel fetch.
- **Per-key RPS limiter** — enforces the flat `SORSA_PER_KEY_RPS` requests-per-second cap. Thread-safe, asyncio-native, shared across all concurrent slice workers.
- **Retry handling** — automatic backoff and retry for HTTP 429, HTTP 5xx, and network/timeout errors.
- **Normalized persistence** — every API response is decomposed into typed columns and upserted into `mindshare.mindshare_post` / `mindshare.mindshare_user`.
- **`project_keywords` merge** — a post seen by multiple keyword searches accumulates all keywords in a `TEXT[]` array rather than clobbering the earlier write.
- **Ingestion run lifecycle** — every pipeline execution creates an `ingestion_run` record that is transitioned from `running` → `completed` or `running` → `failed`.
- **Window checkpoints** — every paginated request window records its cursor and status in `ingestion_window_checkpoint`, enabling future resume logic.

---

## Database schema

One CockroachDB schema is used:

- **`mindshare`** — normalized production data: posts, users, runs, checkpoints.

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
