# Sorsa Ingestion Documentation

This documentation set covers the newly implemented ingestion app under `Sorsa/app`.

The implementation is:

- Cockroach-first
- strict upsert-based
- checkpoint-aware
- local runnable now, worker-portable later

## What is implemented

- API client support for:
  - `/search-tweets`
  - `/comments`
  - `/user-tweets`
  - `/score-id/{x-id}`
- 72-hour ingestion window for keyword search with time-sliced parallel fetch.
- Per-key rate limiting and retry handling for 429/5xx/network errors.
- Unified run tracking and checkpoint persistence in CockroachDB.
- Post-level deduplication by `post_id` with `project_keywords` merge semantics.

## Code layout

- `app/config.py`
- `app/clients/sorsa_client.py`
- `app/db/connection.py`
- `app/db/repository.py`
- `app/pipeline/search_ingestor.py`
- `app/pipeline/aux_ingestors.py`
- `app/pipeline/orchestrator.py`
- `app/cli.py`
- `main.py`

## DDL files

Original DDL (unchanged):

- `Mindshare DDL/ddl mindshare.sql`

New ingestion-focused DDL copy:

- `ddl/ddl_mindshare_ingestion.sql`

