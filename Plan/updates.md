# Sorsa Ingestion Plan (Actual)

## Objective
Build a reliable ingestion pipeline that continuously pulls X/Twitter intelligence from Sorsa APIs, stores normalized data in CockroachDB, and can safely resume from checkpoints without duplicate writes.

## Scope
- Pipeline package: `Sorsa/app/`
- Database DDL (ingestion-safe copy): `Sorsa/ddl/ddl_mindshare_ingestion.sql`
- Legacy DDL preserved (no edits): `Sorsa/Mindshare DDL/ddl mindshare.sql`
- Documentation site: `Sorsa/ingestion-docs/`

## Phase 1 - Foundation (Completed)
- Set up isolated ingestion runtime with config-driven environment loading.
- Implement async Sorsa API client with:
  - Multi-key rotation
  - Rate limiting
  - Retry/backoff handling
- Add repository/data access layer with idempotent upserts.
- Ensure `project_keywords` merges by `post_id` instead of destructive overwrite.

## Phase 2 - End-to-End Ingestion Flow (Completed)
- Build orchestrator to process `search-tweets` in 72-hour windows.
- Chain endpoint flow in this order:
  1. `/search-tweets`
  2. `/comments` for discovered posts
  3. `/user-tweets` for discovered users
  4. `/score-id/{x-id}` for discovered users
- Add CLI entrypoint for local/manual runs:
  - `python main.py --project-keyword <keyword>`

## Phase 3 - Reliability + Recovery (Completed)
- Add run lifecycle tracking in `mindshare.ingestion_run`.
- Add endpoint/window checkpoints in `mindshare.ingestion_window_checkpoint`.
- Guarantee resumability for interrupted runs from latest successful checkpoint.

## Phase 4 - Documentation + Operability (Completed)
- Create separate MkDocs project under `Sorsa/ingestion-docs/`.
- Document:
  - Architecture
  - Data model
  - Pipeline behavior
  - Runbook + quickstart
- Add Mermaid diagrams for ingestion and execution flow clarity.

## Phase 5 - Immediate Next Work (Pending)
- Add automated tests:
  - API client retry/key-rotation tests
  - Repository upsert/idempotency tests
  - Orchestrator checkpoint-resume tests
- Add observability:
  - Structured logs per run and per endpoint window
  - Run-level metrics (fetched, inserted, updated, failed)
- Add production scheduling:
  - Cron/worker trigger with lock to prevent concurrent duplicate runs
- Add data quality guards:
  - Required field validation
  - Dead-letter capture for malformed payloads

## Done Definition for Next Milestone
- A scheduled ingestion run can execute unattended.
- Failed runs resume from checkpoint without data loss or duplication.
- Metrics and logs are sufficient for debugging and reporting.
- Test suite covers critical ingestion and persistence paths.

