# Operations Runbook

## Build docs site

From `Sorsa/ingestion-docs`:

```bash
mkdocs serve
```

or

```bash
mkdocs build
```

## Common runtime checks

1. Confirm run record exists in `mindshare.ingestion_run`.
2. Inspect checkpoint table for stalled/failed windows.
3. Check recent rows in `raw_data.raw_post_ingestion`.
4. Verify merged keyword behavior in `mindshare.mindshare_post`.

## SQL checks

### Last runs

```sql
SELECT run_id, project_keyword, run_status, started_at, finished_at
FROM mindshare.ingestion_run
ORDER BY started_at DESC
LIMIT 20;
```

### Failed windows

```sql
SELECT run_id, endpoint, window_id, status, error_message, updated_at
FROM mindshare.ingestion_window_checkpoint
WHERE status = 'failed'
ORDER BY updated_at DESC
LIMIT 50;
```

### Verify keyword merge on duplicate post

```sql
SELECT post_id, project_keywords, last_seen_at
FROM mindshare.mindshare_post
WHERE post_id = <tweet_id>;
```

## Tuning guidance

- Increase throughput:
  - increase `SEARCH_SLICE_COUNT` cautiously (within single-key rate limits)
- Reduce rate-limit pressure:
  - lower `SORSA_PER_KEY_RPS`
  - increase retry sleep values

## Known limitations in current implementation

- Resume-from-checkpoint reader is not yet fully wired (checkpoints are written, but restart flow currently begins a fresh run).
- Single-key mode is currently enforced by configuration (`SORSA_API_KEY`).
- Batch write optimization can be improved for higher-volume ingest.

These are next recommended hardening steps.

