# Operations Runbook

This page covers how to run, monitor, debug, and tune the ingestion pipeline.

---

## Running the pipeline

### Local run

```bash
# From the repository root
python main.py --project-keyword <keyword>
```

Example:

```bash
python main.py --project-keyword Acurast
```

On success:

```
Ingestion completed. run_id=3fa85f64-5717-4562-b3fc-2c963f66afa6
```

On failure, a Python traceback is printed and the process exits non-zero.

### Running with custom tuning

Override any setting via environment variable:

```bash
SEARCH_SLICE_COUNT=10 SORSA_PER_KEY_RPS=10 python main.py --project-keyword Acurast
```

Or set them all in `.env` before running.

---

## Building and serving the docs site

From `ingestion-docs/`:

```bash
# Live preview (watches for changes)
mkdocs serve

# Build static site to ingestion-docs/site/
mkdocs build
```

---

## Reading the logs

The pipeline emits structured log lines at `INFO` (default) and `DEBUG` (verbose) levels across all modules. A typical successful run produces output like:

```
2026-05-07 18:00:01 [INFO] app.pipeline.orchestrator: Starting ingestion — keyword='Acurast' window=2026-05-04 18:00 UTC → 2026-05-07 18:00 UTC
2026-05-07 18:00:01 [INFO] app.pipeline.orchestrator: Run created — run_id=3fa85f64-5717-4562-b3fc-2c963f66afa6
2026-05-07 18:00:01 [INFO] app.pipeline.orchestrator: Phase 1 — search (/search-tweets) starting
2026-05-07 18:00:01 [INFO] app.pipeline.search_ingestor: Search ingestion starting — keyword='Acurast' slices=20 effective_concurrency=20 window=2026-05-04 18:00 UTC → 2026-05-07 18:00 UTC
2026-05-07 18:00:01 [INFO] app.pipeline.search_ingestor: [slice 1] Starting — key=key_1 window=2026-05-04 18:00 UTC → 2026-05-04 21:36 UTC
2026-05-07 18:00:01 [INFO] app.pipeline.search_ingestor: [slice 2] Starting — key=key_1 window=2026-05-04 21:36 UTC → 2026-05-05 01:12 UTC
...
2026-05-07 18:00:03 [INFO] app.pipeline.search_ingestor: [slice 1] Page 1 — 50 tweets (50 posts, 43 users) | has_next=True
2026-05-07 18:00:03 [INFO] app.pipeline.search_ingestor: [slice 2] Page 1 — 42 tweets (42 posts, 38 users) | has_next=False
2026-05-07 18:00:03 [INFO] app.pipeline.search_ingestor: [slice 2] Done — 42 posts, 38 users across 1 page(s)
2026-05-07 18:00:05 [INFO] app.pipeline.search_ingestor: [slice 1] Page 2 — 50 tweets (47 posts, 40 users) | has_next=False
2026-05-07 18:00:05 [INFO] app.pipeline.search_ingestor: [slice 1] Done — 97 posts, 83 users across 2 page(s)
...
2026-05-07 18:00:09 [INFO] app.pipeline.search_ingestor: Search ingestion complete — total_posts=842 total_users=310 across 20 slices
2026-05-07 18:00:09 [INFO] app.pipeline.orchestrator: Phase 1 complete — posts_found=842 users_found=310
2026-05-07 18:00:09 [INFO] app.clients.sorsa_client: After phase 1: Request counts — total=243 | key_1=243
2026-05-07 18:00:09 [INFO] app.pipeline.orchestrator: Phase 2 — comments (/comments) starting — posts=842
2026-05-07 18:00:09 [INFO] app.pipeline.aux_ingestors: [comments] Starting — 842 post(s) to process | key=key_1
2026-05-07 18:00:09 [INFO] app.pipeline.aux_ingestors: [comments] (1/842) Fetching comments for post_id=1234567890
2026-05-07 18:00:10 [INFO] app.pipeline.aux_ingestors: [comments] post_id=1234567890 done — 37 comment(s) across 2 page(s)
2026-05-07 18:00:10 [INFO] app.pipeline.aux_ingestors: [comments] (2/842) Fetching comments for post_id=9876543210
...
2026-05-07 18:01:23 [INFO] app.pipeline.aux_ingestors: [comments] Phase complete — 18340 comment(s) ingested | 839 post(s) ok, 3 failed
2026-05-07 18:01:23 [INFO] app.clients.sorsa_client: After phase 2: Request counts — total=1105 | key_1=1105
2026-05-07 18:01:23 [INFO] app.pipeline.orchestrator: Phase 3 — user timelines (/user-tweets) starting — users=310
2026-05-07 18:01:23 [INFO] app.pipeline.aux_ingestors: [user-tweets] Starting — 310 user(s) to process | key=key_1
...
2026-05-07 18:02:45 [INFO] app.pipeline.aux_ingestors: [user-tweets] Phase complete — 15420 tweet(s) ingested | 308 user(s) ok, 2 failed
2026-05-07 18:02:45 [INFO] app.clients.sorsa_client: After phase 3: Request counts — total=1729 | key_1=1729
2026-05-07 18:02:45 [INFO] app.pipeline.orchestrator: Phase 4 — user scores (/score-id) starting — users=310
2026-05-07 18:02:45 [INFO] app.pipeline.aux_ingestors: [scores] Starting — 310 user(s) to score | key=key_1
2026-05-07 18:02:45 [INFO] app.pipeline.aux_ingestors: [scores] (1/310) user_id=9876543210 username=alice score=8.45
2026-05-07 18:02:46 [WARNING] app.pipeline.aux_ingestors: [scores] (2/310) user_id=1111111111 failed: 404: {"error": "not found"}
...
2026-05-07 18:03:01 [INFO] app.pipeline.aux_ingestors: [scores] Phase complete — 308 scored, 2 failed out of 310 user(s)
2026-05-07 18:03:01 [INFO] app.clients.sorsa_client: After phase 4: Request counts — total=2039 | key_1=2039
2026-05-07 18:03:01 [INFO] app.clients.sorsa_client: Final totals: Request counts — total=2039 | key_1=2039
2026-05-07 18:03:01 [INFO] app.pipeline.orchestrator: Ingestion completed — run_id=3fa85f64-5717-4562-b3fc-2c963f66afa6
```

### Enabling DEBUG output

To see per-request dispatches, per-page pagination details, and per-DB-operation traces, set the log level to `DEBUG`. The simplest way is to modify `_configure_logging()` in `app/cli.py` or set it via environment:

```bash
PYTHONPATH=. python -c "
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
" && python main.py --project-keyword Acurast
```

Or just temporarily change `level=logging.INFO` to `level=logging.DEBUG` in `app/cli.py`.

### What WARNING lines mean

A `WARNING` from `app.pipeline.aux_ingestors` means a per-post/per-user failure that was swallowed — the run continues. A `WARNING` from `app.clients.sorsa_client` means a retry was triggered. Neither aborts the run.

An `ERROR` from `app.pipeline.orchestrator` or `app.clients.sorsa_client` means the run itself failed.

---

## Monitoring a run

### 1. Check run status

```sql
SELECT run_id, project_keyword, run_status, started_at, finished_at,
       finished_at - started_at AS duration
FROM mindshare.ingestion_run
ORDER BY started_at DESC
LIMIT 20;
```

Possible `run_status` values: `running`, `completed`, `failed`.

A run stuck in `running` with an old `started_at` and no `finished_at` indicates the process was killed or crashed without updating the record.

### 2. Check all checkpoints for a run

```sql
SELECT window_id, endpoint, status, next_cursor, error_message, updated_at
FROM mindshare.ingestion_window_checkpoint
WHERE run_id = '<run_id>'
ORDER BY updated_at DESC;
```

### 3. Check failed windows

```sql
SELECT run_id, project_keyword, window_id, endpoint, status, error_message, updated_at
FROM mindshare.ingestion_window_checkpoint
WHERE status = 'failed'
ORDER BY updated_at DESC
LIMIT 50;
```

Failed windows in Phase 1 (`endpoint = '/search-tweets'`) indicate the run itself failed. Failed windows in aux phases indicate per-item failures that were silently swallowed.

### 4. Verify project keyword merge on a known post

If the same post was ingested by two different keyword runs, its `project_keywords` array should contain both keywords:

```sql
SELECT post_id, project_keywords, last_seen_at, last_ingested_run_id
FROM mindshare.mindshare_post
WHERE post_id = <tweet_id_as_integer>;
```

### 5. Count posts per keyword

```sql
SELECT unnest(project_keywords) AS keyword, COUNT(*) AS post_count
FROM mindshare.mindshare_post
GROUP BY keyword
ORDER BY post_count DESC;
```

### 6. Check user scores populated

```sql
SELECT x_id, x_username, score, followers_count, last_score_fetched_at
FROM mindshare.mindshare_user
ORDER BY last_score_fetched_at DESC
LIMIT 50;
```

---

## Diagnosing failures

### Run marked `failed`

1. Query `error_summary` in `ingestion_run`:
   ```sql
   SELECT error_summary FROM mindshare.ingestion_run WHERE run_id = '<run_id>';
   ```
2. Find the failing search slice checkpoint:
   ```sql
   SELECT window_id, error_message
   FROM mindshare.ingestion_window_checkpoint
   WHERE run_id = '<run_id>' AND status = 'failed' AND endpoint = '/search-tweets';
   ```
3. The error message is the Python exception string from `SorsaClient` — typically a Sorsa API error response or a network timeout.

### Checkpoint table missing

If you see errors like:
```
relation "mindshare.ingestion_window_checkpoint" does not exist
```

Apply the DDL from the [Data Model](data-model.md#mindshareingestion_window_checkpoint) page.

### Rate limit errors (`SorsaRateLimitError`)

If the run fails with a `SorsaRateLimitError`, the Sorsa API returned HTTP 429 on all retry attempts. Solutions:

- Lower `SORSA_PER_KEY_RPS` to reduce the burst rate.
- Increase `SORSA_RETRY_429_SLEEP_SECONDS`.
- Lower `SEARCH_SLICE_COUNT` to reduce total concurrent requests.

### DB connection errors

- Confirm `COCKROACH_DATABASE_URL` is correct.
- Verify the CockroachDB cluster is reachable from your host.
- Check that SSL mode is set correctly (`sslmode=require` or `sslmode=disable` for local dev).
- The engine uses `pool_pre_ping=True`, so stale connections are caught early.

### Posts missing after a run

If a post is not in `mindshare_post`, the upsert likely skipped it because `post_id`, `user_x_id`, or `created_at` were missing from the API payload. Look for `WARNING` log lines containing `upsert_mindshare_posts_batch — skipping row missing field(s)` in the run output. The warning includes the `post_id` and which field(s) were absent.

---

## Tuning guidance

### Increase throughput

| Lever | Effect | Risk |
|---|---|---|
| Increase `SEARCH_SLICE_COUNT` | More parallel slice workers, smaller per-slice time window | Diminishing returns above `SORSA_PER_KEY_RPS`; more connections to DB |
| Increase `SEARCH_MAX_CONCURRENCY` | Raises the ceiling on concurrent tasks | No effect if already limited by `SEARCH_SLICE_COUNT` or `SORSA_PER_KEY_RPS` |
| Increase `SORSA_PER_KEY_RPS` | More requests per second to Sorsa | Will trigger 429s if set above actual API quota |
| Increase `DB_WRITE_BATCH_SIZE` | Fewer, larger DB transactions per slice | Higher memory usage per slice; larger individual transactions |
| Decrease `DB_WRITE_BATCH_SIZE` | More frequent, smaller DB flushes | More DB round trips; useful if memory is constrained |

Effective concurrency is always:
```
min(SEARCH_SLICE_COUNT, SEARCH_MAX_CONCURRENCY, SORSA_PER_KEY_RPS)
```

So all three levers must be raised together to increase effective concurrency.

### Reduce rate-limit pressure

- Lower `SORSA_PER_KEY_RPS` to stay within Sorsa's quota.
- Increase `SORSA_RETRY_429_SLEEP_SECONDS` to back off more gently on 429s.
- Lower `SEARCH_SLICE_COUNT` to reduce the number of concurrent outstanding requests.

### Reduce DB pressure

- The current implementation issues one DB call per tweet per phase (not batched). For high-volume keywords, this results in many small transactions. Future optimization: batch upserts.

---

## Known limitations

### Checkpoint resume not wired

Checkpoints are written after every page fetch. The cursor value is stored and could be used to resume interrupted windows. However, the **resume reader is not implemented** — every CLI run creates a brand new `ingestion_run` and re-fetches the full 72-hour window from scratch. If a run is interrupted halfway through, re-running it will re-fetch everything from the start (safely, due to upsert idempotency), not from the last successful checkpoint.

**Impact:** no data loss, but duplicated API calls on re-runs.

### Single API key only

`Settings.api_keys` always returns a single-element list (`[SORSA_API_KEY.strip()]`). The round-robin key assignment in `build_time_slices` and the `PerKeyRateLimiter` are already designed for multiple keys, but `config.py` does not parse a comma-separated key list. All slices and aux calls use `key_1`.

### Sequential aux phases

Phases 2, 3, and 4 iterate their input sets with a plain `for` loop — no concurrency within each phase. For large result sets (many posts or users), this can be slow. Future work: async task pool for aux phases mirroring the slice concurrency in Phase 1.

### DB write granularity

The search phase accumulates tweets in a per-slice in-memory buffer. A flush (two DB transactions via `executemany`) fires when the buffer hits `DB_WRITE_BATCH_SIZE` records (default: 1000), and always at the natural end of each slice. The minimum number of DB writes for a slice is 1 (if total records ≤ 1000); beyond that it is `ceil(total / 1000)`.

Aux phases (comments, user-tweets) write per page since those are per-post/per-user and individual volumes are much smaller.

Checkpoints and user score writes remain single-row.

### `source_m / source_n / source_u` flags not populated

`mindshare_post` has three source flag columns (`source_m`, `source_n`, `source_u`). These are not written by the ingestion pipeline and always default to `false`. Their intended semantics are not defined in this codebase.

### `IngestionContext` dataclass unused

`app/models.py` defines `IngestionContext` (run_id, project_keyword, slice_id, endpoint, api_key_alias). It is not referenced anywhere in the current implementation. It was likely intended for structured context propagation to the repository layer.

---

## Re-running after failure

Because all writes are idempotent upserts, re-running the same keyword is always safe:

```bash
python main.py --project-keyword Acurast
```

This creates a new `ingestion_run` row and re-fetches the 72-hour window. Existing posts in `mindshare_post` will have their engagement counts refreshed and their `project_keywords` left unchanged (the keyword is already in the array). The `raw_post_ingestion` table will have its `raw_json` updated to the latest payload.

---

## Recommended next steps

1. **Create `mindshare.ingestion_window_checkpoint`** in `ddl/ddl_mindshare_ingestion.sql` (see [Data Model](data-model.md#mindshareingestion_window_checkpoint)).
2. **Implement checkpoint resume** — on start, look up the last `running` or `failed` checkpoint for each slice/window and skip already-completed windows or resume from stored cursor.
3. **Add multi-key support** — parse `SORSA_API_KEY` as a comma-separated list in `Settings.api_keys`.
4. **Batch DB writes** — implemented. Each API page is written in two `executemany` transactions.
5. **Add concurrency to aux phases** — use an `asyncio.Semaphore`-bounded task pool similar to Phase 1's slice workers.
