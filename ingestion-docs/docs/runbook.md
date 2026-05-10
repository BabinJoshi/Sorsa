# Operations Runbook

This page covers how to run, monitor, debug, and tune the ingestion pipeline.

---

## Running the pipeline

### Standard run (72-hour window)

```bash
uv run main.py --project-keyword <keyword>
```

### Multi-keyword run

```bash
uv run main.py --project-keyword "quipnetwork,Quip Network,quip_network,Quip"
```

The pipeline converts comma-separated terms into standard Twitter OR-syntax and sends it to Sorsa:
`quipnetwork OR "Quip Network" OR quip_network OR Quip since:... until:...`

### Short window for local testing

```bash
uv run main.py --project-keyword Acurast --hours 3
```

### Exact time window

```bash
uv run main.py --project-keyword "quipnetwork,Quip Network,quip_network,Quip" \
    --since "2026-05-06 08:01" \
    --until "2026-05-09 08:01"
```

`--since`/`--until` take precedence over `--hours`.

### On success

```
Ingestion completed. run_id=3fa85f64-5717-4562-b3fc-2c963f66afa6  elapsed=19.7 min
```

The `run_id` corresponds to a row in `mindshare.ingestion_run`. Log output is also written to `logs/YYYY-MM-DD/run_HHMMSS_<id>.log`.

On failure, a Python traceback is printed, the run is marked `failed` in the database, and the process exits non-zero.

### Overriding settings at runtime

```bash
SEARCH_SLICE_COUNT=10 SORSA_PER_KEY_RPS=10 uv run main.py --project-keyword Acurast
```

---

## Reading the logs

### Log file location

Every run writes to:
```
logs/YYYY-MM-DD/run_HHMMSS_<8-char-uuid>.log
```

Example: `logs/2026-05-09/run_103915_9b402f04.log`

The log file path is printed on the first INFO line:
```
2026-05-09 10:39:15 [INFO] app.cli: Log file: logs/2026-05-09/run_103915_9b402f04.log
```

Both console and file receive the same log output. Log files are never truncated — each run creates a new file.

### Reading request counts

The orchestrator logs a cumulative request count snapshot after every phase boundary:

```
2026-05-09 10:39:17 [INFO] app.clients.sorsa_client: After phase 1: Request counts — total=71 | key_1=26, key_2=45
2026-05-09 10:39:18 [INFO] app.clients.sorsa_client: After phases 2+3+4: Request counts — total=15357 | key_1=7708, key_2=7649
2026-05-09 10:39:18 [INFO] app.clients.sorsa_client: Final totals: Request counts — total=15357 | key_1=7708, key_2=7649
```

Subtract consecutive snapshots to get per-phase counts. Counts are cumulative and include all retries.

### Reading elapsed time

Total run time is logged at the very end:
```
2026-05-09 10:59:02 [INFO] app.pipeline.orchestrator: Ingestion completed — run_id=... | elapsed=19.7 min (19m 42s)
```

### Enabling DEBUG output

To see per-request dispatches, per-page pagination details, and per-DB-operation traces, temporarily change `level=logging.INFO` to `level=logging.DEBUG` in `_configure_logging()` inside `app/cli.py`.

DEBUG lines include things like:
- `[key_1] POST /search-tweets — attempt 1/5` (every request dispatched)
- `[slice 3] Page 47 — 20 tweets | buffer=940/1000 | has_next=True` (every page fetched)

### What WARNING lines mean

| Source | Meaning |
|---|---|
| `app.clients.sorsa_client` | A retry was triggered (429, 5xx, empty body). Run continues. |
| `app.pipeline.aux_ingestors` | A per-post or per-user fetch failed; being retried or permanently skipped. Run continues. |
| `app.db.repository` | A row was skipped at DB write time (missing required field). Run continues. |

An `ERROR` from `app.pipeline.orchestrator` means the run itself failed (Phase 1 aborted). An `ERROR` from `app.clients.sorsa_client` means retries were exhausted on a specific request. An `ERROR` from `app.db.repository` means all DB write retries were exhausted for a batch.

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

A run stuck in `running` with an old `started_at` and no `finished_at` means the process was killed or crashed without updating the record.

### 2. Count posts per keyword

```sql
SELECT unnest(project_keywords) AS keyword, COUNT(*) AS post_count
FROM mindshare.mindshare_post
GROUP BY keyword
ORDER BY post_count DESC;
```

### 3. Check user scores populated

```sql
SELECT x_id, x_username, score, followers_count, last_score_fetched_at
FROM mindshare.mindshare_user
ORDER BY last_score_fetched_at DESC
LIMIT 50;
```

### 4. Verify project keyword merge on a known post

```sql
SELECT post_id, project_keywords, last_seen_at, last_ingested_run_id
FROM mindshare.mindshare_post
WHERE post_id = '<tweet_id>';
```

A post ingested by two different keyword runs should show both keywords in `project_keywords`.

### 5. Find runs for a specific project

```sql
SELECT run_id, run_status, started_at, finished_at - started_at AS duration, error_summary
FROM mindshare.ingestion_run
WHERE project_keyword = 'quipnetwork'
ORDER BY started_at DESC;
```

---

## Diagnosing failures

### Run marked `failed`

1. Check `error_summary` in `ingestion_run`:
   ```sql
   SELECT error_summary FROM mindshare.ingestion_run WHERE run_id = '<run_id>';
   ```
2. Open the corresponding log file (`logs/YYYY-MM-DD/run_HHMMSS_<id>.log`) and search for `[ERROR]`.
3. Phase 1 failures (`SearchIngestor`) re-raise and abort the run. The exception message is usually a Sorsa API error or network timeout.

### Rate limit errors (`SorsaRateLimitError`)

If the run fails with `SorsaRateLimitError`, Sorsa returned HTTP 429 on all retry attempts. Solutions:

- Lower `SORSA_PER_KEY_RPS` to reduce the burst rate.
- Increase `SORSA_RETRY_429_SLEEP_SECONDS`.
- Lower `SEARCH_SLICE_COUNT` to reduce concurrent outstanding requests.

### DB connection errors

- Confirm `COCKROACH_DATABASE_URL` is correct and reachable.
- Check that SSL mode matches your cluster config (`sslmode=require` for cloud, `sslmode=disable` for local dev).
- `max_inactive_connection_lifetime=300.0` is set on the pool to recycle idle connections before CockroachDB's server-side timeout closes them.
- The repository retries DB write operations up to 3 times on `PostgresConnectionError`, `ConnectionDoesNotExistError`, `InterfaceError`, and `OSError`. If all retries fail, an `ERROR` is logged.

### `skipping row missing field(s)` warnings

```
DB: upsert_mindshare_posts_batch — skipping row missing field(s): id, created_at
```

This happens when a non-tweet object (e.g. a user profile card) appears in the API's `tweets` array. The row is intentionally dropped. This is not recoverable and is expected with Sorsa's API — these objects simply lack the required fields.

### Permanently failed users or posts (aux phases)

After each aux phase, the log reports permanently failed items:
```
[user-tweets] 3 user(s) still failed after retry: [uid1, uid2, uid3]
```

These users/posts had at least one failed page fetch even after the retry pass. Causes:
- Persistent `empty/non-JSON body` responses from Sorsa for a specific user/post.
- The account was deleted or suspended between Phase 1 and Phase 3.

These IDs are only logged — they are not stored in a failed queue. Re-running the pipeline will attempt them again.

### Fewer results with multi-key runs

The Sorsa/Twitter search API is relevance-ranked and non-exhaustive. More API keys → more concurrency, but not necessarily more unique tweets. Factors that affect result count:

- **Slice width** — narrower slices (higher `SEARCH_SLICE_COUNT`) may return fewer results per slice than a single broader query covering the same period. The API's ranking algorithm finds fewer relevant results in a 1.8h window than in a 3.6h window for the same keyword.
- **API sampling** — Twitter's search API returns a relevance-sampled subset, not all matching tweets.

For the best coverage, keep `SEARCH_SLICE_COUNT=20` (3.6h per slice over 72h) unless you have a specific reason to change it.

---

## Tuning guidance

### Increase throughput

| Lever | Effect | Recommendation |
|---|---|---|
| Add more API keys to `SORSA_API_KEYS` | `SEARCH_MAX_CONCURRENCY` and `AUX_MAX_CONCURRENCY` auto-scale | Most impactful; set `SORSA_PER_KEY_RPS` to match actual quota |
| Increase `SORSA_PER_KEY_RPS` | Raises auto-computed concurrency | Only if Sorsa increases your per-key rate limit |
| Increase `SEARCH_SLICE_COUNT` | More slices can run in parallel | Diminishing returns; can hurt result completeness if slices become too narrow |
| Increase `DB_WRITE_BATCH_SIZE` | Fewer, larger DB transactions per slice | Higher per-slice memory; marginal DB improvement |

### Cap concurrency below auto-computed ceiling

Set explicit overrides in `.env` to limit concurrency even when more keys are present:

```env
SEARCH_MAX_CONCURRENCY=20
AUX_MAX_CONCURRENCY=40
```

### Understanding the per-phase performance bottleneck

**Phase 1** is typically fast — 20 slices × concurrent HTTP fetches.

**Phase 3 (user timelines)** typically dominates total run time. A run returning 5,000 unique users with ~30 pages each requires ~150,000 HTTP requests. Even at 100 concurrent requests (2 keys × 50 RPS), this takes ~30 minutes of sustained I/O. Adding more keys reduces this proportionally.

**Phase 2 (comments)** tends to be lighter — most posts have few or no comments.

**Phase 4 (scores)** is one request per user; usually completes quickly at high concurrency.

---

## Re-running after failure

All DB writes are idempotent upserts. Re-running the same keyword is always safe:

```bash
uv run main.py --project-keyword Acurast
```

This creates a new `ingestion_run` row and re-fetches the configured window. Existing posts in `mindshare_post` will have their engagement counts refreshed and `project_keywords` preserved unchanged (the keyword is already in the array).

To re-run for the exact same window as a previous run, use `--since`/`--until`:

```bash
uv run main.py --project-keyword Acurast --since "2026-05-06 08:00" --until "2026-05-09 08:00"
```

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

## Known limitations

### No checkpoint resume

There is no checkpoint/cursor table. Every CLI invocation starts a fresh run and re-fetches the full window from scratch. If a run is interrupted, re-running it will re-fetch everything from the beginning (safely, due to upsert idempotency), not from the last successful page.

**Impact:** no data loss on re-run, but duplicated API calls.

### User timelines fetch all pages (no time bound)

`/user-tweets` fetches all available pages for a user — there is no since/until parameter for this endpoint. This means a user with a long tweet history may trigger hundreds of API calls. All their tweets are stored and upserted, even if they predate the search window.

### Tweets fetched in Phase 1 are re-fetched in Phase 3

Phase 1 (search) finds tweets and collects their author IDs. Phase 3 (user timelines) fetches every page of every author's timeline — including tweets already ingested in Phase 1. The upserts are idempotent so no duplication occurs in the database, but API calls are duplicated. This is a known trade-off of the current design.

### `source_m / source_n / source_u` flags not populated

`mindshare_post` has three source flag columns (`source_m`, `source_n`, `source_u`). These are not written by the ingestion pipeline and always default to `false`. Their intended semantics are not defined in this codebase.

### `IngestionContext` dataclass unused

`app/models.py` defines `IngestionContext`. It is not referenced in the current implementation — intended for future structured context propagation.
