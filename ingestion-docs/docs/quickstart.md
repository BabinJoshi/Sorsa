# Quickstart

This page walks through everything needed to configure and run the ingestion pipeline from a clean checkout.

---

## Prerequisites

- **Python 3.12** (see `.python-version` in the repo root — enforced by pyenv if present)
- **CockroachDB** instance reachable over a `postgresql://` DSN (with or without `sslmode=require`)
- One or more **Sorsa v3 API keys**

---

## 1. Install dependencies

From the repository root:

```bash
pip install -e .
```

Or if the project uses `uv`:

```bash
uv sync
```

Dependencies declared in `pyproject.toml`:

| Package | Purpose |
|---|---|
| `aiohttp>=3.12.0` | Async HTTP client used by `SorsaClient` |
| `asyncpg>=0.30.0` | Async PostgreSQL driver; connects directly to CockroachDB |
| `python-dotenv>=1.1.0` | Loads `.env` file before `Settings` is instantiated |
| `pydantic>=2.11.0` | Data validation for `Settings` and dataclasses |
| `pydantic-settings>=2.10.0` | Env-var binding for `Settings` |

Optional — for building these docs:

```bash
pip install -e ".[docs]"
```

---

## 2. Apply the database schema

Run the ingestion DDL against your CockroachDB cluster. The file is idempotent (`IF NOT EXISTS` guards on every object):

```bash
cockroach sql --url "$COCKROACH_DATABASE_URL" < ddl/ddl_mindshare_ingestion.sql
```

Or paste the contents of `ddl/ddl_mindshare_ingestion.sql` directly into your preferred SQL client.

---

## 3. Configure environment variables

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

### Required variables

| Variable | Description |
|---|---|
| `COCKROACH_DATABASE_URL` | CockroachDB connection URL: `postgresql+asyncpg://USER:PASS@HOST:26257/DB?sslmode=require` |
| `SORSA_API_KEYS` | One or more Sorsa v3 API keys, comma-separated. Single key: `"sk-abc123"`. Two keys: `"sk-abc123,sk-def456"` |

### Optional tuning variables

| Variable | Default | Description |
|---|---|---|
| `SORSA_BASE_URL` | `https://api.sorsa.io/v3` | Sorsa API base URL |
| `SORSA_PER_KEY_RPS` | `20` | Maximum requests per second per key. Raise only if Sorsa increases your rate limit. |
| `SEARCH_SLICE_COUNT` | `20` | How many time slices the search window is divided into |
| `SEARCH_MAX_CONCURRENCY` | *(auto)* | Upper bound on concurrent Phase 1 slice workers. Auto-computed as `len(keys) × SORSA_PER_KEY_RPS` if not set. |
| `AUX_MAX_CONCURRENCY` | *(auto)* | Upper bound on concurrent tasks per aux phase (comments, timelines, scores). Same auto-computation. |
| `SEARCH_ORDER` | `latest` | Tweet sort order passed to `/search-tweets` |
| `SORSA_MAX_RETRIES` | `4` | Maximum retry attempts per HTTP request (429, 5xx, network errors, empty body) |
| `SORSA_RETRY_429_SLEEP_SECONDS` | `1.0` | Sleep between retries on HTTP 429 |
| `SORSA_RETRY_5XX_SLEEP_SECONDS` | `2.0` | Sleep between retries on HTTP 5xx, network error, or empty/non-JSON body |
| `DB_WRITE_BATCH_SIZE` | `1000` | Tweets accumulated in-memory per slice before a DB flush is triggered |

### Adding more API keys

Add the new key to `SORSA_API_KEYS` (comma-separated):

```env
SORSA_API_KEYS=sk-key1,sk-key2,sk-key3
```

No other configuration is needed. `SEARCH_MAX_CONCURRENCY` and `AUX_MAX_CONCURRENCY` auto-scale:
- 1 key × 20 RPS → max concurrency = 20
- 2 keys × 20 RPS → max concurrency = 40
- 2 keys × 50 RPS → max concurrency = 100

To increase the rate if Sorsa upgrades your quota:

```env
SORSA_PER_KEY_RPS=50
```

Both concurrency values update automatically.

### How DB batch writes work

The search phase accumulates tweets in an in-memory buffer per time slice. Two rules apply:

1. **Full batches** — as soon as the buffer reaches `DB_WRITE_BATCH_SIZE` records, that batch is written immediately. The buffer is trimmed and page fetching continues.
2. **Final drain** — after all pages for a slice are fetched, any remaining records (< `DB_WRITE_BATCH_SIZE`) are written as the last batch.

Sorsa returns 20 tweets per page. One full batch of 1000 = 50 pages accumulated before the first write fires:

| Records in slice | Batches written | Sizes |
|---|---|---|
| 800 | 1 | 800 |
| 1000 | 1 | 1000 |
| 1500 | 2 | 1000, 500 |
| 3200 | 4 | 1000, 1000, 1000, 200 |

The buffer is **per slice** — concurrent slices each maintain their own independent buffer.

### How effective concurrency is computed

**Phase 1 (search):**
```
effective_concurrency = max(1, min(SEARCH_SLICE_COUNT, SEARCH_MAX_CONCURRENCY))
```

`SEARCH_MAX_CONCURRENCY` defaults to `len(keys) × SORSA_PER_KEY_RPS`.

| `SEARCH_SLICE_COUNT` | Keys | `SORSA_PER_KEY_RPS` | `SEARCH_MAX_CONCURRENCY` (auto) | Effective |
|---|---|---|---|---|
| 20 | 1 | 20 | 20 | 20 |
| 20 | 2 | 20 | 40 | 20 *(slice count caps it)* |
| 40 | 2 | 20 | 40 | 40 |
| 20 | 2 | 50 | 100 | 20 *(slice count caps it)* |

**Aux phases (comments, timelines, scores):**

Each aux phase has its own semaphore:
```
aux_effective_concurrency = AUX_MAX_CONCURRENCY  (default: len(keys) × SORSA_PER_KEY_RPS)
```

Unlike Phase 1, aux phases are not bounded by `SEARCH_SLICE_COUNT` — they can run up to the full `aux_max_concurrency` tasks simultaneously.

---

## 4. Run the pipeline

```bash
uv run main.py --project-keyword "<keyword(s)>" [--hours N | --since DATETIME [--until DATETIME]]
```

| Argument | Required | Default | Description |
|---|---|---|---|
| `--project-keyword` | yes | — | One keyword, or multiple comma-separated terms |
| `--hours N` | no | `72` | How many hours back to search (ignored if `--since` is given) |
| `--since DATETIME` | no | — | Explicit UTC start: `"2026-05-06 08:01"` or `"2026-05-06"` |
| `--until DATETIME` | no | now | Explicit UTC end (only meaningful with `--since`) |

### Single-keyword run (72 hours)

```bash
uv run main.py --project-keyword Acurast
```

Search query sent to API: `Acurast since:... until:...`

### Multi-keyword run (OR syntax)

```bash
uv run main.py --project-keyword "quipnetwork,Quip Network,quip_network,Quip"
```

Search query sent to API: `quipnetwork OR "Quip Network" OR quip_network OR Quip since:... until:...`

The **first term** (`quipnetwork`) is used as the project label stored in the database. All terms equally contribute to what is fetched.

### Local / test run with `--hours`

```bash
uv run main.py --project-keyword Acurast --hours 1
uv run main.py --project-keyword "quipnetwork,Quip Network" --hours 3
```

### Exact time window with `--since` / `--until`

To target a specific window (useful for debugging or re-running a known data-rich period):

```bash
uv run main.py --project-keyword "quipnetwork,Quip Network,quip_network,Quip" \
    --since "2026-05-06 08:01" \
    --until "2026-05-09 08:01"
```

`--since` without `--until` uses the current time as the end:

```bash
uv run main.py --project-keyword Acurast --since "2026-05-08 00:00"
```

### On success

```
Ingestion completed. run_id=3fa85f64-5717-4562-b3fc-2c963f66afa6  elapsed=19.7 min
```

Log output is also written to `logs/YYYY-MM-DD/run_HHMMSS_<id>.log`.

---

## 5. Verify the run

Check the run record:

```sql
SELECT run_id, project_keyword, run_status, started_at, finished_at,
       finished_at - started_at AS duration
FROM mindshare.ingestion_run
ORDER BY started_at DESC
LIMIT 5;
```

Check ingested posts per project:

```sql
SELECT unnest(project_keywords) AS keyword, COUNT(*) AS post_count
FROM mindshare.mindshare_post
GROUP BY keyword
ORDER BY post_count DESC;
```

Check user scores:

```sql
SELECT x_id, x_username, score, followers_count, last_score_fetched_at
FROM mindshare.mindshare_user
ORDER BY last_score_fetched_at DESC
LIMIT 20;
```

---

## How `.env` loading works

`app/cli.py` calls `load_dotenv()` explicitly before instantiating `Settings`. This means:

- A `.env` file at the working directory will always be picked up.
- `Settings` (Pydantic-settings) will override with any actual environment variables already set, per standard Pydantic-settings precedence (env vars > `.env` > defaults).
- `extra = "ignore"` means unknown variables in `.env` are silently ignored.
