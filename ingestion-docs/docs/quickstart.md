# Quickstart

This page walks through everything needed to configure and run the ingestion pipeline from a clean checkout.

---

## Prerequisites

- **Python 3.12** (see `.python-version` in the repo root — enforced by pyenv if present)
- **CockroachDB** instance reachable over a `postgresql+asyncpg://` SQLAlchemy URL
- A **Sorsa v3 API key**

---

## 1. Install dependencies

From the repository root:

```bash
pip install -e .
```

Dependencies declared in `pyproject.toml`:

| Package | Purpose |
|---|---|
| `aiohttp>=3.12.0` | Async HTTP client used by `SorsaClient` |
| `asyncpg>=0.30.0` | Async PostgreSQL driver used by SQLAlchemy |
| `python-dotenv>=1.1.0` | Loads `.env` file before `Settings` is instantiated |
| `pydantic>=2.11.0` | Data validation for `Settings` and dataclasses |
| `pydantic-settings>=2.10.0` | Env-var binding for `Settings` |
| `sqlalchemy>=2.0.0` | Async engine and session management |

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

!!! warning "Missing checkpoint table"
    `ingestion_window_checkpoint` is written to by the repository but is **not** defined in `ddl/ddl_mindshare_ingestion.sql`. You must create this table manually before running the pipeline. See the [Data Model](data-model.md#mindshareingestion_window_checkpoint) page for the full DDL.

Do **not** edit or replace `Mindshare DDL/ddl mindshare.sql` — that file is intentionally preserved as the legacy reference copy.

---

## 3. Configure environment variables

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

### Required variables

| Variable | Description |
|---|---|
| `COCKROACH_DATABASE_URL` | SQLAlchemy async URL: `postgresql+asyncpg://USER:PASS@HOST:26257/DB?sslmode=require` |
| `SORSA_API_KEY` | Your Sorsa v3 API key |

### Optional tuning variables

| Variable | Default | Description |
|---|---|---|
| `SORSA_BASE_URL` | `https://api.sorsa.io/v3` | Sorsa API base URL |
| `SORSA_PER_KEY_RPS` | `20` | Maximum requests per second for the API key |
| `SEARCH_SLICE_COUNT` | `20` | How many time slices the 72-hour window is divided into |
| `SEARCH_MAX_CONCURRENCY` | `20` | Upper bound on concurrent slice worker tasks |
| `SEARCH_ORDER` | `latest` | Tweet sort order passed to `/search-tweets` |
| `SORSA_MAX_RETRIES` | `4` | Maximum retry attempts per HTTP request |
| `SORSA_RETRY_429_SLEEP_SECONDS` | `1.0` | Sleep duration between retries on HTTP 429 |
| `SORSA_RETRY_5XX_SLEEP_SECONDS` | `2.0` | Sleep duration between retries on HTTP 5xx or network error |
| `DB_WRITE_BATCH_SIZE` | `1000` | Tweets accumulated per time slice before a DB flush is triggered (see below) |

### How DB batch writes work

The search phase accumulates tweets in an in-memory buffer per time slice. Two rules apply:

1. **Full batches** — as soon as the buffer reaches `DB_WRITE_BATCH_SIZE` records (default: 1000), that batch is written to DB immediately. The buffer is trimmed and page fetching continues.
2. **Final batch** — after all pages for a slice are fetched, any remaining records (< 1000) are written as the last batch.

Since Sorsa returns 20 tweets per page, one full batch of 1000 = 50 pages accumulated before the first write fires:

| Records in slice | Batches written | Sizes |
|---|---|---|
| 800 | 1 | 800 |
| 1000 | 1 | 1000 |
| 1500 | 2 | 1000, 500 |
| 3200 | 4 | 1000, 1000, 1000, 200 |
| 3000 | 3 | 1000, 1000, 1000 |

The buffer is **per slice** — concurrent slices each maintain their own independent buffer. The checkpoint is still written after every page regardless of buffer state.

### How effective concurrency is computed

The pipeline caps concurrent slice workers at:

```
effective_concurrency = min(SEARCH_SLICE_COUNT, SEARCH_MAX_CONCURRENCY, SORSA_PER_KEY_RPS)
```

This is enforced via a `asyncio.Semaphore` in `SearchIngestor.ingest_72h`. Examples:

| `SEARCH_SLICE_COUNT` | `SEARCH_MAX_CONCURRENCY` | `SORSA_PER_KEY_RPS` | Effective concurrency |
|---|---|---|---|
| 10 | 20 | 20 | 10 |
| 20 | 20 | 20 | 20 |
| 20 | 5 | 20 | 5 |
| 20 | 20 | 8 | 8 |

---

## 4. Run the pipeline

```bash
python main.py --project-keyword "<keyword(s)>" [--hours N]
```

| Argument | Required | Default | Description |
|---|---|---|---|
| `--project-keyword` | yes | — | One keyword, or multiple comma-separated terms (passed directly to Sorsa). |
| `--hours` | no | `72` | How many hours back to search. Use a small value for local testing. |

### Single-keyword run

```bash
python main.py --project-keyword Acurast
```

Sorsa receives: `Acurast since:... until:...`

### Multi-keyword run (project aliases)

Pass a comma-separated string. The pipeline sends it directly to the Sorsa API, which handles the multi-term matching internally:

```bash
python main.py --project-keyword "quipnetwork,Quip Network,quip_network,Quip"
```

Sorsa receives: `quipnetwork,Quip Network,quip_network,Quip since:... until:...`

The **first term** (`quipnetwork` above) is used as the project label stored in the database (`ingestion_run.project_keyword`, `mindshare_post.project_keywords`). All terms equally contribute to what is fetched.

**Local / test run (1 hour of data):**

```bash
python main.py --project-keyword Acurast --hours 1
python main.py --project-keyword "quipnetwork,Quip Network" --hours 1
```

On success, the CLI prints:

```
Ingestion completed. run_id=<uuid>
```

The `run_id` corresponds to a row in `mindshare.ingestion_run` and can be used to query checkpoints and raw data.

---

## 5. Verify the run

Check the run record:

```sql
SELECT run_id, project_keyword, run_status, started_at, finished_at
FROM mindshare.ingestion_run
ORDER BY started_at DESC
LIMIT 5;
```

Check ingested posts:

```sql
SELECT COUNT(*), project_keywords
FROM mindshare.mindshare_post
GROUP BY project_keywords
ORDER BY COUNT(*) DESC;
```

---

## How `.env` loading works

`app/cli.py` calls `load_dotenv()` explicitly before instantiating `Settings`. This means:

- A `.env` file at the working directory will always be picked up.
- The `Settings` class (Pydantic settings) will then override with any actual environment variables that are already set, per standard Pydantic-settings precedence.
- The `extra = "ignore"` setting means unknown variables in `.env` are silently ignored.
