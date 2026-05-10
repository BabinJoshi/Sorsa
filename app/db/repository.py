from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

import asyncpg

logger = logging.getLogger(__name__)


def _safe_str(value: Any) -> str:
    """Stringify value; treat Python None or the literal string 'None' as empty."""
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s == "None" else s


def _int_or_none(value: Any) -> int | None:
    """Parse to int; return None for missing/null/unparseable values.
    Uses float() first so '123.0' and 123.0 are handled correctly.
    """
    s = _safe_str(value)
    if not s:
        return None
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    """Parse to int with a fallback default for None/unparseable values."""
    result = _int_or_none(value)
    return result if result is not None else default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Parse to float with a fallback default."""
    s = _safe_str(value)
    if not s:
        return default
    try:
        return float(s)
    except (ValueError, OverflowError):
        return default


_TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"  # e.g. "Fri May 08 23:52:09 +0000 2026"


def _parse_created_at(value: Any) -> datetime | None:
    """Convert a created_at value to datetime.

    Accepts an existing datetime, an ISO-8601 string, or Twitter's
    'Fri May 08 23:52:09 +0000 2026' format. Returns None if unparseable.
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    try:
        return datetime.strptime(s, _TWITTER_DATE_FMT)
    except ValueError:
        pass
    try:
        from datetime import timezone
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


class IngestionRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------ #
    # Run lifecycle                                                        #
    # ------------------------------------------------------------------ #

    async def create_run(self, project_keyword: str, since: datetime, until: datetime) -> str:
        run_id = str(uuid4())
        await self._pool.execute(
            """
            INSERT INTO mindshare.ingestion_run (
                run_id, project_keyword, since_ts, until_ts, run_status, started_at
            ) VALUES ($1, $2, $3, $4, 'running', now())
            """,
            run_id, project_keyword, since, until,
        )
        logger.debug("DB: ingestion_run created — run_id=%s keyword=%r", run_id, project_keyword)
        return run_id

    async def mark_run_finished(self, run_id: str, status: str, error: str | None = None) -> None:
        await self._pool.execute(
            """
            UPDATE mindshare.ingestion_run
            SET run_status = $1, error_summary = $2, finished_at = now()
            WHERE run_id = $3
            """,
            status, error, run_id,
        )
        logger.debug("DB: ingestion_run updated — run_id=%s status=%s", run_id, status)

    # ------------------------------------------------------------------ #
    # Batch writes — normalized posts                                      #
    # ------------------------------------------------------------------ #

    async def upsert_mindshare_posts_batch(
        self,
        payloads: list[dict[str, Any]],
        project_keyword: str,
        run_id: str,
    ) -> None:
        """
        Upsert a batch of normalized posts in a single transaction.
        Rows missing post_id, user_x_id, or created_at are skipped with a warning.
        """
        records: list[tuple[Any, ...]] = []
        skipped = 0

        for payload in payloads:
            user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
            post_id   = _int_or_none(payload.get("id"))
            user_x_id = _int_or_none((user or {}).get("id"))
            created_at = _parse_created_at(payload.get("created_at"))

            if post_id is None or user_x_id is None or created_at is None:
                missing = [
                    f for f, v in [
                        ("id", post_id),
                        ("user.id", user_x_id),
                        ("created_at", created_at),
                    ]
                    if v is None
                ]
                logger.warning(
                    "DB: upsert_mindshare_posts_batch — skipping row missing field(s): %s "
                    "(raw_id=%r raw_user_id=%r raw_created_at=%r)",
                    ", ".join(missing),
                    payload.get("id"),
                    (user or {}).get("id"),
                    payload.get("created_at"),
                )
                skipped += 1
                continue

            records.append((
                post_id,                                                              # $1  post_id
                user_x_id,                                                            # $2  user_x_id
                [project_keyword],                                                    # $3  project_keywords TEXT[]
                _safe_str(payload.get("full_text")),                                  # $4  full_text
                _int_or_none((payload.get("retweeted_status") or {}).get("id")),      # $5  retweeted_post_id
                _int_or_none(payload.get("in_reply_to_tweet_id")),                    # $6  replied_post_id
                _int_or_none((payload.get("quoted_status") or {}).get("id")),         # $7  quoted_post_id
                _int_or_none(payload.get("conversation_id_str")),                     # $8  root_post_id
                _safe_int(payload.get("view_count")),                                 # $9  view_count
                _safe_int(payload.get("reply_count")),                                # $10 reply_count
                _safe_int(payload.get("retweet_count")),                              # $11 retweet_count
                _safe_int(payload.get("quote_count")),                                # $12 quote_count
                _safe_int(payload.get("likes_count")),                                # $13 favorite_count
                json.dumps(payload.get("entities") or []),                            # $14 entities (→ jsonb)
                created_at,                                                           # $15 post_created_at (datetime)
                run_id,                                                               # $16 last_ingested_run_id
            ))

        if not records:
            return

        sql = """
            INSERT INTO mindshare.mindshare_post (
                post_id,
                user_x_id,
                project_keywords,
                full_text,
                retweeted_post_id,
                replied_post_id,
                quoted_post_id,
                root_post_id,
                view_count,
                reply_count,
                retweet_count,
                quote_count,
                favorite_count,
                entities,
                post_created_at,
                last_ingested_run_id,
                last_seen_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13,
                $14::jsonb, $15, $16, now()
            )
            ON CONFLICT (post_id)
            DO UPDATE SET
                project_keywords = (
                    SELECT array_agg(DISTINCT k)
                    FROM unnest(
                        COALESCE(mindshare.mindshare_post.project_keywords, ARRAY[]::TEXT[])
                        || EXCLUDED.project_keywords
                    ) AS k
                ),
                full_text = CASE
                    WHEN EXCLUDED.full_text IS NOT NULL AND EXCLUDED.full_text != ''
                    THEN EXCLUDED.full_text
                    ELSE mindshare.mindshare_post.full_text
                END,
                user_x_id            = EXCLUDED.user_x_id,
                retweeted_post_id    = COALESCE(EXCLUDED.retweeted_post_id, mindshare.mindshare_post.retweeted_post_id),
                replied_post_id      = COALESCE(EXCLUDED.replied_post_id,   mindshare.mindshare_post.replied_post_id),
                quoted_post_id       = COALESCE(EXCLUDED.quoted_post_id,    mindshare.mindshare_post.quoted_post_id),
                root_post_id         = COALESCE(EXCLUDED.root_post_id,      mindshare.mindshare_post.root_post_id),
                view_count           = EXCLUDED.view_count,
                reply_count          = EXCLUDED.reply_count,
                retweet_count        = EXCLUDED.retweet_count,
                quote_count          = EXCLUDED.quote_count,
                favorite_count       = EXCLUDED.favorite_count,
                entities             = COALESCE(EXCLUDED.entities, mindshare.mindshare_post.entities),
                last_ingested_run_id = EXCLUDED.last_ingested_run_id,
                last_seen_at         = now()
        """

        _DB_WRITE_RETRIES = 3
        for attempt in range(1, _DB_WRITE_RETRIES + 1):
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(sql, records)
                break
            except (
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                OSError,
            ) as exc:
                if attempt >= _DB_WRITE_RETRIES:
                    logger.error(
                        "DB: upsert_mindshare_posts_batch — connection error on final "
                        "attempt %d/%d: %s",
                        attempt, _DB_WRITE_RETRIES, exc,
                    )
                    raise
                logger.warning(
                    "DB: upsert_mindshare_posts_batch — connection error (attempt %d/%d), "
                    "retrying in %.1fs: %s",
                    attempt, _DB_WRITE_RETRIES, attempt * 1.0, exc,
                )
                await asyncio.sleep(attempt * 1.0)

        logger.debug(
            "DB: upsert_mindshare_posts_batch — %d rows upserted, %d skipped",
            len(records),
            skipped,
        )

    # ------------------------------------------------------------------ #
    # User scores                                                         #
    # ------------------------------------------------------------------ #

    async def upsert_user_score(
        self,
        x_id: str,
        username: str | None,
        display_name: str | None,
        avatar_url: str | None,
        followers_count: int | None,
        score: float | int | None,
    ) -> None:
        x_id_int = _int_or_none(x_id)
        if x_id_int is None:
            logger.warning("DB: upsert_user_score skipped — unparseable x_id=%r", x_id)
            return
        await self._pool.execute(
            """
            INSERT INTO mindshare.mindshare_user (
                x_id, x_username, display_name, score, avatar_url,
                adjustment_config, followers_count, verified, last_score_fetched_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                '{"default": 1}'::jsonb,
                $6, false, now()
            )
            ON CONFLICT (x_id)
            DO UPDATE SET
                x_username            = COALESCE(NULLIF(EXCLUDED.x_username, ''),   mindshare.mindshare_user.x_username),
                display_name          = COALESCE(NULLIF(EXCLUDED.display_name, ''), mindshare.mindshare_user.display_name),
                score                 = COALESCE(EXCLUDED.score,                    mindshare.mindshare_user.score),
                avatar_url            = COALESCE(NULLIF(EXCLUDED.avatar_url, ''),   mindshare.mindshare_user.avatar_url),
                followers_count       = COALESCE(EXCLUDED.followers_count,          mindshare.mindshare_user.followers_count),
                last_score_fetched_at = now(),
                updated_at            = now()
            """,
            x_id_int,
            _safe_str(username),
            _safe_str(display_name),
            _safe_float(score),
            _safe_str(avatar_url),
            _safe_int(followers_count),
        )
        logger.debug("DB: upsert_user_score — x_id=%s", x_id)
