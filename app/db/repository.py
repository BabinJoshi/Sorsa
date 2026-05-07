from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class IngestionRepository:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self._session_maker = session_maker

    async def create_run(self, project_keyword: str, since: datetime, until: datetime) -> str:
        run_id = str(uuid4())
        sql = text(
            """
            INSERT INTO mindshare.ingestion_run (
                run_id, project_keyword, since_ts, until_ts, run_status, started_at
            ) VALUES (:run_id, :project_keyword, :since_ts, :until_ts, 'running', now())
            """
        )
        async with self._session_maker() as session:
            await session.execute(
                sql,
                {
                    "run_id": run_id,
                    "project_keyword": project_keyword,
                    "since_ts": since,
                    "until_ts": until,
                },
            )
            await session.commit()
        return run_id

    async def mark_run_finished(self, run_id: str, status: str, error: str | None = None) -> None:
        sql = text(
            """
            UPDATE mindshare.ingestion_run
            SET run_status = :status,
                error_summary = :error,
                finished_at = now()
            WHERE run_id = :run_id
            """
        )
        async with self._session_maker() as session:
            await session.execute(sql, {"run_id": run_id, "status": status, "error": error})
            await session.commit()

    async def upsert_post_raw(
        self,
        run_id: str,
        project_keyword: str,
        endpoint: str,
        payload: dict[str, Any],
    ) -> None:
        post_id = str(payload.get("id", "")).strip()
        if not post_id:
            return
        sql = text(
            """
            INSERT INTO raw_data.raw_post_ingestion (
                run_id, project_keyword, endpoint, post_id, raw_json, fetched_at
            )
            VALUES (:run_id, :project_keyword, :endpoint, :post_id, :raw_json::jsonb, :fetched_at)
            ON CONFLICT (post_id, endpoint)
            DO UPDATE SET
                raw_json = EXCLUDED.raw_json,
                fetched_at = EXCLUDED.fetched_at,
                project_keyword = EXCLUDED.project_keyword,
                run_id = EXCLUDED.run_id
            """
        )
        async with self._session_maker() as session:
            await session.execute(
                sql,
                {
                    "run_id": run_id,
                    "project_keyword": project_keyword,
                    "endpoint": endpoint,
                    "post_id": post_id,
                    "raw_json": json.dumps(payload),
                    "fetched_at": datetime.now(timezone.utc),
                },
            )
            await session.commit()

    async def upsert_mindshare_post(self, payload: dict[str, Any], project_keyword: str, run_id: str) -> None:
        post_id = str(payload.get("id", "")).strip()
        user = payload.get("user", {}) if isinstance(payload.get("user"), dict) else {}
        user_x_id = str(user.get("id", "")).strip()
        created_at = payload.get("created_at")
        if not post_id or not user_x_id or not created_at:
            return

        sql = text(
            """
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
                :post_id::INT8,
                :user_x_id::INT8,
                ARRAY[:project_keyword],
                :full_text,
                NULLIF(:retweeted_post_id, '')::INT8,
                NULLIF(:replied_post_id, '')::INT8,
                NULLIF(:quoted_post_id, '')::INT8,
                NULLIF(:root_post_id, '')::INT8,
                :view_count,
                :reply_count,
                :retweet_count,
                :quote_count,
                :favorite_count,
                :entities::jsonb,
                :post_created_at::timestamptz,
                :run_id,
                now()
            )
            ON CONFLICT (post_id)
            DO UPDATE SET
                project_keywords = (
                    SELECT array_agg(DISTINCT k)
                    FROM unnest(
                        COALESCE(mindshare.mindshare_post.project_keywords, ARRAY[]::TEXT[])
                        || ARRAY[EXCLUDED.project_keywords[1]]
                    ) AS k
                ),
                full_text = CASE
                    WHEN EXCLUDED.full_text IS NOT NULL AND EXCLUDED.full_text != '' THEN EXCLUDED.full_text
                    ELSE mindshare.mindshare_post.full_text
                END,
                user_x_id = EXCLUDED.user_x_id,
                retweeted_post_id = COALESCE(EXCLUDED.retweeted_post_id, mindshare.mindshare_post.retweeted_post_id),
                replied_post_id = COALESCE(EXCLUDED.replied_post_id, mindshare.mindshare_post.replied_post_id),
                quoted_post_id = COALESCE(EXCLUDED.quoted_post_id, mindshare.mindshare_post.quoted_post_id),
                root_post_id = COALESCE(EXCLUDED.root_post_id, mindshare.mindshare_post.root_post_id),
                view_count = EXCLUDED.view_count,
                reply_count = EXCLUDED.reply_count,
                retweet_count = EXCLUDED.retweet_count,
                quote_count = EXCLUDED.quote_count,
                favorite_count = EXCLUDED.favorite_count,
                entities = COALESCE(EXCLUDED.entities, mindshare.mindshare_post.entities),
                last_ingested_run_id = EXCLUDED.last_ingested_run_id,
                last_seen_at = now()
            """
        )
        async with self._session_maker() as session:
            await session.execute(
                sql,
                {
                    "post_id": post_id,
                    "user_x_id": user_x_id,
                    "project_keyword": project_keyword,
                    "full_text": payload.get("full_text", ""),
                    "retweeted_post_id": str(
                        (payload.get("retweeted_status") or {}).get("id", "")
                    ),
                    "replied_post_id": str(payload.get("in_reply_to_tweet_id", "")),
                    "quoted_post_id": str(
                        (payload.get("quoted_status") or {}).get("id", "")
                    ),
                    "root_post_id": str(payload.get("conversation_id_str", "")),
                    "view_count": int(payload.get("view_count", 0) or 0),
                    "reply_count": int(payload.get("reply_count", 0) or 0),
                    "retweet_count": int(payload.get("retweet_count", 0) or 0),
                    "quote_count": int(payload.get("quote_count", 0) or 0),
                    "favorite_count": int(payload.get("likes_count", 0) or 0),
                    "entities": json.dumps(payload.get("entities", [])),
                    "post_created_at": created_at,
                    "run_id": run_id,
                },
            )
            await session.commit()

    async def upsert_window_checkpoint(
        self,
        run_id: str,
        project_keyword: str,
        window_id: str,
        endpoint: str,
        api_key_alias: str,
        next_cursor: str | None,
        status: str,
        error_message: str | None = None,
    ) -> None:
        sql = text(
            """
            INSERT INTO mindshare.ingestion_window_checkpoint (
                run_id, project_keyword, window_id, endpoint, api_key_alias,
                next_cursor, status, error_message, updated_at
            )
            VALUES (
                :run_id, :project_keyword, :window_id, :endpoint, :api_key_alias,
                :next_cursor, :status, :error_message, now()
            )
            ON CONFLICT (run_id, project_keyword, window_id, endpoint)
            DO UPDATE SET
                api_key_alias = EXCLUDED.api_key_alias,
                next_cursor = EXCLUDED.next_cursor,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = now()
            """
        )
        async with self._session_maker() as session:
            await session.execute(
                sql,
                {
                    "run_id": run_id,
                    "project_keyword": project_keyword,
                    "window_id": window_id,
                    "endpoint": endpoint,
                    "api_key_alias": api_key_alias,
                    "next_cursor": next_cursor,
                    "status": status,
                    "error_message": error_message,
                },
            )
            await session.commit()

    async def upsert_user_score(
        self,
        x_id: str,
        username: str | None,
        display_name: str | None,
        avatar_url: str | None,
        followers_count: int | None,
        score: float | int | None,
    ) -> None:
        if not x_id.strip():
            return
        sql = text(
            """
            INSERT INTO mindshare.mindshare_user (
                x_id, x_username, display_name, score, avatar_url,
                adjustment_config, followers_count, verified, last_score_fetched_at
            )
            VALUES (
                :x_id::INT8,
                :x_username,
                :display_name,
                :score,
                :avatar_url,
                '{"default": 1}'::jsonb,
                :followers_count,
                false,
                now()
            )
            ON CONFLICT (x_id)
            DO UPDATE SET
                x_username = COALESCE(NULLIF(EXCLUDED.x_username, ''), mindshare.mindshare_user.x_username),
                display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), mindshare.mindshare_user.display_name),
                score = COALESCE(EXCLUDED.score, mindshare.mindshare_user.score),
                avatar_url = COALESCE(NULLIF(EXCLUDED.avatar_url, ''), mindshare.mindshare_user.avatar_url),
                followers_count = COALESCE(EXCLUDED.followers_count, mindshare.mindshare_user.followers_count),
                last_score_fetched_at = now(),
                updated_at = now()
            """
        )
        async with self._session_maker() as session:
            await session.execute(
                sql,
                {
                    "x_id": x_id,
                    "x_username": username or "",
                    "display_name": display_name or "",
                    "score": float(score or 0),
                    "avatar_url": avatar_url or "",
                    "followers_count": int(followers_count or 0),
                },
            )
            await session.commit()

