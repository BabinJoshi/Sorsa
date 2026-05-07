from __future__ import annotations

from typing import Iterable

from app.clients.sorsa_client import ApiKeyState, SorsaClient
from app.db.repository import IngestionRepository


class AuxiliaryIngestors:
    def __init__(self, repo: IngestionRepository, sorsa_client: SorsaClient) -> None:
        self._repo = repo
        self._client = sorsa_client

    async def ingest_comments_for_posts(
        self,
        run_id: str,
        project_keyword: str,
        posts: Iterable[str],
        key: ApiKeyState,
    ) -> None:
        for post_id in posts:
            cursor: str | None = None
            window_id = f"comments_{post_id}"
            while True:
                try:
                    data = await self._client.comments(
                        key_state=key, tweet_link=post_id, cursor=cursor
                    )
                    tweets = data.get("tweets", []) or []
                    for tweet in tweets:
                        if not isinstance(tweet, dict):
                            continue
                        await self._repo.upsert_post_raw(
                            run_id=run_id,
                            project_keyword=project_keyword,
                            endpoint="/comments",
                            payload=tweet,
                        )
                        await self._repo.upsert_mindshare_post(
                            payload=tweet,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                    cursor = data.get("next_cursor")
                    await self._repo.upsert_window_checkpoint(
                        run_id=run_id,
                        project_keyword=project_keyword,
                        window_id=window_id,
                        endpoint="/comments",
                        api_key_alias=key.alias,
                        next_cursor=cursor,
                        status="running" if cursor else "completed",
                    )
                    if not cursor:
                        break
                except Exception as exc:
                    await self._repo.upsert_window_checkpoint(
                        run_id=run_id,
                        project_keyword=project_keyword,
                        window_id=window_id,
                        endpoint="/comments",
                        api_key_alias=key.alias,
                        next_cursor=cursor,
                        status="failed",
                        error_message=str(exc),
                    )
                    break

    async def ingest_user_tweets(
        self,
        run_id: str,
        project_keyword: str,
        user_ids: Iterable[str],
        key: ApiKeyState,
    ) -> None:
        for user_id in user_ids:
            cursor: str | None = None
            window_id = f"user_{user_id}"
            while True:
                try:
                    data = await self._client.user_tweets(
                        key_state=key, user_id=user_id, cursor=cursor
                    )
                    tweets = data.get("tweets", []) or []
                    for tweet in tweets:
                        if not isinstance(tweet, dict):
                            continue
                        await self._repo.upsert_post_raw(
                            run_id=run_id,
                            project_keyword=project_keyword,
                            endpoint="/user-tweets",
                            payload=tweet,
                        )
                        await self._repo.upsert_mindshare_post(
                            payload=tweet,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                    cursor = data.get("next_cursor")
                    await self._repo.upsert_window_checkpoint(
                        run_id=run_id,
                        project_keyword=project_keyword,
                        window_id=window_id,
                        endpoint="/user-tweets",
                        api_key_alias=key.alias,
                        next_cursor=cursor,
                        status="running" if cursor else "completed",
                    )
                    if not cursor:
                        break
                except Exception as exc:
                    await self._repo.upsert_window_checkpoint(
                        run_id=run_id,
                        project_keyword=project_keyword,
                        window_id=window_id,
                        endpoint="/user-tweets",
                        api_key_alias=key.alias,
                        next_cursor=cursor,
                        status="failed",
                        error_message=str(exc),
                    )
                    break

    async def ingest_scores(
        self,
        run_id: str,
        project_keyword: str,
        user_ids: Iterable[str],
        key: ApiKeyState,
    ) -> None:
        for user_id in user_ids:
            window_id = f"score_{user_id}"
            try:
                data = await self._client.score_id(key_state=key, x_id=user_id)
                await self._repo.upsert_user_score(
                    x_id=user_id,
                    username=data.get("username"),
                    display_name=data.get("display_name"),
                    avatar_url=data.get("profile_image_url"),
                    followers_count=data.get("followers_count"),
                    score=data.get("score"),
                )
                await self._repo.upsert_window_checkpoint(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    window_id=window_id,
                    endpoint="/score-id",
                    api_key_alias=key.alias,
                    next_cursor=None,
                    status="completed",
                )
            except Exception as exc:
                await self._repo.upsert_window_checkpoint(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    window_id=window_id,
                    endpoint="/score-id",
                    api_key_alias=key.alias,
                    next_cursor=None,
                    status="failed",
                    error_message=str(exc),
                )

