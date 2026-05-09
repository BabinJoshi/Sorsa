from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from app.clients.sorsa_client import ApiKeyState, SorsaClient
from app.db.repository import IngestionRepository

logger = logging.getLogger(__name__)


class AuxiliaryIngestors:
    def __init__(self, repo: IngestionRepository, sorsa_client: SorsaClient) -> None:
        self._repo = repo
        self._client = sorsa_client

    # ------------------------------------------------------------------ #
    # Phase 2 — comments                                                  #
    # ------------------------------------------------------------------ #

    async def ingest_comments_for_posts(
        self,
        run_id: str,
        project_keyword: str,
        posts: Iterable[str],
        keys: list[ApiKeyState],
        max_concurrency: int,
    ) -> None:
        post_list = list(posts)
        logger.info(
            "[comments] Starting — %d post(s) | keys=%d | concurrency=%d",
            len(post_list), len(keys), max_concurrency,
        )
        sem = asyncio.Semaphore(max_concurrency)

        async def _fetch_post(idx: int, post_id: str) -> tuple[int, bool]:
            key = keys[idx % len(keys)]
            async with sem:
                cursor: str | None = None
                page_num = 0
                count = 0
                logger.info(
                    "[comments] (%d/%d) post_id=%s key=%s",
                    idx + 1, len(post_list), post_id, key.alias,
                )
                while True:
                    try:
                        data = await self._client.comments(
                            key_state=key, tweet_link=post_id, cursor=cursor
                        )
                        raw = data.get("tweets", []) or []
                        tweets = [t for t in raw if isinstance(t, dict)]
                        page_num += 1
                        await self._repo.upsert_mindshare_posts_batch(
                            payloads=tweets,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                        count += len(tweets)
                        cursor = data.get("next_cursor")
                        logger.debug(
                            "[comments] post_id=%s page %d — %d comments | has_next=%s",
                            post_id, page_num, len(tweets), bool(cursor),
                        )
                        if not cursor:
                            logger.info(
                                "[comments] post_id=%s done — %d comment(s) across %d page(s)",
                                post_id, count, page_num,
                            )
                            return count, True
                    except Exception as exc:
                        logger.warning(
                            "[comments] post_id=%s failed on page %d after %d comment(s): %s",
                            post_id, page_num + 1, count, exc,
                        )
                        return count, False

        results = await asyncio.gather(
            *[_fetch_post(i, pid) for i, pid in enumerate(post_list)]
        )
        total = sum(c for c, _ in results)
        failed = sum(1 for _, ok in results if not ok)
        logger.info(
            "[comments] Phase complete — %d comment(s) ingested | %d post(s) ok, %d failed",
            total, len(post_list) - failed, failed,
        )

    # ------------------------------------------------------------------ #
    # Phase 3 — user timelines                                            #
    # ------------------------------------------------------------------ #

    async def ingest_user_tweets(
        self,
        run_id: str,
        project_keyword: str,
        user_ids: Iterable[str],
        keys: list[ApiKeyState],
        max_concurrency: int,
    ) -> None:
        user_list = list(user_ids)
        logger.info(
            "[user-tweets] Starting — %d user(s) | keys=%d | concurrency=%d",
            len(user_list), len(keys), max_concurrency,
        )
        sem = asyncio.Semaphore(max_concurrency)

        async def _fetch_user(idx: int, user_id: str) -> tuple[int, bool]:
            key = keys[idx % len(keys)]
            async with sem:
                cursor: str | None = None
                page_num = 0
                count = 0
                logger.info(
                    "[user-tweets] (%d/%d) user_id=%s key=%s",
                    idx + 1, len(user_list), user_id, key.alias,
                )
                while True:
                    try:
                        data = await self._client.user_tweets(
                            key_state=key, user_id=user_id, cursor=cursor
                        )
                        raw = data.get("tweets", []) or []
                        tweets = [t for t in raw if isinstance(t, dict)]
                        page_num += 1
                        await self._repo.upsert_mindshare_posts_batch(
                            payloads=tweets,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                        count += len(tweets)
                        cursor = data.get("next_cursor")
                        logger.debug(
                            "[user-tweets] user_id=%s page %d — %d tweets | has_next=%s",
                            user_id, page_num, len(tweets), bool(cursor),
                        )
                        if not cursor:
                            logger.info(
                                "[user-tweets] user_id=%s done — %d tweet(s) across %d page(s)",
                                user_id, count, page_num,
                            )
                            return count, True
                    except Exception as exc:
                        logger.warning(
                            "[user-tweets] user_id=%s failed on page %d after %d tweet(s): %s",
                            user_id, page_num + 1, count, exc,
                        )
                        return count, False

        results = await asyncio.gather(
            *[_fetch_user(i, uid) for i, uid in enumerate(user_list)]
        )
        total = sum(c for c, _ in results)
        failed = sum(1 for _, ok in results if not ok)
        logger.info(
            "[user-tweets] Phase complete — %d tweet(s) ingested | %d user(s) ok, %d failed",
            total, len(user_list) - failed, failed,
        )

    # ------------------------------------------------------------------ #
    # Phase 4 — user scores                                               #
    # ------------------------------------------------------------------ #

    async def ingest_scores(
        self,
        run_id: str,
        project_keyword: str,
        user_ids: Iterable[str],
        keys: list[ApiKeyState],
        max_concurrency: int,
    ) -> None:
        user_list = list(user_ids)
        logger.info(
            "[scores] Starting — %d user(s) | keys=%d | concurrency=%d",
            len(user_list), len(keys), max_concurrency,
        )
        sem = asyncio.Semaphore(max_concurrency)

        async def _score_user(idx: int, user_id: str) -> bool:
            key = keys[idx % len(keys)]
            async with sem:
                try:
                    data = await self._client.score_id(key_state=key, x_id=user_id)
                    score = data.get("score")
                    username = data.get("username") or ""
                    await self._repo.upsert_user_score(
                        x_id=user_id,
                        username=username,
                        display_name=data.get("display_name"),
                        avatar_url=data.get("profile_image_url"),
                        followers_count=data.get("followers_count"),
                        score=score,
                    )
                    logger.info(
                        "[scores] (%d/%d) user_id=%s username=%s score=%s",
                        idx + 1, len(user_list), user_id, username or "—", score,
                    )
                    return True
                except Exception as exc:
                    logger.warning(
                        "[scores] (%d/%d) user_id=%s failed: %s",
                        idx + 1, len(user_list), user_id, exc,
                    )
                    return False

        results = await asyncio.gather(
            *[_score_user(i, uid) for i, uid in enumerate(user_list)]
        )
        succeeded = sum(1 for ok in results if ok)
        failed = len(results) - succeeded
        logger.info(
            "[scores] Phase complete — %d scored, %d failed out of %d user(s)",
            succeeded, failed, len(user_list),
        )
