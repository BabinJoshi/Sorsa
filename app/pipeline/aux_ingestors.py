from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable

from app.clients.sorsa_client import ApiKeyState, SorsaClient
from app.db.repository import IngestionRepository

logger = logging.getLogger(__name__)


def _matches_any_term(full_text: str, terms: list[str]) -> bool:
    """Return True if full_text contains at least one of the search terms (case-insensitive)."""
    text_lower = full_text.lower()
    return any(t.lower() in text_lower for t in terms)


def _fmt_elapsed(t0: datetime) -> str:
    secs = (datetime.now(timezone.utc) - t0).total_seconds()
    return f"{secs / 60:.1f}min ({int(secs // 60)}m {int(secs % 60)}s)"


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
        filter_terms: list[str] | None = None,
    ) -> None:
        post_list = list(posts)
        t0 = datetime.now(timezone.utc)
        logger.info(
            "[comments] Starting — %d post(s) | keys=%d | concurrency=%d | filter_terms=%s",
            len(post_list), len(keys), max_concurrency,
            filter_terms or "none",
        )
        sem = asyncio.Semaphore(max_concurrency)

        async def _fetch_post(idx: int, post_id: str) -> tuple[int, bool]:
            key = keys[idx % len(keys)]
            async with sem:
                cursor: str | None = None
                page_num = 0
                count = 0
                filtered_out = 0
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

                        if filter_terms:
                            matched = [
                                t for t in tweets
                                if _matches_any_term(t.get("full_text", ""), filter_terms)
                            ]
                            filtered_out += len(tweets) - len(matched)
                            tweets = matched

                        await self._repo.upsert_mindshare_posts_batch(
                            payloads=tweets,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                        count += len(tweets)
                        cursor = data.get("next_cursor")
                        logger.debug(
                            "[comments] post_id=%s page %d — %d comments (filtered=%d) | has_next=%s",
                            post_id, page_num, len(tweets), filtered_out, bool(cursor),
                        )
                        if not cursor:
                            logger.info(
                                "[comments] post_id=%s done — %d comment(s) across %d page(s) (%d filtered)",
                                post_id, count, page_num, filtered_out,
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
        failed_ids = [pid for (_, ok), pid in zip(results, post_list) if not ok]

        if failed_ids:
            logger.warning(
                "[comments] %d post(s) failed — retrying once: %s",
                len(failed_ids), failed_ids,
            )
            retry_results = await asyncio.gather(
                *[_fetch_post(i, pid) for i, pid in enumerate(failed_ids)]
            )
            total += sum(c for c, _ in retry_results)
            failed_ids = [pid for (_, ok), pid in zip(retry_results, failed_ids) if not ok]
            if failed_ids:
                logger.error(
                    "[comments] %d post(s) still failed after retry: %s",
                    len(failed_ids), failed_ids,
                )

        failed = len(failed_ids)
        elapsed_secs = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info(
            "[comments] Phase complete — %d comment(s) ingested | %d post(s) ok, %d permanently failed | elapsed=%s",
            total, len(post_list) - failed, failed, _fmt_elapsed(t0),
        )
        return elapsed_secs

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
        filter_terms: list[str] | None = None,
    ) -> None:
        user_list = list(user_ids)
        t0 = datetime.now(timezone.utc)
        logger.info(
            "[user-tweets] Starting — %d user(s) | keys=%d | concurrency=%d | filter_terms=%s",
            len(user_list), len(keys), max_concurrency,
            filter_terms or "none",
        )
        sem = asyncio.Semaphore(max_concurrency)

        async def _fetch_user(idx: int, user_id: str) -> tuple[int, bool]:
            key = keys[idx % len(keys)]
            async with sem:
                cursor: str | None = None
                page_num = 0
                count = 0
                filtered_out = 0
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

                        if filter_terms:
                            matched = [
                                t for t in tweets
                                if _matches_any_term(t.get("full_text", ""), filter_terms)
                            ]
                            filtered_out += len(tweets) - len(matched)
                            tweets = matched

                        await self._repo.upsert_mindshare_posts_batch(
                            payloads=tweets,
                            project_keyword=project_keyword,
                            run_id=run_id,
                        )
                        count += len(tweets)
                        cursor = data.get("next_cursor")
                        logger.debug(
                            "[user-tweets] user_id=%s page %d — %d tweets (filtered=%d) | has_next=%s",
                            user_id, page_num, len(tweets), filtered_out, bool(cursor),
                        )
                        if not cursor:
                            logger.info(
                                "[user-tweets] user_id=%s done — %d tweet(s) across %d page(s) (%d filtered)",
                                user_id, count, page_num, filtered_out,
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
        failed_ids = [uid for (_, ok), uid in zip(results, user_list) if not ok]

        if failed_ids:
            logger.warning(
                "[user-tweets] %d user(s) failed — retrying once: %s",
                len(failed_ids), failed_ids,
            )
            retry_results = await asyncio.gather(
                *[_fetch_user(i, uid) for i, uid in enumerate(failed_ids)]
            )
            total += sum(c for c, _ in retry_results)
            failed_ids = [uid for (_, ok), uid in zip(retry_results, failed_ids) if not ok]
            if failed_ids:
                logger.error(
                    "[user-tweets] %d user(s) still failed after retry: %s",
                    len(failed_ids), failed_ids,
                )

        failed = len(failed_ids)
        elapsed_secs = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info(
            "[user-tweets] Phase complete — %d tweet(s) ingested | %d user(s) ok, %d permanently failed | elapsed=%s",
            total, len(user_list) - failed, failed, _fmt_elapsed(t0),
        )
        return elapsed_secs

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
        t0 = datetime.now(timezone.utc)
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
        elapsed_secs = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info(
            "[scores] Phase complete — %d scored, %d failed out of %d user(s) | elapsed=%s",
            succeeded, failed, len(user_list), _fmt_elapsed(t0),
        )
        return elapsed_secs
