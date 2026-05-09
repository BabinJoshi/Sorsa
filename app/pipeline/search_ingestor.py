from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone  # timedelta used by build_time_slices
from typing import Any

from app.clients.sorsa_client import ApiKeyState, SorsaClient, to_sorsa_date
from app.db.repository import IngestionRepository
from app.models import TimeSlice

logger = logging.getLogger(__name__)


def build_time_slices(
    since: datetime, until: datetime, slice_count: int, keys: list[ApiKeyState]
) -> list[TimeSlice]:
    total_seconds = (until - since).total_seconds()
    slice_count = max(1, slice_count)
    step = total_seconds / slice_count
    slices: list[TimeSlice] = []
    for i in range(slice_count):
        start = since + timedelta(seconds=i * step)
        end = since + timedelta(seconds=(i + 1) * step) if i < slice_count - 1 else until
        key_alias = keys[i % len(keys)].alias
        slices.append(TimeSlice(slice_id=i + 1, since=start, until=end, api_key_alias=key_alias))
    return slices


class SearchIngestor:
    def __init__(
        self,
        repo: IngestionRepository,
        sorsa_client: SorsaClient,
    ) -> None:
        self._repo = repo
        self._client = sorsa_client

    async def ingest_window(
        self,
        run_id: str,
        project_keyword: str,
        search_query: str,
        keys: list[ApiKeyState],
        order: str,
        slice_count: int,
        max_concurrency: int,
        batch_size: int,
        since: datetime,
        until: datetime,
    ) -> tuple[set[str], set[str]]:
        slices = build_time_slices(since, until, slice_count, keys)
        key_map = {k.alias: k for k in keys}
        all_post_ids: set[str] = set()
        all_user_ids: set[str] = set()
        # max_concurrency already accounts for all keys (len(keys) * per_key_rps),
        # so per_key_rps is not used here — it would incorrectly cap multi-key capacity.
        effective_concurrency = max(1, min(slice_count, max_concurrency))
        sem = asyncio.Semaphore(effective_concurrency)

        logger.info(
            "Search ingestion starting — label=%r query=%r slices=%d "
            "effective_concurrency=%d batch_size=%d window=%s → %s",
            project_keyword,
            search_query,
            slice_count,
            effective_concurrency,
            batch_size,
            since.strftime("%Y-%m-%d %H:%M UTC"),
            until.strftime("%Y-%m-%d %H:%M UTC"),
        )

        async def run_slice(slice_: TimeSlice) -> tuple[set[str], set[str]]:
            async with sem:
                return await self._ingest_slice(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    search_query=search_query,
                    key_state=key_map[slice_.api_key_alias],
                    slice_=slice_,
                    order=order,
                    batch_size=batch_size,
                )

        tasks = [run_slice(slice_) for slice_ in slices]
        results = await asyncio.gather(*tasks)
        for post_ids, user_ids in results:
            all_post_ids.update(post_ids)
            all_user_ids.update(user_ids)

        logger.info(
            "Search ingestion complete — total_posts=%d total_users=%d across %d slices",
            len(all_post_ids),
            len(all_user_ids),
            slice_count,
        )
        return all_post_ids, all_user_ids

    async def _write_batch(
        self,
        batch: list[dict[str, Any]],
        run_id: str,
        project_keyword: str,
        slice_id: int,
        batch_num: int,
        total_written: int,
    ) -> int:
        """Write one batch to the normalized posts table, return updated total_written."""
        await self._repo.upsert_mindshare_posts_batch(
            payloads=batch,
            project_keyword=project_keyword,
            run_id=run_id,
        )
        total_written += len(batch)
        logger.info(
            "[slice %d] Batch %d written — %d records (slice total: %d)",
            slice_id,
            batch_num,
            len(batch),
            total_written,
        )
        return total_written

    async def _ingest_slice(
        self,
        run_id: str,
        project_keyword: str,
        search_query: str,
        key_state: ApiKeyState,
        slice_: TimeSlice,
        order: str,
        batch_size: int,
    ) -> tuple[set[str], set[str]]:
        cursor: str | None = None
        post_ids: set[str] = set()
        user_ids: set[str] = set()
        page_num = 0
        batch_num = 0
        total_written = 0

        # Accumulates tweets across pages; flushed in batches of batch_size.
        # Whatever remains after the last page is written as the final batch.
        buffer: list[dict[str, Any]] = []

        logger.info(
            "[slice %d] Starting — key=%s window=%s → %s",
            slice_.slice_id,
            key_state.alias,
            slice_.since.strftime("%Y-%m-%d %H:%M UTC"),
            slice_.until.strftime("%Y-%m-%d %H:%M UTC"),
        )

        try:
            while True:
                data = await self._client.search_tweets(
                    key_state=key_state,
                    query=search_query,
                    since_iso=to_sorsa_date(slice_.since),
                    until_iso=to_sorsa_date(slice_.until),
                    cursor=cursor,
                    order=order,
                )
                raw_tweets = data.get("tweets", []) or []
                tweets = [t for t in raw_tweets if isinstance(t, dict)]
                page_num += 1
                page_post_ids: list[str] = []
                page_user_ids: list[str] = []

                for tweet in tweets:
                    post_id = str(tweet.get("id", "")).strip()
                    user_id = str((tweet.get("user") or {}).get("id", "")).strip()
                    if post_id:
                        post_ids.add(post_id)
                        page_post_ids.append(post_id)
                    if user_id:
                        user_ids.add(user_id)
                        page_user_ids.append(user_id)

                buffer.extend(tweets)
                cursor = data.get("next_cursor")

                logger.info(
                    "[slice %d] Page %d — %d tweets (%d posts, %d users) "
                    "| buffer=%d/%d | has_next=%s",
                    slice_.slice_id,
                    page_num,
                    len(tweets),
                    len(page_post_ids),
                    len(page_user_ids),
                    len(buffer),
                    batch_size,
                    bool(cursor),
                )

                # Write every full batch as soon as it is ready.
                # Partial buffer (< batch_size) stays in memory until the next page fills it.
                while len(buffer) >= batch_size:
                    batch_num += 1
                    total_written = await self._write_batch(
                        batch=buffer[:batch_size],
                        run_id=run_id,
                        project_keyword=project_keyword,
                        slice_id=slice_.slice_id,
                        batch_num=batch_num,
                        total_written=total_written,
                    )
                    buffer = buffer[batch_size:]

                if not cursor:
                    break

        except Exception as exc:
            logger.error(
                "[slice %d] Failed on page %d — buffer=%d unflushed, already written=%d: %s",
                slice_.slice_id,
                page_num + 1,
                len(buffer),
                total_written,
                exc,
            )
            raise

        # Write the final partial batch (< batch_size records remaining after all pages)
        if buffer:
            batch_num += 1
            total_written = await self._write_batch(
                batch=buffer,
                run_id=run_id,
                project_keyword=project_keyword,
                slice_id=slice_.slice_id,
                batch_num=batch_num,
                total_written=total_written,
            )

        logger.info(
            "[slice %d] Done — %d posts, %d users | %d page(s) | %d batch(es) | total written=%d",
            slice_.slice_id,
            len(post_ids),
            len(user_ids),
            page_num,
            batch_num,
            total_written,
        )
        return post_ids, user_ids
