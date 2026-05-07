from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from app.clients.sorsa_client import ApiKeyState, SorsaClient, to_sorsa_date
from app.db.repository import IngestionRepository
from app.models import TimeSlice


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

    async def ingest_72h(
        self,
        run_id: str,
        project_keyword: str,
        keys: list[ApiKeyState],
        order: str,
        slice_count: int,
        max_concurrency: int,
        per_key_rps: int,
    ) -> tuple[set[str], set[str]]:
        until = datetime.now(timezone.utc)
        since = until - timedelta(hours=72)
        slices = build_time_slices(since, until, slice_count, keys)
        key_map = {k.alias: k for k in keys}
        all_post_ids: set[str] = set()
        all_user_ids: set[str] = set()
        effective_concurrency = max(
            1,
            min(
                slice_count,
                max_concurrency,
                per_key_rps,
            ),
        )
        sem = asyncio.Semaphore(effective_concurrency)

        async def run_slice(slice_: TimeSlice) -> tuple[set[str], set[str]]:
            async with sem:
                return await self._ingest_slice(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    key_state=key_map[slice_.api_key_alias],
                    slice_=slice_,
                    order=order,
                )

        tasks = [
            run_slice(slice_)
            for slice_ in slices
        ]
        results = await asyncio.gather(*tasks)
        for post_ids, user_ids in results:
            all_post_ids.update(post_ids)
            all_user_ids.update(user_ids)
        return all_post_ids, all_user_ids

    async def _ingest_slice(
        self,
        run_id: str,
        project_keyword: str,
        key_state: ApiKeyState,
        slice_: TimeSlice,
        order: str,
    ) -> tuple[set[str], set[str]]:
        cursor: str | None = None
        window_id = f"slice_{slice_.slice_id}"
        post_ids: set[str] = set()
        user_ids: set[str] = set()
        while True:
            try:
                data = await self._client.search_tweets(
                    key_state=key_state,
                    query=project_keyword,
                    since_iso=to_sorsa_date(slice_.since),
                    until_iso=to_sorsa_date(slice_.until),
                    cursor=cursor,
                    order=order,
                )
                tweets = data.get("tweets", []) or []
                for tweet in tweets:
                    if not isinstance(tweet, dict):
                        continue
                    post_id = str(tweet.get("id", "")).strip()
                    user_id = str((tweet.get("user") or {}).get("id", "")).strip()
                    if post_id:
                        post_ids.add(post_id)
                    if user_id:
                        user_ids.add(user_id)
                    await self._repo.upsert_post_raw(
                        run_id=run_id,
                        project_keyword=project_keyword,
                        endpoint="/search-tweets",
                        payload=tweet,
                    )
                    await self._repo.upsert_mindshare_post(
                        payload=tweet,
                        project_keyword=project_keyword,
                        run_id=run_id,
                    )

                cursor = data.get("next_cursor")
                status = "running" if cursor else "completed"
                await self._repo.upsert_window_checkpoint(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    window_id=window_id,
                    endpoint="/search-tweets",
                    api_key_alias=key_state.alias,
                    next_cursor=cursor,
                    status=status,
                )
                if not cursor:
                    return post_ids, user_ids
            except Exception as exc:
                await self._repo.upsert_window_checkpoint(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    window_id=window_id,
                    endpoint="/search-tweets",
                    api_key_alias=key_state.alias,
                    next_cursor=cursor,
                    status="failed",
                    error_message=str(exc),
                )
                raise

