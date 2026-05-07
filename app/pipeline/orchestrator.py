from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.clients.sorsa_client import ApiKeyState, PerKeyRateLimiter, SorsaClient
from app.config import Settings
from app.db.connection import build_engine, build_session_maker
from app.db.repository import IngestionRepository
from app.pipeline.aux_ingestors import AuxiliaryIngestors
from app.pipeline.search_ingestor import SearchIngestor


class IngestionOrchestrator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine = build_engine(settings)
        self._session_maker = build_session_maker(self._engine)
        self._repo = IngestionRepository(self._session_maker)

        limiter = PerKeyRateLimiter(settings.sorsa_per_key_rps)
        self._client = SorsaClient(
            base_url=settings.sorsa_base_url,
            timeout_seconds=60,
            max_retries=settings.max_retries,
            retry_429_sleep_seconds=settings.retry_429_sleep_seconds,
            retry_5xx_sleep_seconds=settings.retry_5xx_sleep_seconds,
            limiter=limiter,
        )
        self._search_ingestor = SearchIngestor(self._repo, self._client)
        self._aux_ingestors = AuxiliaryIngestors(self._repo, self._client)

    def _api_keys(self) -> list[ApiKeyState]:
        return [
            ApiKeyState(alias=f"key_{idx+1}", api_key=value)
            for idx, value in enumerate(self._settings.api_keys)
        ]

    async def run_project_ingestion(self, project_keyword: str) -> str:
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=72)
        run_id = await self._repo.create_run(
            project_keyword=project_keyword,
            since=since,
            until=now,
        )
        try:
            post_ids, user_ids = await self._search_ingestor.ingest_72h(
                run_id=run_id,
                project_keyword=project_keyword,
                keys=self._api_keys(),
                order=self._settings.search_order,
                slice_count=self._settings.search_slice_count,
                max_concurrency=self._settings.search_max_concurrency,
                per_key_rps=self._settings.sorsa_per_key_rps,
            )
            if self._api_keys():
                first_key = self._api_keys()[0]
                await self._aux_ingestors.ingest_comments_for_posts(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    posts=post_ids,
                    key=first_key,
                )
                await self._aux_ingestors.ingest_user_tweets(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    user_ids=user_ids,
                    key=first_key,
                )
                await self._aux_ingestors.ingest_scores(
                    run_id=run_id,
                    project_keyword=project_keyword,
                    user_ids=user_ids,
                    key=first_key,
                )
            await self._repo.mark_run_finished(run_id, "completed")
            return run_id
        except Exception as exc:
            await self._repo.mark_run_finished(run_id, "failed", str(exc))
            raise

    async def close(self) -> None:
        await self._engine.dispose()

