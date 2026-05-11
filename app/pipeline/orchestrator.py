from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import asyncpg

from app.clients.sorsa_client import ApiKeyState, PerKeyRateLimiter, SorsaClient
from app.config import Settings
from app.db.repository import IngestionRepository
from app.pipeline.aux_ingestors import AuxiliaryIngestors
from app.pipeline.search_ingestor import SearchIngestor

logger = logging.getLogger(__name__)


class IngestionOrchestrator:
    def __init__(self, settings: Settings, pool: asyncpg.Pool) -> None:
        self._settings = settings
        self._pool = pool
        self._repo = IngestionRepository(pool)

        self._limiter = PerKeyRateLimiter(settings.sorsa_per_key_rps)
        self._client = SorsaClient(
            base_url=settings.sorsa_base_url,
            timeout_seconds=60,
            max_retries=settings.max_retries,
            retry_429_sleep_seconds=settings.retry_429_sleep_seconds,
            retry_5xx_sleep_seconds=settings.retry_5xx_sleep_seconds,
            limiter=self._limiter,
        )
        self._search_ingestor = SearchIngestor(self._repo, self._client)
        self._aux_ingestors = AuxiliaryIngestors(self._repo, self._client)

    def _api_keys(self) -> list[ApiKeyState]:
        return [
            ApiKeyState(alias=f"key_{idx+1}", api_key=value)
            for idx, value in enumerate(self._settings.api_keys)
        ]

    async def run_project_ingestion(
        self,
        project_keyword: str,
        hours: int = 72,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> tuple[str, dict[str, float]]:
        # Split on commas to get individual search terms.
        # project_label = first term, used as the DB identifier.
        # search_query  = Twitter OR syntax: single words unquoted, multi-word phrases quoted.
        #   e.g. "quipnetwork, Quip Network, quip_network"
        #        → quipnetwork OR "Quip Network" OR quip_network
        terms = [t.strip() for t in project_keyword.split(",") if t.strip()]
        project_label = terms[0] if terms else project_keyword
        search_query = " OR ".join(
            f'"{t}"' if " " in t else t for t in terms
        )

        # Resolve the search window.
        # --since / --until take priority; fall back to --hours from now.
        effective_until = until or datetime.now(timezone.utc)
        effective_since = since or (effective_until - timedelta(hours=hours))

        logger.info(
            "Starting ingestion — label=%r search_query=%r window=%s → %s",
            project_label,
            search_query,
            effective_since.strftime("%Y-%m-%d %H:%M UTC"),
            effective_until.strftime("%Y-%m-%d %H:%M UTC"),
        )

        run_id = await self._repo.create_run(
            project_keyword=project_label,
            since=effective_since,
            until=effective_until,
        )
        logger.info("Run created — run_id=%s", run_id)

        all_keys = self._api_keys()
        aux_concurrency = self._settings.aux_max_concurrency

        def _elapsed_secs(t0: datetime) -> float:
            return (datetime.now(timezone.utc) - t0).total_seconds()

        def _fmt(secs: float) -> str:
            return f"{secs / 60:.1f}min ({int(secs // 60)}m {int(secs % 60)}s)"

        phase_timings: dict[str, float] = {}

        try:
            logger.info("Phase 1 — search (/search-tweets) starting")
            t0_phase1 = datetime.now(timezone.utc)
            post_ids, user_ids = await self._search_ingestor.ingest_window(
                run_id=run_id,
                project_keyword=project_label,
                search_query=search_query,
                keys=all_keys,
                order=self._settings.search_order,
                slice_count=self._settings.search_slice_count,
                max_concurrency=self._settings.search_max_concurrency,
                batch_size=self._settings.db_write_batch_size,
                since=effective_since,
                until=effective_until,
            )
            phase_timings["Phase 1 (search)"] = _elapsed_secs(t0_phase1)
            logger.info(
                "Phase 1 complete — posts_found=%d users_found=%d | elapsed=%s",
                len(post_ids),
                len(user_ids),
                _fmt(phase_timings["Phase 1 (search)"]),
            )
            self._limiter.log_counts("After phase 1:")

            if all_keys and (post_ids or user_ids):
                skip_comments    = self._settings.skip_comments
                skip_user_tweets = self._settings.skip_user_tweets
                skip_scores      = self._settings.skip_scores

                skipped = [
                    label for flag, label in [
                        (skip_comments,    "comments"),
                        (skip_user_tweets, "user-tweets"),
                        (skip_scores,      "scores"),
                    ] if flag
                ]
                if skipped:
                    logger.info("Skipping phase(s): %s", ", ".join(skipped).upper())

                logger.info(
                    "Phases 2+3+4 — starting concurrently "
                    "(posts=%d users=%d concurrency=%d%s)",
                    len(post_ids), len(user_ids), aux_concurrency,
                    f" | skipped: {', '.join(skipped)}" if skipped else "",
                )

                # Build (phase_label, coroutine) pairs so we can map results back to names.
                aux_pairs: list[tuple[str, object]] = []
                if not skip_comments:
                    aux_pairs.append((
                        "Phase 2 (comments)",
                        self._aux_ingestors.ingest_comments_for_posts(
                            run_id=run_id,
                            project_keyword=project_label,
                            posts=post_ids,
                            keys=all_keys,
                            max_concurrency=aux_concurrency,
                            filter_terms=terms,
                        ),
                    ))
                if not skip_user_tweets:
                    aux_pairs.append((
                        "Phase 3 (user-tweets)",
                        self._aux_ingestors.ingest_user_tweets(
                            run_id=run_id,
                            project_keyword=project_label,
                            user_ids=user_ids,
                            keys=all_keys,
                            max_concurrency=aux_concurrency,
                            filter_terms=terms,
                        ),
                    ))
                if not skip_scores:
                    aux_pairs.append((
                        "Phase 4 (scores)",
                        self._aux_ingestors.ingest_scores(
                            run_id=run_id,
                            project_keyword=project_label,
                            user_ids=user_ids,
                            keys=all_keys,
                            max_concurrency=aux_concurrency,
                        ),
                    ))

                if aux_pairs:
                    t0_aux = datetime.now(timezone.utc)
                    elapsed_results = await asyncio.gather(*(coro for _, coro in aux_pairs))
                    for (label, _), secs in zip(aux_pairs, elapsed_results):
                        phase_timings[label] = secs
                    phase_timings["Phases 2+3+4 (combined)"] = _elapsed_secs(t0_aux)
                    logger.info(
                        "Phases 2+3+4 complete — elapsed=%s",
                        _fmt(phase_timings["Phases 2+3+4 (combined)"]),
                    )
                self._limiter.log_counts("After phases 2+3+4:")

            await self._repo.mark_run_finished(run_id, "completed")
            self._limiter.log_counts("Final totals:")
            logger.info("Ingestion completed — run_id=%s", run_id)
            return run_id, phase_timings
        except Exception as exc:
            await self._repo.mark_run_finished(run_id, "failed", str(exc))
            self._limiter.log_counts("Counts at failure:")
            logger.error("Ingestion failed — run_id=%s error=%s", run_id, exc)
            raise

    async def close(self) -> None:
        await self._pool.close()
