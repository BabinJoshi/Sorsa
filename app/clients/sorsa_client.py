from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class SorsaClientError(Exception):
    pass


class SorsaRateLimitError(SorsaClientError):
    pass


@dataclass
class ApiKeyState:
    alias: str
    api_key: str


class PerKeyRateLimiter:
    def __init__(self, rps: int) -> None:
        self._rps = max(1, rps)
        self._events: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()
        self._request_counts: dict[str, int] = {}

    async def acquire(self, alias: str) -> None:
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                q = self._events.setdefault(alias, deque())
                while q and now - q[0] >= 1.0:
                    q.popleft()
                if len(q) < self._rps:
                    q.append(now)
                    self._request_counts[alias] = self._request_counts.get(alias, 0) + 1
                    return
                sleep_for = max(0.01, 1.0 - (now - q[0]))
            logger.debug("[%s] RPS cap reached (%d/s), waiting %.2fs", alias, self._rps, sleep_for)
            await asyncio.sleep(sleep_for)

    def get_counts(self) -> dict[str, int]:
        """Return a snapshot of total requests dispatched per key alias."""
        return dict(self._request_counts)

    def log_counts(self, context: str = "") -> None:
        """Log current request counts for all key aliases."""
        counts = self.get_counts()
        if not counts:
            logger.info("%sRequest counts: no requests dispatched yet", f"{context} " if context else "")
            return
        total = sum(counts.values())
        per_key = ", ".join(f"{alias}={count}" for alias, count in sorted(counts.items()))
        logger.info("%sRequest counts — total=%d | %s", f"{context} " if context else "", total, per_key)


class SorsaClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int,
        max_retries: int,
        retry_429_sleep_seconds: float,
        retry_5xx_sleep_seconds: float,
        limiter: PerKeyRateLimiter,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._max_retries = max_retries
        self._retry_429_sleep_seconds = retry_429_sleep_seconds
        self._retry_5xx_sleep_seconds = retry_5xx_sleep_seconds
        self._limiter = limiter

    async def _request(
        self,
        method: str,
        path: str,
        key_state: ApiKeyState,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        headers = {"ApiKey": key_state.api_key, "Accept": "application/json"}
        if method == "POST":
            headers["Content-Type"] = "application/json"

        for attempt in range(1, self._max_retries + 1):
            await self._limiter.acquire(key_state.alias)
            logger.debug(
                "[%s] %s %s — attempt %d/%d",
                key_state.alias, method, path, attempt, self._max_retries,
            )
            try:
                async with aiohttp.ClientSession(timeout=self._timeout) as session:
                    async with session.request(
                        method, url, headers=headers, json=payload, params=params
                    ) as response:
                        try:
                            data = await response.json(content_type=None)
                        except (json.JSONDecodeError, aiohttp.ContentTypeError):
                            if attempt < self._max_retries:
                                logger.warning(
                                    "[%s] %s %s — empty/non-JSON body on attempt %d/%d; "
                                    "sleeping %.1fs before retry",
                                    key_state.alias, method, path, attempt, self._max_retries,
                                    self._retry_5xx_sleep_seconds,
                                )
                                await asyncio.sleep(self._retry_5xx_sleep_seconds)
                                continue
                            logger.error(
                                "[%s] %s %s — empty/non-JSON body on final attempt %d/%d",
                                key_state.alias, method, path, attempt, self._max_retries,
                            )
                            raise SorsaClientError("Non-JSON or empty response from API")

                        if 200 <= response.status < 300:
                            logger.debug(
                                "[%s] %s %s → %d OK",
                                key_state.alias, method, path, response.status,
                            )
                            return data if isinstance(data, dict) else {}

                        if response.status == 429 and attempt < self._max_retries:
                            logger.warning(
                                "[%s] %s %s → 429 rate-limited; sleeping %.1fs before retry %d/%d",
                                key_state.alias, method, path,
                                self._retry_429_sleep_seconds, attempt + 1, self._max_retries,
                            )
                            await asyncio.sleep(self._retry_429_sleep_seconds)
                            continue

                        if response.status >= 500 and attempt < self._max_retries:
                            logger.warning(
                                "[%s] %s %s → %d server error; sleeping %.1fs before retry %d/%d",
                                key_state.alias, method, path, response.status,
                                self._retry_5xx_sleep_seconds, attempt + 1, self._max_retries,
                            )
                            await asyncio.sleep(self._retry_5xx_sleep_seconds)
                            continue

                        if response.status == 429:
                            logger.error(
                                "[%s] %s %s → 429 rate-limited on final attempt %d/%d",
                                key_state.alias, method, path, attempt, self._max_retries,
                            )
                            raise SorsaRateLimitError(f"{response.status}: {data}")

                        logger.error(
                            "[%s] %s %s → %d non-retryable error: %s",
                            key_state.alias, method, path, response.status, data,
                        )
                        raise SorsaClientError(f"{response.status}: {data}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    logger.error(
                        "[%s] %s %s — network/timeout error on final attempt %d/%d: %s",
                        key_state.alias, method, path, attempt, self._max_retries, exc,
                    )
                    raise SorsaClientError(str(exc)) from exc
                logger.warning(
                    "[%s] %s %s — network/timeout error: %s; sleeping %.1fs before retry %d/%d",
                    key_state.alias, method, path, exc,
                    self._retry_5xx_sleep_seconds, attempt + 1, self._max_retries,
                )
                await asyncio.sleep(self._retry_5xx_sleep_seconds)

        raise SorsaClientError("request retries exhausted")

    async def search_tweets(
        self,
        key_state: ApiKeyState,
        query: str,
        since_iso: str,
        until_iso: str,
        cursor: str | None,
        order: str,
    ) -> dict[str, Any]:
        search_query = f'{query} since:{since_iso} until:{until_iso}'
        payload: dict[str, Any] = {"query": search_query, "order": order}
        if cursor:
            payload["next_cursor"] = cursor
        return await self._request("POST", "/search-tweets", key_state, payload)

    async def comments(
        self, key_state: ApiKeyState, tweet_link: str, cursor: str | None
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"tweet_link": tweet_link}
        if cursor:
            payload["next_cursor"] = cursor
        return await self._request("POST", "/comments", key_state, payload)

    async def user_tweets(
        self, key_state: ApiKeyState, user_id: str, cursor: str | None
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"user_id": user_id}
        if cursor:
            payload["next_cursor"] = cursor
        return await self._request("POST", "/user-tweets", key_state, payload)

    async def score_id(self, key_state: ApiKeyState, x_id: str) -> dict[str, Any]:
        return await self._request("GET", "/score", key_state, params={"user_id": x_id})


def to_sorsa_date(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%d_%H:%M:%S_UTC")
