from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp


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

    async def acquire(self, alias: str) -> None:
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                q = self._events.setdefault(alias, deque())
                while q and now - q[0] >= 1.0:
                    q.popleft()
                if len(q) < self._rps:
                    q.append(now)
                    return
                sleep_for = max(0.01, 1.0 - (now - q[0]))
            await asyncio.sleep(sleep_for)


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
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        headers = {"ApiKey": key_state.api_key, "Accept": "application/json"}
        if method == "POST":
            headers["Content-Type"] = "application/json"

        for attempt in range(1, self._max_retries + 1):
            await self._limiter.acquire(key_state.alias)
            try:
                async with aiohttp.ClientSession(timeout=self._timeout) as session:
                    async with session.request(
                        method, url, headers=headers, json=payload
                    ) as response:
                        data = await response.json(content_type=None)
                        if 200 <= response.status < 300:
                            return data if isinstance(data, dict) else {}

                        if response.status == 429 and attempt < self._max_retries:
                            await asyncio.sleep(self._retry_429_sleep_seconds)
                            continue
                        if response.status >= 500 and attempt < self._max_retries:
                            await asyncio.sleep(self._retry_5xx_sleep_seconds)
                            continue
                        if response.status == 429:
                            raise SorsaRateLimitError(f"{response.status}: {data}")
                        raise SorsaClientError(f"{response.status}: {data}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise SorsaClientError(str(exc)) from exc
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
        return await self._request("GET", f"/score-id/{x_id}", key_state)


def to_sorsa_date(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%d_%H:%M:%S_UTC")

