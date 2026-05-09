from __future__ import annotations

import asyncpg

from app.config import Settings


async def build_pool(settings: Settings) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool.

    max_inactive_connection_lifetime recycles idle connections before
    CockroachDB's session timeout closes them, preventing
    'underlying connection is closed' errors during long runs.
    """
    pool = await asyncpg.create_pool(
        settings.asyncpg_dsn,
        min_size=2,
        max_size=20,
        max_inactive_connection_lifetime=300.0,  # recycle after 5 min idle
    )
    assert pool is not None
    return pool
