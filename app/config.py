from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    cockroach_database_url: str = Field(
        ...,
        description="Database URL. Accepts postgresql+asyncpg:// or plain postgresql:// / postgres://",
        alias="COCKROACH_DATABASE_URL",
    )

    @property
    def asyncpg_dsn(self) -> str:
        """Return a DSN suitable for asyncpg (strips the +asyncpg driver tag if present)."""
        return (
            self.cockroach_database_url
            .replace("postgresql+asyncpg://", "postgresql://")
            .replace("postgres+asyncpg://", "postgres://")
        )
    sorsa_base_url: str = Field(
        default="https://api.sorsa.io/v3",
        alias="SORSA_BASE_URL",
    )
    sorsa_api_keys: str = Field(
        ...,
        description=(
            "One or more Sorsa API keys as a comma-separated string. "
            "Example: SORSA_API_KEYS=key1,key2,key3"
        ),
        alias="SORSA_API_KEYS",
    )
    sorsa_per_key_rps: int = Field(default=20, alias="SORSA_PER_KEY_RPS")
    search_slice_count: int = Field(default=20, alias="SEARCH_SLICE_COUNT")
    search_max_concurrency_override: int | None = Field(
        default=None,
        alias="SEARCH_MAX_CONCURRENCY",
    )
    search_order: str = Field(default="latest", alias="SEARCH_ORDER")
    aux_max_concurrency_override: int | None = Field(
        default=None,
        alias="AUX_MAX_CONCURRENCY",
    )
    max_retries: int = Field(default=4, alias="SORSA_MAX_RETRIES")
    retry_429_sleep_seconds: float = Field(
        default=1.0, alias="SORSA_RETRY_429_SLEEP_SECONDS"
    )
    retry_5xx_sleep_seconds: float = Field(
        default=2.0, alias="SORSA_RETRY_5XX_SLEEP_SECONDS"
    )
    db_write_batch_size: int = Field(
        default=1000,
        description=(
            "Number of records accumulated across API pages before a batch DB write is "
            "triggered. Writes also flush at the natural end of each time slice so no "
            "data is held in memory longer than one slice."
        ),
        alias="DB_WRITE_BATCH_SIZE",
    )
    skip_user_tweets: bool = Field(
        default=False,
        description="Set to true to skip Phase 3 (user timelines) entirely.",
        alias="SKIP_USER_TWEETS",
    )
    skip_comments: bool = Field(
        default=False,
        description="Set to true to skip Phase 2 (comments) entirely.",
        alias="SKIP_COMMENTS",
    )
    skip_scores: bool = Field(
        default=False,
        description="Set to true to skip Phase 4 (user scores) entirely.",
        alias="SKIP_SCORES",
    )

    @property
    def api_keys(self) -> list[str]:
        """Parse SORSA_API_KEYS into a list, preserving order."""
        keys = [k.strip() for k in self.sorsa_api_keys.split(",") if k.strip()]
        if not keys:
            raise ValueError("SORSA_API_KEYS must contain at least one API key")
        return keys

    @property
    def search_max_concurrency(self) -> int:
        """Max concurrent search slices.
        Uses SEARCH_MAX_CONCURRENCY if set, otherwise auto = len(keys) * per_key_rps."""
        return self.search_max_concurrency_override or (len(self.api_keys) * self.sorsa_per_key_rps)

    @property
    def aux_max_concurrency(self) -> int:
        """Max concurrent requests for phases 2–4.
        Uses AUX_MAX_CONCURRENCY if set, otherwise auto = len(keys) * per_key_rps."""
        return self.aux_max_concurrency_override or (len(self.api_keys) * self.sorsa_per_key_rps)

