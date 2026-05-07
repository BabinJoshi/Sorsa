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
        description="SQLAlchemy async URL, e.g. postgresql+asyncpg://...",
        alias="COCKROACH_DATABASE_URL",
    )
    sorsa_base_url: str = Field(
        default="https://api.sorsa.io/v3",
        alias="SORSA_BASE_URL",
    )
    sorsa_api_key: str = Field(
        ...,
        description="Single Sorsa API key",
        alias="SORSA_API_KEY",
    )
    sorsa_per_key_rps: int = Field(default=20, alias="SORSA_PER_KEY_RPS")
    search_slice_count: int = Field(default=20, alias="SEARCH_SLICE_COUNT")
    search_max_concurrency: int = Field(
        default=20, alias="SEARCH_MAX_CONCURRENCY"
    )
    search_order: str = Field(default="latest", alias="SEARCH_ORDER")
    max_retries: int = Field(default=4, alias="SORSA_MAX_RETRIES")
    retry_429_sleep_seconds: float = Field(
        default=1.0, alias="SORSA_RETRY_429_SLEEP_SECONDS"
    )
    retry_5xx_sleep_seconds: float = Field(
        default=2.0, alias="SORSA_RETRY_5XX_SLEEP_SECONDS"
    )

    @property
    def api_keys(self) -> list[str]:
        return [self.sorsa_api_key.strip()]

