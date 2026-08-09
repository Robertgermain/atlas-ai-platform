"""Application configuration loaded from the environment."""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for Atlas infrastructure."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ATLAS_",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql+psycopg://atlas:atlas@127.0.0.1:5433/atlas",
    )
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0)
    # Orchestration timeout for Future.result (does not kill processor threads).
    worker_processing_timeout_seconds: float = Field(default=60.0, gt=0)
    worker_lease_seconds: float = Field(default=90.0, gt=0)

    model_provider: Literal["fake", "openai", "anthropic"] = Field(default="fake")
    model_name: str | None = Field(default=None)
    # Provider HTTP/SDK request timeout and ledger attempt deadline_at duration.
    # Not a hard wall-clock around the entire structured-invoke + ledger path.
    model_call_timeout_seconds: float = Field(default=25.0, gt=0)
    plan_prompt_version: str = Field(default="plan.v1")
    draft_prompt_version: str = Field(default="draft.v1")
    openai_api_key: SecretStr | None = Field(default=None)
    anthropic_api_key: SecretStr | None = Field(default=None)


def get_settings() -> Settings:
    """Return settings from the current environment."""
    return Settings()
