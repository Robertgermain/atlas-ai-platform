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
    draft_prompt_version: str = Field(default="draft.v2")
    openai_api_key: SecretStr | None = Field(default=None)
    anthropic_api_key: SecretStr | None = Field(default=None)

    # Governed research tools (Milestone 9). Defaults keep CI offline.
    tool_provider: Literal["fake", "tavily"] = Field(default="fake")
    tool_fetch_enabled: bool = Field(default=False)
    tool_attempt_timeout_seconds: float = Field(default=8.0, gt=0)
    research_node_tool_deadline_seconds: float = Field(default=45.0, gt=0)
    tool_max_logical_calls_per_research_node: int = Field(default=6, ge=1)
    tool_max_attempts_per_call: int = Field(default=2, ge=1, le=2)
    tavily_api_key: SecretStr | None = Field(default=None)

    # Recovery / retry backoff (Slice 12B).
    retry_base_seconds: float = Field(default=5.0, gt=0)
    retry_max_backoff_seconds: float = Field(default=60.0, gt=0)
    retry_jitter_max_seconds: float = Field(default=0.0, ge=0)

    # Operator review API (Slice 12B). Off by default.
    review_api_enabled: bool = Field(default=False)

    # Embeddings / retrieval (Milestone 10B). Default fake keeps CI offline.
    embedding_provider: Literal["fake", "openai"] = Field(default="fake")
    embedding_profile: Literal["embeddings.v1"] = Field(default="embeddings.v1")
    embedding_call_timeout_seconds: float = Field(default=25.0, gt=0)
    retrieval_default_k: int = Field(default=5, ge=1, le=8)
    retrieval_use_hnsw: bool = Field(default=True)


def get_settings() -> Settings:
    """Return settings from the current environment."""
    return Settings()
