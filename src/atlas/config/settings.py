"""Application configuration loaded from the environment."""

from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
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

    # Ephemeral coordination (Milestone 13 Slice 13A). Redis is never
    # authoritative: it backs rate limiting and worker heartbeats only.
    # Default `noop` keeps CI/local dev offline unless explicitly enabled.
    coordination_provider: Literal["noop", "redis"] = Field(default="noop")
    redis_url: str = Field(default="redis://127.0.0.1:6380/0")
    # Bounded connect/socket timeouts so a slow/unavailable Redis fails open
    # quickly instead of adding meaningful latency to requests or the worker.
    redis_connect_timeout_seconds: float = Field(default=0.2, gt=0)
    redis_socket_timeout_seconds: float = Field(default=0.2, gt=0)

    # POST /v1/research-jobs rate limit, keyed by direct peer IP. Idempotent
    # replays count toward the limit (the limiter runs before idempotency
    # resolution).
    rate_limit_max_requests: int = Field(default=10, ge=1)
    rate_limit_window_seconds: int = Field(default=60, ge=1)

    # Worker heartbeat (dedicated thread; independent of the poll/process loop).
    # TTL must be at least twice the interval so a single missed refresh cannot
    # expire the key before the next scheduled beat.
    heartbeat_interval_seconds: float = Field(default=5.0, gt=0)
    heartbeat_ttl_seconds: int = Field(default=15, ge=1)

    # Transactional outbox relay (Milestone 13 Slice 13B). Kafka delivery is
    # deferred; these knobs govern claim batching and publish leases only.
    outbox_relay_batch_size: int = Field(default=50, ge=1, le=500)
    outbox_publish_lease_seconds: float = Field(default=30.0, gt=0)

    # Real Kafka broker (Milestone 13 Slice 13C1). The executable
    # ``python -m atlas.outbox`` requires Kafka unconditionally; there is no
    # settings-driven fake-producer selection at runtime (fake remains
    # test-only via direct construction). The reserved topic name is a fixed
    # constant (``atlas.eventing.topic.RESEARCH_JOB_EVENTS_TOPIC_V1``), never
    # settings-configurable, so no arbitrary runtime topic can be selected.
    kafka_bootstrap_servers: str = Field(default="127.0.0.1:9094")
    # Bounds one record's Kafka delivery-callback-confirmed publish() call.
    # Must stay safely below outbox_publish_lease_seconds (validated below)
    # so a claimed row cannot outlive its lease while still "in flight".
    kafka_delivery_timeout_seconds: float = Field(default=10.0, gt=0)
    # Shared bound for socket.timeout.ms and request.timeout.ms.
    kafka_socket_timeout_seconds: float = Field(default=10.0, gt=0)
    kafka_topic_verify_timeout_seconds: float = Field(default=10.0, gt=0)
    # Kafka relay executable poll/backoff interval between claim attempts
    # when the previous attempt claimed nothing or failed to publish.
    outbox_relay_poll_interval_seconds: float = Field(default=1.0, gt=0)
    # Explicit safety margin (seconds) that kafka_delivery_timeout_seconds
    # must stay below outbox_publish_lease_seconds by. See
    # _validate_kafka_delivery_timeout_margin below.
    kafka_delivery_timeout_lease_margin_seconds: float = Field(default=5.0, ge=0)

    @model_validator(mode="after")
    def _validate_heartbeat_timing(self) -> Self:
        if self.heartbeat_ttl_seconds < (2 * self.heartbeat_interval_seconds):
            raise ValueError(
                "heartbeat_ttl_seconds must be at least twice "
                "heartbeat_interval_seconds"
            )
        return self

    @model_validator(mode="after")
    def _validate_kafka_delivery_timeout_margin(self) -> Self:
        margin = self.kafka_delivery_timeout_lease_margin_seconds
        bounded = self.kafka_delivery_timeout_seconds + margin
        if bounded > self.outbox_publish_lease_seconds:
            raise ValueError(
                "kafka_delivery_timeout_seconds plus "
                "kafka_delivery_timeout_lease_margin_seconds must not exceed "
                "outbox_publish_lease_seconds."
            )
        return self


def get_settings() -> Settings:
    """Return settings from the current environment."""
    return Settings()
