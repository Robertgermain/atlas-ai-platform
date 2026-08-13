"""Application configuration loaded from the environment."""

from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from atlas.config.timeout_math import (
    effective_connect_timeout_seconds,
    effective_statement_timeout_seconds,
)


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

    # Business Kafka consumer (Milestone 13 Slice 13C2A). Consumer group and
    # client identity are fixed constants (atlas.consumer.identity), never
    # settings-configurable, matching the reserved-topic-constant pattern.
    # These knobs govern only bounded polling and Kafka group timing.
    consumer_poll_timeout_seconds: float = Field(default=1.0, gt=0)
    consumer_session_timeout_seconds: float = Field(default=10.0, gt=0)
    consumer_max_poll_interval_seconds: float = Field(default=300.0, gt=0)

    # Kafka consumer bounded retry, DLQ, and replay (Milestone 13 Slice
    # 13C2B). Retry attempts are process-local (never durable): a consumer
    # restart resets the transient-infrastructure retry budget. See
    # atlas.consumer.timing for the same worst-case timing formula applied
    # at runtime; _validate_consumer_retry_timing_margin below intentionally
    # duplicates that arithmetic rather than importing it, to avoid a
    # config -> consumer import cycle (atlas.consumer already imports
    # atlas.config transitively).
    consumer_retry_max_attempts: int = Field(default=3, ge=1)
    consumer_retry_base_seconds: float = Field(default=1.0, gt=0)
    consumer_retry_max_backoff_seconds: float = Field(default=30.0, gt=0)
    # Deterministic (no jitter) is intentional for the current
    # single-consumer/single-partition architecture, where randomized
    # backoff buys no contention benefit but would make the worst-case
    # timing bound probabilistic instead of exact. Future multi-partition
    # or multi-consumer work may reconsider this.
    consumer_retry_jitter_max_seconds: float = Field(default=0.0, ge=0)
    consumer_retry_safety_margin_seconds: float = Field(default=60.0, ge=0)
    consumer_db_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    consumer_db_pool_timeout_seconds: float = Field(default=5.0, gt=0)
    consumer_db_statement_timeout_seconds: float = Field(default=5.0, gt=0)
    # Non-DB-timeout allowance (object construction, Pydantic validation,
    # JSON encode/decode, GC/scheduler jitter) that no PostgreSQL-side
    # timeout bounds.
    consumer_retry_processing_overhead_seconds: float = Field(default=2.0, ge=0)
    # Conservative cap on SQL statements/round trips per processing attempt.
    # Verified maximum today is 5 (normal apply); this cap is intentionally
    # larger, see tests/persistence/test_consumer_statement_counts.py.
    consumer_max_db_round_trips_per_attempt: int = Field(default=8, ge=1)
    consumer_replay_lease_seconds: float = Field(default=90.0, gt=0)

    # Prometheus metrics (Milestone 15 Slice 15A2). The API exposes ``/metrics``
    # on its own existing HTTP port; the worker, outbox relay, and Kafka
    # consumer each bind a minimal internal-only HTTP server on this fixed
    # port to serve their own process-local registry. Never published to the
    # host by docker-compose.yml -- see docs/TECHNICAL_DESIGN.md. A bind
    # failure is fail-open (the process continues without a metrics
    # endpoint); metrics are operational telemetry, never authoritative.
    metrics_port: int = Field(default=9464, ge=1, le=65535)

    # Distributed tracing (Milestone 15 Slice 15A3). OTLP/HTTP export to the
    # local OpenTelemetry Collector. Fail-open:
    # atlas.observability.tracing.provider.configure_tracing never blocks or
    # fails process startup -- an exporter/processor construction failure
    # leaves spans created in-process but never exported. deployment_
    # environment is a fixed bounded label (never derived from arbitrary
    # environment content); "local" is correct for every current runtime
    # (host-run process, Compose) -- "kind"/"aws" are reserved for
    # Milestone 17+/18+ and not selected by anything today.
    otel_exporter_otlp_traces_endpoint: str = Field(
        default="http://otel-collector:4318/v1/traces"
    )
    otel_deployment_environment: Literal["local", "kind", "aws"] = Field(
        default="local"
    )

    @model_validator(mode="after")
    def _validate_heartbeat_timing(self) -> Self:
        if self.heartbeat_ttl_seconds < (2 * self.heartbeat_interval_seconds):
            raise ValueError(
                "heartbeat_ttl_seconds must be at least twice "
                "heartbeat_interval_seconds"
            )
        return self

    @model_validator(mode="after")
    def _validate_consumer_retry_timing_margin(self) -> Self:
        """Fail closed unless the worst-case retry episode fits under Kafka's bound.

        Mirrors atlas.consumer.timing.worst_case_total_processing_seconds
        exactly (kept duplicated on purpose -- see the field comment above).
        Uses ``effective_connect_timeout_seconds``/``effective_statement_
        timeout_seconds`` (ceiling-rounded, floored at 1) rather than the
        raw configured floats, matching exactly what ``atlas.consumer.db.
        build_consumer_engine`` applies to the real engine at runtime --
        otherwise a fractional configured value could round up to a larger
        effective timeout than this proof assumed, silently invalidating
        the margin it exists to guarantee.
        """
        worst_case_attempt_seconds = (
            self.consumer_db_pool_timeout_seconds
            + effective_connect_timeout_seconds(
                self.consumer_db_connect_timeout_seconds
            )
            + self.consumer_max_db_round_trips_per_attempt
            * effective_statement_timeout_seconds(
                self.consumer_db_statement_timeout_seconds
            )
            + self.consumer_retry_processing_overhead_seconds
        )
        backoff_sum = sum(
            min(
                self.consumer_retry_base_seconds * (2**attempt_index),
                self.consumer_retry_max_backoff_seconds,
            )
            for attempt_index in range(self.consumer_retry_max_attempts - 1)
        )
        worst_case_total_processing_seconds = (
            self.consumer_retry_max_attempts * worst_case_attempt_seconds
            + backoff_sum
            + self.consumer_retry_safety_margin_seconds
        )
        if (
            worst_case_total_processing_seconds
            >= self.consumer_max_poll_interval_seconds
        ):
            raise ValueError(
                "Kafka consumer retry timing settings (max attempts, DB "
                "timeouts, round-trip cap, backoff, safety margin) must "
                "sum to strictly less than consumer_max_poll_interval_seconds."
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
