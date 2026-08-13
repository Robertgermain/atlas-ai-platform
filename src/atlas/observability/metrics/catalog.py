"""The Atlas Prometheus metric catalog (Slice 15A2).

:class:`AtlasMetrics` owns one ``prometheus_client.CollectorRegistry`` and
every metric Atlas defines, plus one ``observe_*``/``set_*`` method per
metric family. This is the *only* place production code constructs a
Prometheus metric object -- callers never import ``prometheus_client``
directly, so there is exactly one place that can register a duplicate
metric name or accept an unbounded label value.

Process singleton, not a global at import time
-------------------------------------------------

:func:`default_metrics` lazily constructs one process-wide
:class:`AtlasMetrics` bound to ``prometheus_client.REGISTRY`` (the
default global registry ``generate_latest()`` reads from with no
arguments) and returns the same instance on every call. Production code
(API, worker, outbox relay, consumer) always calls
:func:`default_metrics` -- directly, or through a
``field(default_factory=default_metrics)`` constructor default -- rather
than constructing :class:`AtlasMetrics` itself, so every process has
exactly one registry. Tests that need isolation construct
``AtlasMetrics(CollectorRegistry())`` directly and inject it, which
sidesteps ``default_metrics()``'s shared singleton and therefore never
collides with another test's metric registrations.

Failure containment
---------------------

Every ``observe_*``/``set_*`` method catches any exception the
underlying ``prometheus_client`` call raises (e.g. an unexpected label
count), logs :attr:`atlas.observability.events.Event.
METRIC_OBSERVATION_FAILED` through the Slice 15A1 structured logger
(never the caller's business exception, never the rejected value), and
returns normally. A metrics observation can therefore never cause a job,
API request, provider call, Kafka publication, or consumer transaction
to fail. This module's own logging call never itself observes a metric,
so there is no recursion between metrics and logging in either
direction.

Authoritative-event semantics (see docs/TECHNICAL_DESIGN.md for the full
table): durable business-transition counters (research-job terminal
outcomes, worker processing outcomes, outbox publications, evaluation
runs) are observed strictly *after* the authoritative database
transaction that decides the outcome commits -- never before, and never
speculatively for an attempt that might still be reclaimed by another
owner. This means an unavoidable crash window exists between a durable
commit and the in-memory counter increment: Prometheus is operational
telemetry, not an exactly-once ledger, and PostgreSQL/Kafka durable state
remains authoritative regardless of what any counter reads.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Final

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)

from atlas.observability.events import Event
from atlas.observability.logging import log_exception_boundary

_logger = logging.getLogger(__name__)

#: Prometheus's own recommended default; reused verbatim for HTTP request
#: duration because Atlas's API has no single configured request timeout
#: to derive a tighter bound from (its request handlers are DB-bound
#: reads/writes; provider/tool calls only ever happen on the worker's own
#: background processing path, never synchronously inside an API request).
HTTP_REQUEST_DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

#: Upper bound derived from the larger of the two configured attempt
#: timeouts this histogram covers: ``model_call_timeout_seconds`` (default
#: 25s) and ``tool_attempt_timeout_seconds`` (default 8s). 30s comfortably
#: exceeds both defaults' attempt deadline so a successful attempt's
#: latency is almost always captured in a finite bucket; ``+Inf``
#: (supplied automatically by ``prometheus_client.Histogram``) still
#: captures a slower attempt under a larger configured timeout.
MODEL_TOOL_ATTEMPT_DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    15.0,
    20.0,
    25.0,
    30.0,
)

#: Upper bound derived from the largest single-node deadline in the
#: workflow: ``research_node_tool_deadline_seconds`` (default 45s, for the
#: ``research`` node's own bounded tool budget). 90s leaves headroom above
#: that for the ``plan``/``draft`` nodes' own ``model_call_timeout_seconds``
#: (default 25s) plus ledger I/O overhead.
WORKFLOW_NODE_DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.1,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    20.0,
    30.0,
    45.0,
    60.0,
    90.0,
)

#: Approved fixed 300s top finite bucket for whole-job processing
#: duration (Slice 15A2 approval #10): comfortably above
#: ``worker_processing_timeout_seconds`` (default 60s) to still bucket a
#: job that ran long before the orchestration timeout fired, while
#: ``+Inf`` catches anything slower without needing an ever-larger finite
#: bucket added later.
JOB_PROCESSING_DURATION_BUCKETS: Final[tuple[float, ...]] = (
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    180.0,
    300.0,
)

_OTHER: Final[str] = "other"


def _bounded(value: str, allowed: frozenset[str]) -> str:
    """Return ``value`` if it is in ``allowed``, else the fixed ``"other"`` label.

    Defense in depth: every call site today already passes a value drawn
    from a closed application-level enum or a small fixed set of outcome
    strings, but this still bounds the label at the metrics boundary
    itself so a future call-site typo or a new, not-yet-allowlisted value
    degrades to one bounded label rather than growing cardinality.
    """
    return value if value in allowed else _OTHER


_HTTP_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", _OTHER}
)
_HTTP_ROUTES: Final[frozenset[str]] = frozenset(
    {
        "/health",
        "/ready",
        "/metrics",
        "/v1/research-jobs",
        "/v1/research-jobs/{job_id}",
        "/v1/research-jobs/{job_id}/evaluation",
        "/v1/research-jobs/{job_id}/citations",
        "/v1/research-jobs/{job_id}/review-decisions",
        "/v1/evidence/documents",
        "/v1/evidence/items/{evidence_item_id}",
        "unmatched",
        _OTHER,
    }
)
_HTTP_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "200",
        "202",
        "404",
        "409",
        "422",
        "429",
        "500",
        "503",
        "1xx_other",
        "2xx_other",
        "3xx_other",
        "4xx_other",
        "5xx_other",
        _OTHER,
    }
)
_JOB_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"completed", "failed", _OTHER}
)
_WORKER_CLAIM_OUTCOMES: Final[frozenset[str]] = frozenset({"claimed", "empty", _OTHER})
_WORKER_PROCESSING_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "completed",
        "failed",
        "claim_ownership_lost",
        "finalization_failed",
        "paused_for_review",
        "retry_scheduled",
        _OTHER,
    }
)
_WORKFLOW_NODE_NAMES: Final[frozenset[str]] = frozenset(
    {
        "validate",
        "plan",
        "research",
        "draft",
        "verify_citations",
        "evaluate",
        "policy",
        "repair",
        "await_review",
        "complete",
        "terminal",
        _OTHER,
    }
)
_NODE_OUTCOMES: Final[frozenset[str]] = frozenset({"completed", "failed", _OTHER})
_MODEL_PROVIDERS: Final[frozenset[str]] = frozenset({"openai", "anthropic", _OTHER})
_MODEL_NODE_NAMES: Final[frozenset[str]] = frozenset({"plan", "draft", _OTHER})
_ATTEMPT_OUTCOMES: Final[frozenset[str]] = frozenset({"succeeded", "failed", _OTHER})
_RETRY_CLASSES: Final[frozenset[str]] = frozenset(
    {
        "timeout",
        "rate_limited",
        "temporary",
        "auth_config",
        "invalid_request",
        "invalid_structured_output",
        "refusal",
        "permission_denied",
        "ssrf_blocked",
        "content_rejected",
        "budget_exhausted",
        "unknown",
        "none",
        _OTHER,
    }
)
_TOKEN_TYPES: Final[frozenset[str]] = frozenset({"input", "output"})
_TOOL_IDS: Final[frozenset[str]] = frozenset({"web_search", "fetch_url", _OTHER})
_TOOL_PROVIDERS: Final[frozenset[str]] = frozenset({"fake", "tavily", "httpx", _OTHER})
_EVALUATION_PROFILES: Final[frozenset[str]] = frozenset(
    {"evaluation.candidate.v1", _OTHER}
)
_EVALUATION_RUN_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"succeeded", "failed", _OTHER}
)
_EVALUATION_DIMENSIONS: Final[frozenset[str]] = frozenset(
    {
        "citation_integrity",
        "tool_use",
        "report_structure",
        "coverage",
        "completeness",
        "lexical_id_groundedness",
        "semantic_groundedness",
        _OTHER,
    }
)
_DIMENSION_OUTCOMES: Final[frozenset[str]] = frozenset({"passed", "failed", _OTHER})
_REVIEW_DECISIONS: Final[frozenset[str]] = frozenset({"approve", "reject", _OTHER})
_OUTBOX_PUBLICATION_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "empty",
        "published",
        "recoverable_failure",
        "fatal_failure",
        "ownership_lost",
        "unexpected_failure",
        _OTHER,
    }
)
_CONSUMER_MESSAGE_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "no_message",
        "applied",
        "duplicate",
        "dead_lettered",
        "poll_recoverable_error",
        "terminal_error",
        _OTHER,
    }
)
_RATE_LIMIT_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"allowed", "denied", "failed_open", _OTHER}
)
_HEARTBEAT_OUTCOMES: Final[frozenset[str]] = frozenset({"success", "failure", _OTHER})
_LANGSMITH_OPERATIONS: Final[frozenset[str]] = frozenset(
    {"initialize", "enqueue", "export", "flush", _OTHER}
)
_LANGSMITH_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"success", "error", "timeout", "disabled", _OTHER}
)

#: Mirrors ``atlas.recovery.policy.PolicyAction`` (the ``Literal`` Atlas
#: itself defines and returns from every ``PolicyDecision``). Kept as an
#: independent literal set here, not an import, so the generic metrics
#: catalog never depends on the domain recovery-policy module.
_RECOVERY_ACTIONS: Final[frozenset[str]] = frozenset(
    {"complete", "repair", "await_review", "retry", "terminal", _OTHER}
)
#: Mirrors every ``atlas.recovery.policy.FailureCategory`` member.
_FAILURE_CATEGORIES: Final[frozenset[str]] = frozenset(
    {
        "QUALITY_CITATION_INTEGRITY",
        "QUALITY_STRUCTURE",
        "QUALITY_COVERAGE",
        "QUALITY_GROUNDEDNESS",
        "QUALITY_COMPLETENESS",
        "QUALITY_TOOL_POLICY",
        "TRANSIENT_TIMEOUT",
        "TRANSIENT_RATE_LIMIT",
        "TRANSIENT_PROVIDER",
        "PERMANENT_VALIDATION",
        "PERMANENT_AUTH_CONFIG",
        "PERMANENT_BUDGET_EXHAUSTED",
        "REPAIRABLE_DRAFT",
        "NEEDS_HUMAN_REVIEW",
        "TERMINAL_UNKNOWN",
        _OTHER,
    }
)
#: The three bounded stages a Kafka-consumer per-record retry loop can be
#: backing off in (see ``atlas.consumer.runner.ConsumerRunner``): applying
#: the business effect, committing the offset, or persisting a dead-letter
#: row. Never the unbounded exception class or attempt number.
_CONSUMER_RETRY_STAGES: Final[frozenset[str]] = frozenset(
    {"apply", "commit", "dead_letter_upsert", _OTHER}
)
_CONSUMER_OFFSET_COMMIT_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"success", "failure", "deadline_exceeded", "shutdown_requested", _OTHER}
)
#: Mirrors ``atlas.consumer.errors.ALLOWED_FAILURE_CODES`` -- the same
#: fixed allowlist backing the ``consumer_dead_letters.failure_code``
#: PostgreSQL CHECK constraint. Kept as an independent literal set here,
#: not an import, so the generic metrics catalog never depends on the
#: Kafka-consumer module.
_CONSUMER_DEAD_LETTER_FAILURE_CODES: Final[frozenset[str]] = frozenset(
    {
        "missing_headers",
        "unexpected_headers_shape",
        "unexpected_header_key_type",
        "duplicate_header_key",
        "null_header_value",
        "undecodable_header_value",
        "unexpected_header_value_type",
        "unexpected_header_keys",
        "event_type_header_mismatch",
        "event_version_header_mismatch",
        "aggregate_type_header_mismatch",
        "missing_value",
        "value_too_large",
        "undecodable_value",
        "invalid_json",
        "value_not_an_object",
        "schema_validation_failed",
        "lifecycle_order_violation",
        _OTHER,
    }
)


class AtlasMetrics:
    """Owns one registry and every Atlas Prometheus metric.

    Constructed with an explicit ``CollectorRegistry`` (never a mutable
    default argument): production code uses :func:`default_metrics`'s
    shared singleton; tests construct their own isolated instance with a
    fresh ``CollectorRegistry()`` so repeated test-module imports never
    raise ``prometheus_client``'s duplicate-registration error.
    """

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry

        self._http_requests_total = Counter(
            "atlas_http_requests_total",
            "Total HTTP requests handled by the Atlas API.",
            ("method", "route", "status"),
            registry=registry,
        )
        self._http_request_duration_seconds = Histogram(
            "atlas_http_request_duration_seconds",
            "HTTP request duration in seconds, observed after the response is built.",
            ("method", "route", "status"),
            buckets=HTTP_REQUEST_DURATION_BUCKETS,
            registry=registry,
        )
        self._research_job_submissions_total = Counter(
            "atlas_research_job_submissions_total",
            "Research jobs newly created (idempotent replays are not counted again).",
            registry=registry,
        )
        self._research_job_terminal_total = Counter(
            "atlas_research_job_terminal_total",
            "Research jobs that reached a durable terminal status.",
            ("status",),
            registry=registry,
        )
        self._worker_claims_total = Counter(
            "atlas_worker_claims_total",
            "Worker claim attempts against the research-job queue.",
            ("outcome",),
            registry=registry,
        )
        self._worker_processing_outcomes_total = Counter(
            "atlas_worker_processing_outcomes_total",
            "Worker processing attempts by their authoritative finalize outcome.",
            ("outcome",),
            registry=registry,
        )
        self._worker_job_processing_duration_seconds = Histogram(
            "atlas_worker_job_processing_duration_seconds",
            "Wall-clock seconds from claim to a worker processing outcome.",
            ("outcome",),
            buckets=JOB_PROCESSING_DURATION_BUCKETS,
            registry=registry,
        )
        self._workflow_node_executions_total = Counter(
            "atlas_workflow_node_executions_total",
            "LangGraph node executions by node and outcome.",
            ("node_name", "outcome"),
            registry=registry,
        )
        self._workflow_node_duration_seconds = Histogram(
            "atlas_workflow_node_duration_seconds",
            "LangGraph node execution duration in seconds.",
            ("node_name", "outcome"),
            buckets=WORKFLOW_NODE_DURATION_BUCKETS,
            registry=registry,
        )
        self._model_invocations_total = Counter(
            "atlas_model_invocations_total",
            "Logical model invocations by their ledger-committed outcome.",
            ("node_name", "provider", "outcome"),
            registry=registry,
        )
        self._model_attempts_total = Counter(
            "atlas_model_attempts_total",
            "Physical model provider attempts by outcome and retry class.",
            ("node_name", "provider", "outcome", "retry_class"),
            registry=registry,
        )
        self._model_attempt_duration_seconds = Histogram(
            "atlas_model_attempt_duration_seconds",
            "Physical model provider attempt duration in seconds.",
            ("node_name", "provider"),
            buckets=MODEL_TOOL_ATTEMPT_DURATION_BUCKETS,
            registry=registry,
        )
        self._model_tokens_total = Counter(
            "atlas_model_tokens_total",
            "Tokens recorded against a succeeded model attempt.",
            ("node_name", "provider", "token_type"),
            registry=registry,
        )
        self._model_cost_usd_total = Counter(
            "atlas_model_cost_usd_total",
            "Estimated USD cost recorded against a succeeded model attempt.",
            ("node_name", "provider"),
            registry=registry,
        )
        self._tool_invocations_total = Counter(
            "atlas_tool_invocations_total",
            "Logical tool invocations by their ledger-committed outcome.",
            ("tool_id", "provider", "outcome"),
            registry=registry,
        )
        self._tool_attempts_total = Counter(
            "atlas_tool_attempts_total",
            "Physical tool provider attempts by outcome and retry class.",
            ("tool_id", "provider", "outcome", "retry_class"),
            registry=registry,
        )
        self._tool_attempt_duration_seconds = Histogram(
            "atlas_tool_attempt_duration_seconds",
            "Physical tool provider attempt duration in seconds.",
            ("tool_id", "provider"),
            buckets=MODEL_TOOL_ATTEMPT_DURATION_BUCKETS,
            registry=registry,
        )
        self._evaluation_runs_total = Counter(
            "atlas_evaluation_runs_total",
            "Evaluation runs by profile and their finalized outcome.",
            ("profile", "outcome"),
            registry=registry,
        )
        self._evaluation_dimension_outcomes_total = Counter(
            "atlas_evaluation_dimension_outcomes_total",
            "Graded evaluation dimensions by dimension and pass/fail outcome.",
            ("dimension", "outcome"),
            registry=registry,
        )
        self._human_review_decisions_total = Counter(
            "atlas_human_review_decisions_total",
            "Durably recorded operator review decisions.",
            ("decision",),
            registry=registry,
        )
        self._outbox_relay_runs_total = Counter(
            "atlas_outbox_relay_runs_total",
            "Outbox relay run_once() calls, one per call, by its RelayRunOutcome.",
            ("outcome",),
            registry=registry,
        )
        self._outbox_published_events_total = Counter(
            "atlas_outbox_published_events_total",
            "Individual outbox events actually marked published by the relay.",
            registry=registry,
        )
        self._outbox_relay_lock_held = Gauge(
            "atlas_outbox_relay_lock_held",
            "1 while this process holds the singleton outbox-relay advisory lock.",
            registry=registry,
        )
        self._outbox_backlog_size = Gauge(
            "atlas_outbox_backlog_size",
            "Unpublished outbox row count, sampled by the relay's poll loop.",
            registry=registry,
        )
        self._outbox_backlog_age_seconds = Gauge(
            "atlas_outbox_backlog_age_seconds",
            "Age in seconds of the oldest unpublished outbox row at last sample.",
            registry=registry,
        )
        self._outbox_backlog_collection_success_timestamp_seconds = Gauge(
            "atlas_outbox_backlog_collection_success_timestamp_seconds",
            "Unix timestamp of the last successful backlog sample; staleness signal.",
            registry=registry,
        )
        self._consumer_messages_total = Counter(
            "atlas_consumer_messages_total",
            "Kafka consumer poll-loop outcomes, by outcome.",
            ("outcome",),
            registry=registry,
        )
        self._consumer_retry_attempts_total = Counter(
            "atlas_consumer_retry_attempts_total",
            "Per-record consumer retry-loop backoff decisions, by bounded stage.",
            ("stage",),
            registry=registry,
        )
        self._consumer_offset_commit_outcomes_total = Counter(
            "atlas_consumer_offset_commit_outcomes_total",
            "Kafka offset-commit attempts, by outcome.",
            ("outcome",),
            registry=registry,
        )
        self._consumer_dead_letters_total = Counter(
            "atlas_consumer_dead_letters_total",
            "Durably persisted consumer dead-letter rows, by failure_code.",
            ("failure_code",),
            registry=registry,
        )
        self._recovery_decisions_total = Counter(
            "atlas_recovery_decisions_total",
            "Freshly-persisted (non-replayed) recovery policy decisions.",
            ("action", "failure_category"),
            registry=registry,
        )
        self._redis_rate_limit_decisions_total = Counter(
            "atlas_redis_rate_limit_decisions_total",
            "POST /v1/research-jobs rate-limit decisions, by outcome.",
            ("outcome",),
            registry=registry,
        )
        self._worker_heartbeat_writes_total = Counter(
            "atlas_worker_heartbeat_writes_total",
            "Worker heartbeat-recorder write attempts, by outcome.",
            ("outcome",),
            registry=registry,
        )
        self._worker_heartbeat_last_success_timestamp_seconds = Gauge(
            "atlas_worker_heartbeat_last_success_timestamp_seconds",
            "Unix timestamp of this process's last successful heartbeat write.",
            registry=registry,
        )
        self._database_readiness_failures_total = Counter(
            "atlas_database_readiness_failures_total",
            "API /ready checks that reported the database not ready.",
            registry=registry,
        )
        self._langsmith_operations_total = Counter(
            "atlas_langsmith_operations_total",
            "LangSmith client operations by phase and bounded outcome.",
            ("operation", "outcome"),
            registry=registry,
        )

    def _contain(self, boundary: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except Exception as exc:
            log_exception_boundary(
                _logger,
                Event.METRIC_OBSERVATION_FAILED,
                exc,
                level=logging.WARNING,
                outcome=boundary,
            )

    # -- HTTP --------------------------------------------------------------

    def observe_http_request(
        self, *, method: str, route: str, status: str, duration_seconds: float
    ) -> None:
        m = _bounded(method, _HTTP_METHODS)
        r = _bounded(route, _HTTP_ROUTES)
        s = _bounded(status, _HTTP_STATUSES)

        def _do() -> None:
            self._http_requests_total.labels(method=m, route=r, status=s).inc()
            self._http_request_duration_seconds.labels(
                method=m, route=r, status=s
            ).observe(duration_seconds)

        self._contain("http_request", _do)

    # -- Research jobs -------------------------------------------------------

    def observe_research_job_submitted(self) -> None:
        self._contain(
            "research_job_submitted", self._research_job_submissions_total.inc
        )

    def observe_research_job_terminal(self, *, status: str) -> None:
        bounded = _bounded(status, _JOB_TERMINAL_STATUSES)
        self._contain(
            "research_job_terminal",
            lambda: self._research_job_terminal_total.labels(status=bounded).inc(),
        )

    # -- Worker --------------------------------------------------------------

    def observe_worker_claim(self, *, outcome: str) -> None:
        bounded = _bounded(outcome, _WORKER_CLAIM_OUTCOMES)
        self._contain(
            "worker_claim",
            lambda: self._worker_claims_total.labels(outcome=bounded).inc(),
        )

    def observe_worker_processing(
        self, *, outcome: str, duration_seconds: float
    ) -> None:
        bounded = _bounded(outcome, _WORKER_PROCESSING_OUTCOMES)

        def _do() -> None:
            self._worker_processing_outcomes_total.labels(outcome=bounded).inc()
            self._worker_job_processing_duration_seconds.labels(
                outcome=bounded
            ).observe(duration_seconds)

        self._contain("worker_processing", _do)

    # -- Workflow nodes --------------------------------------------------------

    def observe_workflow_node(
        self, *, node_name: str, outcome: str, duration_seconds: float
    ) -> None:
        n = _bounded(node_name, _WORKFLOW_NODE_NAMES)
        o = _bounded(outcome, _NODE_OUTCOMES)

        def _do() -> None:
            self._workflow_node_executions_total.labels(node_name=n, outcome=o).inc()
            self._workflow_node_duration_seconds.labels(node_name=n, outcome=o).observe(
                duration_seconds
            )

        self._contain("workflow_node", _do)

    # -- Model invocations -----------------------------------------------------

    def observe_model_attempt(
        self,
        *,
        node_name: str,
        provider: str,
        outcome: str,
        retry_class: str,
        duration_seconds: float,
    ) -> None:
        n = _bounded(node_name, _MODEL_NODE_NAMES)
        p = _bounded(provider, _MODEL_PROVIDERS)
        o = _bounded(outcome, _ATTEMPT_OUTCOMES)
        rc = _bounded(retry_class, _RETRY_CLASSES)

        def _do() -> None:
            self._model_attempts_total.labels(
                node_name=n, provider=p, outcome=o, retry_class=rc
            ).inc()
            self._model_attempt_duration_seconds.labels(
                node_name=n, provider=p
            ).observe(duration_seconds)

        self._contain("model_attempt", _do)

    def observe_model_invocation(
        self, *, node_name: str, provider: str, outcome: str
    ) -> None:
        n = _bounded(node_name, _MODEL_NODE_NAMES)
        p = _bounded(provider, _MODEL_PROVIDERS)
        o = _bounded(outcome, _ATTEMPT_OUTCOMES)
        self._contain(
            "model_invocation",
            lambda: self._model_invocations_total.labels(
                node_name=n, provider=p, outcome=o
            ).inc(),
        )

    def observe_model_tokens(
        self,
        *,
        node_name: str,
        provider: str,
        input_tokens: int | None,
        output_tokens: int | None,
    ) -> None:
        n = _bounded(node_name, _MODEL_NODE_NAMES)
        p = _bounded(provider, _MODEL_PROVIDERS)

        def _do() -> None:
            if input_tokens is not None and input_tokens > 0:
                self._model_tokens_total.labels(
                    node_name=n, provider=p, token_type="input"
                ).inc(input_tokens)
            if output_tokens is not None and output_tokens > 0:
                self._model_tokens_total.labels(
                    node_name=n, provider=p, token_type="output"
                ).inc(output_tokens)

        self._contain("model_tokens", _do)

    def observe_model_cost(
        self, *, node_name: str, provider: str, cost_usd: float | None
    ) -> None:
        if cost_usd is None or cost_usd <= 0:
            return
        n = _bounded(node_name, _MODEL_NODE_NAMES)
        p = _bounded(provider, _MODEL_PROVIDERS)
        self._contain(
            "model_cost",
            lambda: self._model_cost_usd_total.labels(node_name=n, provider=p).inc(
                cost_usd
            ),
        )

    # -- Tool invocations ------------------------------------------------------

    def observe_tool_attempt(
        self,
        *,
        tool_id: str,
        provider: str,
        outcome: str,
        retry_class: str,
        duration_seconds: float,
    ) -> None:
        t = _bounded(tool_id, _TOOL_IDS)
        p = _bounded(provider, _TOOL_PROVIDERS)
        o = _bounded(outcome, _ATTEMPT_OUTCOMES)
        rc = _bounded(retry_class, _RETRY_CLASSES)

        def _do() -> None:
            self._tool_attempts_total.labels(
                tool_id=t, provider=p, outcome=o, retry_class=rc
            ).inc()
            self._tool_attempt_duration_seconds.labels(tool_id=t, provider=p).observe(
                duration_seconds
            )

        self._contain("tool_attempt", _do)

    def observe_tool_invocation(
        self, *, tool_id: str, provider: str, outcome: str
    ) -> None:
        t = _bounded(tool_id, _TOOL_IDS)
        p = _bounded(provider, _TOOL_PROVIDERS)
        o = _bounded(outcome, _ATTEMPT_OUTCOMES)
        self._contain(
            "tool_invocation",
            lambda: self._tool_invocations_total.labels(
                tool_id=t, provider=p, outcome=o
            ).inc(),
        )

    # -- Evaluation --------------------------------------------------------------

    def observe_evaluation_run(self, *, profile: str, outcome: str) -> None:
        p = _bounded(profile, _EVALUATION_PROFILES)
        o = _bounded(outcome, _EVALUATION_RUN_OUTCOMES)
        self._contain(
            "evaluation_run",
            lambda: self._evaluation_runs_total.labels(profile=p, outcome=o).inc(),
        )

    def observe_evaluation_dimension(self, *, dimension: str, outcome: str) -> None:
        d = _bounded(dimension, _EVALUATION_DIMENSIONS)
        o = _bounded(outcome, _DIMENSION_OUTCOMES)
        self._contain(
            "evaluation_dimension",
            lambda: self._evaluation_dimension_outcomes_total.labels(
                dimension=d, outcome=o
            ).inc(),
        )

    # -- Recovery / human review ---------------------------------------------

    def observe_human_review_decision(self, *, decision: str) -> None:
        d = _bounded(decision, _REVIEW_DECISIONS)
        self._contain(
            "human_review_decision",
            lambda: self._human_review_decisions_total.labels(decision=d).inc(),
        )

    # -- Outbox relay --------------------------------------------------------

    def observe_outbox_relay_run(self, *, outcome: str) -> None:
        """Count one completed ``OutboxRelay.run_once()`` call by its outcome.

        One observation per call regardless of how many rows that call
        published -- see :meth:`observe_outbox_published_events` for the
        separate, precise published-event-volume counter.
        """
        o = _bounded(outcome, _OUTBOX_PUBLICATION_OUTCOMES)
        self._contain(
            "outbox_relay_run",
            lambda: self._outbox_relay_runs_total.labels(outcome=o).inc(),
        )

    def observe_outbox_published_events(self, *, count: int) -> None:
        """Count actual outbox events marked published (not relay-loop calls)."""
        if count <= 0:
            return
        self._contain(
            "outbox_published_events",
            lambda: self._outbox_published_events_total.inc(count),
        )

    def set_outbox_relay_lock_held(self, *, held: bool) -> None:
        self._contain(
            "outbox_relay_lock_held",
            lambda: self._outbox_relay_lock_held.set(1 if held else 0),
        )

    def set_outbox_backlog(
        self, *, size: int, oldest_age_seconds: float | None
    ) -> None:
        def _do() -> None:
            self._outbox_backlog_size.set(size)
            self._outbox_backlog_age_seconds.set(
                oldest_age_seconds if oldest_age_seconds is not None else 0.0
            )

        self._contain("outbox_backlog", _do)

    def mark_outbox_backlog_collection_success(
        self, *, at_epoch_seconds: float
    ) -> None:
        self._contain(
            "outbox_backlog_collection_success",
            lambda: self._outbox_backlog_collection_success_timestamp_seconds.set(
                at_epoch_seconds
            ),
        )

    # -- Kafka consumer --------------------------------------------------------

    def observe_consumer_message(self, *, outcome: str) -> None:
        o = _bounded(outcome, _CONSUMER_MESSAGE_OUTCOMES)
        self._contain(
            "consumer_message",
            lambda: self._consumer_messages_total.labels(outcome=o).inc(),
        )

    def observe_consumer_retry_attempt(self, *, stage: str) -> None:
        s = _bounded(stage, _CONSUMER_RETRY_STAGES)
        self._contain(
            "consumer_retry_attempt",
            lambda: self._consumer_retry_attempts_total.labels(stage=s).inc(),
        )

    def observe_consumer_offset_commit(self, *, outcome: str) -> None:
        o = _bounded(outcome, _CONSUMER_OFFSET_COMMIT_OUTCOMES)
        self._contain(
            "consumer_offset_commit",
            lambda: self._consumer_offset_commit_outcomes_total.labels(outcome=o).inc(),
        )

    def observe_consumer_dead_letter(self, *, failure_code: str) -> None:
        f = _bounded(failure_code, _CONSUMER_DEAD_LETTER_FAILURE_CODES)
        self._contain(
            "consumer_dead_letter",
            lambda: self._consumer_dead_letters_total.labels(failure_code=f).inc(),
        )

    # -- Recovery policy -------------------------------------------------------

    def observe_recovery_decision(self, *, action: str, failure_category: str) -> None:
        """Count one freshly-persisted (non-replayed) ``PolicyDecision``.

        Callers must observe this only after the ``insert_policy_decision``
        transaction that authoritatively persisted it has committed, and
        only when that insert actually created a new row -- never on an
        idempotent replay of an already-decided fingerprint.
        """
        a = _bounded(action, _RECOVERY_ACTIONS)
        c = _bounded(failure_category, _FAILURE_CATEGORIES)
        self._contain(
            "recovery_decision",
            lambda: self._recovery_decisions_total.labels(
                action=a, failure_category=c
            ).inc(),
        )

    # -- Redis coordination ----------------------------------------------------

    def observe_redis_rate_limit_decision(self, *, outcome: str) -> None:
        o = _bounded(outcome, _RATE_LIMIT_OUTCOMES)
        self._contain(
            "redis_rate_limit_decision",
            lambda: self._redis_rate_limit_decisions_total.labels(outcome=o).inc(),
        )

    def observe_heartbeat_write(self, *, outcome: str) -> None:
        o = _bounded(outcome, _HEARTBEAT_OUTCOMES)
        self._contain(
            "heartbeat_write",
            lambda: self._worker_heartbeat_writes_total.labels(outcome=o).inc(),
        )

    def mark_heartbeat_last_success(self, *, at_epoch_seconds: float) -> None:
        self._contain(
            "heartbeat_last_success",
            lambda: self._worker_heartbeat_last_success_timestamp_seconds.set(
                at_epoch_seconds
            ),
        )

    # -- Database readiness ----------------------------------------------------

    def observe_database_readiness_failure(self) -> None:
        self._contain(
            "database_readiness_failure",
            self._database_readiness_failures_total.inc,
        )

    def observe_langsmith_operation(self, *, operation: str, outcome: str) -> None:
        o = _bounded(operation, _LANGSMITH_OPERATIONS)
        c = _bounded(outcome, _LANGSMITH_OUTCOMES)
        self._contain(
            "langsmith_operation",
            lambda: self._langsmith_operations_total.labels(
                operation=o, outcome=c
            ).inc(),
        )


_default_instance: AtlasMetrics | None = None


def default_metrics() -> AtlasMetrics:
    """Return the process-wide :class:`AtlasMetrics` singleton.

    Bound to ``prometheus_client.REGISTRY`` -- the same default registry
    ``prometheus_client.generate_latest()`` reads with no arguments -- so
    the API's ``/metrics`` route and each non-API role's internal metrics
    HTTP server can render it without threading the instance through
    every call site by hand. Constructed lazily (not at import time) so
    importing this module never has the side effect of registering
    metrics against the global registry.
    """
    global _default_instance
    if _default_instance is None:
        _default_instance = AtlasMetrics(REGISTRY)
    return _default_instance
