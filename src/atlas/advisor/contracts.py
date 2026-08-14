"""Typed advisory input/output contracts (Slice 15C2).

Model-visible facts are a discriminated union. Extra fields are forbidden.
Unsupported stored strings never appear on these models.
"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from atlas.advisor.catalogs import (
    ANALYSIS_SCHEMA_VERSION,
    FACTS_VERSION,
    JOB_ID_PATTERN,
    MAX_MISSING_SOURCES,
    MAX_RESEARCH_JOB_ID_LENGTH,
    MAX_SIGNALS,
    SIGNAL_ID_PATTERN,
    MissingSourceCode,
)

_SIGNAL_ID_FIELD = Field(pattern=SIGNAL_ID_PATTERN)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class JobStatusSignal(_Strict):
    signal_type: Literal["job.status"] = "job.status"
    signal_id: str = _SIGNAL_ID_FIELD
    status: Literal["PENDING", "RUNNING", "AWAITING_REVIEW", "COMPLETED", "FAILED"]


class JobEvaluationProfileSignal(_Strict):
    signal_type: Literal["job.evaluation_profile"] = "job.evaluation_profile"
    signal_id: str = _SIGNAL_ID_FIELD
    profile: Literal[
        "evaluation.v1",
        "evaluation.candidate.v1",
        "evaluation.candidate.fake.v1",
    ]


class JobContinuationModeSignal(_Strict):
    signal_type: Literal["job.continuation_mode"] = "job.continuation_mode"
    signal_id: str = _SIGNAL_ID_FIELD
    mode: Literal["NONE", "JOB_RETRY", "REVIEW_COMPLETE"]


class JobRepairCountSignal(_Strict):
    signal_type: Literal["job.repair_count"] = "job.repair_count"
    signal_id: str = _SIGNAL_ID_FIELD
    count: Annotated[int, Field(ge=0, le=1_000_000)]


class JobRetryCountSignal(_Strict):
    signal_type: Literal["job.job_retry_count"] = "job.job_retry_count"
    signal_id: str = _SIGNAL_ID_FIELD
    count: Annotated[int, Field(ge=0, le=1_000_000)]


class JobEvaluationAttemptCountSignal(_Strict):
    signal_type: Literal["job.evaluation_attempt_count"] = (
        "job.evaluation_attempt_count"
    )
    signal_id: str = _SIGNAL_ID_FIELD
    count: Annotated[int, Field(ge=0, le=1_000_000)]


class WorkflowNodeOutcomeSignal(_Strict):
    signal_type: Literal["workflow_node.outcome"] = "workflow_node.outcome"
    signal_id: str = _SIGNAL_ID_FIELD
    node_name: Literal[
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
    ]
    attempt: Annotated[int, Field(ge=1, le=1_000_000)]
    status: Literal["STARTED", "COMPLETED", "FAILED"]
    error_class: Annotated[str, Field(min_length=1, max_length=128)] | None = None

    @field_validator("error_class")
    @classmethod
    def closed_error_class(cls, value: str | None) -> str | None:
        from atlas.advisor.catalogs import ADVISORY_ERROR_CLASSES

        if value is None:
            return None
        if value not in ADVISORY_ERROR_CLASSES:
            raise ValueError("error_class must be a closed catalog value")
        return value


class ModelOutcomeCountSignal(_Strict):
    signal_type: Literal["model.outcome_count"] = "model.outcome_count"
    signal_id: str = _SIGNAL_ID_FIELD
    node_name: Literal["plan", "draft", "evaluate"]
    provider: Literal["fake", "openai", "anthropic"]
    status: Literal["IN_PROGRESS", "SUCCEEDED", "FAILED"]
    count: Annotated[int, Field(ge=1, le=1_000_000)]
    retry_class: (
        Literal[
            "timeout",
            "rate_limited",
            "temporary",
            "auth_config",
            "invalid_request",
            "invalid_structured_output",
            "refusal",
            "unknown",
            "none",
        ]
        | None
    ) = None
    error_class: Annotated[str, Field(min_length=1, max_length=128)] | None = None

    @field_validator("error_class")
    @classmethod
    def closed_model_error_class(cls, value: str | None) -> str | None:
        from atlas.advisor.catalogs import ADVISORY_ERROR_CLASSES

        if value is None:
            return None
        if value not in ADVISORY_ERROR_CLASSES:
            raise ValueError("error_class must be a closed catalog value")
        return value


class ToolOutcomeCountSignal(_Strict):
    signal_type: Literal["tool.outcome_count"] = "tool.outcome_count"
    signal_id: str = _SIGNAL_ID_FIELD
    tool_id: Literal["web_search", "fetch_url"]
    provider: Literal["fake", "tavily", "httpx"]
    status: Literal["IN_PROGRESS", "SUCCEEDED", "FAILED"]
    count: Annotated[int, Field(ge=1, le=1_000_000)]
    retry_class: (
        Literal[
            "timeout",
            "rate_limited",
            "temporary",
            "auth_config",
            "invalid_request",
            "permission_denied",
            "ssrf_blocked",
            "content_rejected",
            "budget_exhausted",
            "unknown",
            "none",
        ]
        | None
    ) = None


class EvaluationRunSignal(_Strict):
    signal_type: Literal["evaluation.run"] = "evaluation.run"
    signal_id: str = _SIGNAL_ID_FIELD
    status: Literal["IN_PROGRESS", "SUCCEEDED", "FAILED"]
    profile: Literal[
        "evaluation.v1",
        "evaluation.candidate.v1",
        "evaluation.candidate.fake.v1",
    ]
    passed: bool | None = None
    aggregate_score: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    disposition_hint: (
        Literal["complete", "terminal", "repair", "await_review", "retry"] | None
    ) = None

    @model_validator(mode="after")
    def require_succeeded_fields(self) -> EvaluationRunSignal:
        if self.status == "SUCCEEDED" and (
            self.passed is None or self.aggregate_score is None
        ):
            raise ValueError("succeeded evaluation.run requires passed and score")
        return self


class EvaluationDimensionSignal(_Strict):
    signal_type: Literal["evaluation.dimension"] = "evaluation.dimension"
    signal_id: str = _SIGNAL_ID_FIELD
    name: Literal[
        "citation_integrity",
        "tool_use",
        "report_structure",
        "coverage",
        "completeness",
        "lexical_id_groundedness",
        "semantic_groundedness",
    ]
    score: Annotated[float, Field(ge=0.0, le=1.0)]
    passed: bool
    method: Literal["deterministic", "llm", "skipped"]
    failure_codes: Annotated[list[str], Field(max_length=8)] = Field(
        default_factory=list
    )

    @field_validator("failure_codes")
    @classmethod
    def closed_failure_codes(cls, value: list[str]) -> list[str]:
        from atlas.advisor.catalogs import ADVISORY_FAILURE_CODES

        for code in value:
            if code not in ADVISORY_FAILURE_CODES:
                raise ValueError("failure_codes must be closed catalog values")
        return value


class RecoveryDecisionSignal(_Strict):
    signal_type: Literal["recovery.decision"] = "recovery.decision"
    signal_id: str = _SIGNAL_ID_FIELD
    decision: Literal["complete", "repair", "await_review", "retry", "terminal"]
    failure_category: Literal[
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
    ]
    reason_code: Literal[
        "EVALUATION_PASSED",
        "EVALUATION_ATTEMPT_CAP",
        "HARD_QUALITY_FAIL",
        "STRUCTURE_REPAIR",
        "STRUCTURE_REPAIR_EXHAUSTED",
        "SOFT_QUALITY_REPAIR",
        "REPAIR_EXHAUSTED",
        "AMBIGUOUS_QUALITY",
        "OWNERSHIP_CONFLICT",
        "UNCLASSIFIED_TERMINAL",
        "TRANSIENT_RETRY",
        "RETRY_CAP",
        "PERMANENT_FAIL",
    ]
    attempt_number: Annotated[int, Field(ge=1, le=1_000_000)] | None = None

    @model_validator(mode="after")
    def attempt_number_matches_decision(self) -> Self:
        if self.decision == "retry":
            if self.attempt_number is None:
                raise ValueError("retry decisions require attempt_number")
        elif self.attempt_number is not None:
            raise ValueError("attempt_number is only allowed for retry decisions")
        return self


class ReviewDecisionCountSignal(_Strict):
    signal_type: Literal["review.decision_count"] = "review.decision_count"
    signal_id: str = _SIGNAL_ID_FIELD
    decision: Literal["approve", "reject"]
    count: Annotated[int, Field(ge=1, le=1_000_000)]


class OutboxSummarySignal(_Strict):
    signal_type: Literal["outbox.summary"] = "outbox.summary"
    signal_id: str = _SIGNAL_ID_FIELD
    event_type: Literal[
        "research_job.created",
        "research_job.completed",
        "research_job.failed",
        "research_job.awaiting_review",
        "research_job.retry_scheduled",
    ]
    unpublished_count: Annotated[int, Field(ge=0, le=1_000_000)]
    published_count: Annotated[int, Field(ge=0, le=1_000_000)]
    max_publish_attempts: Annotated[int, Field(ge=0, le=1_000_000)]
    last_publish_error_class: (
        Annotated[str, Field(min_length=1, max_length=128)] | None
    ) = None

    @field_validator("last_publish_error_class")
    @classmethod
    def closed_publish_error_class(cls, value: str | None) -> str | None:
        from atlas.advisor.catalogs import ADVISORY_ERROR_CLASSES

        if value is None:
            return None
        if value not in ADVISORY_ERROR_CLASSES:
            raise ValueError("error_class must be a closed catalog value")
        return value


class ConsumerProjectionSignal(_Strict):
    signal_type: Literal["consumer.projection"] = "consumer.projection"
    signal_id: str = _SIGNAL_ID_FIELD
    last_event_type: Literal[
        "research_job.created",
        "research_job.completed",
        "research_job.failed",
        "research_job.awaiting_review",
        "research_job.retry_scheduled",
    ]


class ConsumerDeadLetterSignal(_Strict):
    signal_type: Literal["consumer.dead_letter"] = "consumer.dead_letter"
    signal_id: str = _SIGNAL_ID_FIELD
    failure_code: Literal[
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
    ]
    replay_state: Literal[
        "PENDING",
        "REPLAYING",
        "REPLAY_FAILED",
        "REPLAYED_APPLIED",
        "REPLAYED_DUPLICATE",
    ]
    replay_eligible: bool
    count: Annotated[int, Field(ge=1, le=1_000_000)]


class AlertNameSignal(_Strict):
    """Test-port facts only. The production DB assembler never emits this type."""

    signal_type: Literal["alert.name"] = "alert.name"
    signal_id: str = _SIGNAL_ID_FIELD
    alert_name: Literal[
        "AtlasHighHttpErrorRatio",
        "AtlasWorkerHeartbeatStale",
        "AtlasOutboxBacklogGrowing",
        "AtlasScrapeTargetDown",
    ]


AdvisorySignal = Annotated[
    JobStatusSignal
    | JobEvaluationProfileSignal
    | JobContinuationModeSignal
    | JobRepairCountSignal
    | JobRetryCountSignal
    | JobEvaluationAttemptCountSignal
    | WorkflowNodeOutcomeSignal
    | ModelOutcomeCountSignal
    | ToolOutcomeCountSignal
    | EvaluationRunSignal
    | EvaluationDimensionSignal
    | RecoveryDecisionSignal
    | ReviewDecisionCountSignal
    | OutboxSummarySignal
    | ConsumerProjectionSignal
    | ConsumerDeadLetterSignal
    | AlertNameSignal,
    Field(discriminator="signal_type"),
]


class AdvisoryIncidentFacts(_Strict):
    """Deterministic model-visible incident facts. No analysis_id or timestamps."""

    facts_version: Literal["advisory.incident.v1"] = FACTS_VERSION
    research_job_id: Annotated[
        str, Field(min_length=1, max_length=MAX_RESEARCH_JOB_ID_LENGTH)
    ]
    signals: Annotated[
        list[AdvisorySignal], Field(min_length=1, max_length=MAX_SIGNALS)
    ]
    missing_sources: Annotated[
        list[MissingSourceCode], Field(max_length=MAX_MISSING_SOURCES)
    ] = Field(default_factory=list)

    @field_validator("research_job_id")
    @classmethod
    def validate_job_id(cls, value: str) -> str:
        import re

        cleaned = value.strip()
        if not re.fullmatch(JOB_ID_PATTERN, cleaned):
            raise ValueError("research_job_id charset rejected")
        return cleaned

    @field_validator("missing_sources")
    @classmethod
    def sort_missing(cls, value: list[MissingSourceCode]) -> list[MissingSourceCode]:
        return sorted(set(value))


class AdvisoryHypothesis(_Strict):
    statement: Annotated[str, Field(min_length=1, max_length=300)]
    likelihood: Literal["low", "medium", "high"]
    signal_ids: Annotated[list[str], Field(min_length=1, max_length=8)]


class AdvisoryRecommendation(_Strict):
    step: Annotated[str, Field(min_length=1, max_length=300)]
    action_kind: Literal[
        "investigate",
        "inspect_state",
        "review_runbook",
        "collect_more_telemetry",
    ]
    signal_ids: Annotated[list[str], Field(min_length=1, max_length=8)]


class AdvisoryAnalysis(_Strict):
    schema_version: Literal["advisory.analysis.v1"] = ANALYSIS_SCHEMA_VERSION
    incident_summary: Annotated[str, Field(min_length=1, max_length=500)]
    hypotheses: Annotated[list[AdvisoryHypothesis], Field(min_length=1, max_length=5)]
    recommendations: Annotated[
        list[AdvisoryRecommendation], Field(min_length=1, max_length=5)
    ]
    confidence: Literal["low", "medium", "high"]
    limitations: Annotated[list[str], Field(max_length=8)] = Field(default_factory=list)
    unknowns: Annotated[list[str], Field(max_length=8)] = Field(default_factory=list)

    @field_validator("limitations", "unknowns")
    @classmethod
    def bound_optional_strings(cls, value: list[str]) -> list[str]:
        for item in value:
            if not (1 <= len(item) <= 200):
                raise ValueError("optional text exceeds bound")
        return value


class AdvisoryStdoutEnvelope(_Strict):
    """Operator stdout only. Never sent to the model."""

    analysis_id: Annotated[str, Field(min_length=36, max_length=36)]
    research_job_id: Annotated[
        str, Field(min_length=1, max_length=MAX_RESEARCH_JOB_ID_LENGTH)
    ]
    analysis: AdvisoryAnalysis
