"""Closed catalogs for advisory snapshot facts (Slice 15C2).

Unsupported stored strings are omitted from model-visible facts. The
assembler records a closed ``missing_sources`` code and never copies the
raw value into the snapshot, prompt, logs, or LangSmith metadata.
"""

from __future__ import annotations

from typing import Final, Literal

from atlas.consumer.errors import ALLOWED_FAILURE_CODES
from atlas.evaluation.contracts import ALLOWED_EVALUATION_PROFILES
from atlas.eventing.contracts import SUPPORTED_EVENT_TYPES
from atlas.models.contracts import ProviderId, RetryClass
from atlas.recovery.policy import FailureCategory
from atlas.tools.contracts import ToolId, ToolProviderId, ToolRetryClass

FACTS_VERSION: Literal["advisory.incident.v1"] = "advisory.incident.v1"
ANALYSIS_SCHEMA_VERSION: Literal["advisory.analysis.v1"] = "advisory.analysis.v1"
ADVISORY_PROMPT_VERSION: Literal["advisory.incident.v1"] = "advisory.incident.v1"
ADVISORY_IDENTITY: Literal["advisory.ops.v1"] = "advisory.ops.v1"
ADVISORY_FAKE_IDENTITY: Literal["advisory.fake.v1"] = "advisory.fake.v1"
ADVISORY_NODE_NAME: Literal["advise"] = "advise"
FROZEN_LIVE_ADVISORY_PROVIDER: Literal["openai"] = "openai"
FROZEN_LIVE_ADVISORY_MODEL: Literal["gpt-4o-mini"] = "gpt-4o-mini"
FROZEN_LIVE_ADVISORY_TEMPERATURE: float = 0.0

MAX_SIGNALS: Final[int] = 64
MAX_WORKFLOW_NODES: Final[int] = 32
MAX_MODEL_GROUPS: Final[int] = 12
MAX_TOOL_GROUPS: Final[int] = 8
MAX_EVALUATION_DIMENSIONS: Final[int] = 7
MAX_RECOVERY_DECISIONS: Final[int] = 8
MAX_OUTBOX_GROUPS: Final[int] = 8
MAX_DEAD_LETTERS: Final[int] = 8
MAX_FAILURE_CODES_PER_DIMENSION: Final[int] = 8
MAX_MISSING_SOURCES: Final[int] = 16
MAX_FACTS_UTF8_BYTES: Final[int] = 12_288
MAX_USER_PROMPT_UTF8_BYTES: Final[int] = 12_288
MAX_OUTPUT_UTF8_BYTES: Final[int] = 8_192
MAX_RESEARCH_JOB_ID_LENGTH: Final[int] = 128
ADVISORY_ANALYSIS_DEADLINE_MARGIN_SECONDS: Final[float] = 10.0
MALFORMED_ATTEMPT_CAP: Final[int] = 2

JOB_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._:-]+$"
SIGNAL_ID_PATTERN: Final[str] = r"^sig:(0[1-9]|[1-5][0-9]|6[0-4])$"
NODE_ERROR_PATTERN: Final[str] = (
    r"^([A-Za-z][A-Za-z0-9_]{0,127}): node execution failed$"
)

JOB_STATUSES: Final[frozenset[str]] = frozenset(
    {"PENDING", "RUNNING", "AWAITING_REVIEW", "COMPLETED", "FAILED"}
)
CONTINUATION_MODES: Final[frozenset[str]] = frozenset(
    {"NONE", "JOB_RETRY", "REVIEW_COMPLETE"}
)
WORKFLOW_NODE_NAMES: Final[frozenset[str]] = frozenset(
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
    }
)
WORKFLOW_NODE_STATUSES: Final[frozenset[str]] = frozenset(
    {"STARTED", "COMPLETED", "FAILED"}
)
MODEL_NODE_NAMES: Final[frozenset[str]] = frozenset({"plan", "draft", "evaluate"})
MODEL_PROVIDERS: Final[frozenset[str]] = frozenset(item.value for item in ProviderId)
INVOCATION_STATUSES: Final[frozenset[str]] = frozenset(
    {"IN_PROGRESS", "SUCCEEDED", "FAILED"}
)
RETRY_CLASSES: Final[frozenset[str]] = frozenset(item.value for item in RetryClass)
TOOL_IDS: Final[frozenset[str]] = frozenset(item.value for item in ToolId)
TOOL_PROVIDERS: Final[frozenset[str]] = frozenset(item.value for item in ToolProviderId)
TOOL_RETRY_CLASSES: Final[frozenset[str]] = frozenset(
    item.value for item in ToolRetryClass
)
EVALUATION_PROFILES: Final[frozenset[str]] = frozenset(ALLOWED_EVALUATION_PROFILES)
EVALUATION_RUN_STATUSES: Final[frozenset[str]] = frozenset(
    {"IN_PROGRESS", "SUCCEEDED", "FAILED"}
)
DISPOSITION_HINTS: Final[frozenset[str]] = frozenset(
    {"complete", "terminal", "repair", "await_review", "retry"}
)
GRADER_METHODS: Final[frozenset[str]] = frozenset({"deterministic", "llm", "skipped"})
DIMENSION_NAMES: Final[frozenset[str]] = frozenset(
    {
        "citation_integrity",
        "tool_use",
        "report_structure",
        "coverage",
        "completeness",
        "lexical_id_groundedness",
        "semantic_groundedness",
    }
)
RECOVERY_DECISIONS: Final[frozenset[str]] = frozenset(
    {"complete", "repair", "await_review", "retry", "terminal"}
)
FAILURE_CATEGORIES: Final[frozenset[str]] = frozenset(
    item.value for item in FailureCategory
)
REVIEW_DECISIONS: Final[frozenset[str]] = frozenset({"approve", "reject"})
EVENT_TYPES: Final[frozenset[str]] = frozenset(SUPPORTED_EVENT_TYPES)
CONSUMER_FAILURE_CODES: Final[frozenset[str]] = frozenset(ALLOWED_FAILURE_CODES)
REPLAY_STATES: Final[frozenset[str]] = frozenset(
    {
        "PENDING",
        "REPLAYING",
        "REPLAY_FAILED",
        "REPLAYED_APPLIED",
        "REPLAYED_DUPLICATE",
    }
)
ALERT_NAMES: Final[frozenset[str]] = frozenset(
    {
        "AtlasHighHttpErrorRatio",
        "AtlasWorkerHeartbeatStale",
        "AtlasOutboxBacklogGrowing",
        "AtlasScrapeTargetDown",
    }
)

ADVISORY_FAILURE_CODES: Final[frozenset[str]] = frozenset(
    {
        "CITATION_PROVENANCE_INCOMPLETE",
        "CITATION_EMPTY_CLAIM",
        "CITATION_UNLINKED",
        "TOOL_REQUIRED",
        "TOOL_UNKNOWN_ORIGIN",
        "TOOL_NODE_VIOLATION",
        "TOOL_BUDGET_EXCEEDED",
        "STRUCTURE_EMPTY_DRAFT",
        "STRUCTURE_MISSING_SECTION",
        "STRUCTURE_EMPTY_PLAN",
        "COVERAGE_FACET_MISSING",
        "COVERAGE_BELOW_MIN",
        "COMPLETENESS_FACET_MISSING",
        "GROUNDEDNESS_ID_OUTSIDE_LINKS",
        "SEMANTIC_UNSUPPORTED",
        "SEMANTIC_UNCLEAR",
        "SEMANTIC_GROUNDEDNESS_WEAK",
    }
)
ADVISORY_REASON_CODES: Final[frozenset[str]] = frozenset(
    {
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
    }
)
ADVISORY_ERROR_CLASSES: Final[frozenset[str]] = frozenset(
    {
        "ModelTimeoutError",
        "ModelRateLimitedError",
        "ModelTemporaryError",
        "ModelAuthConfigError",
        "ModelInvalidRequestError",
        "ModelInvalidStructuredOutputError",
        "ModelRefusalError",
        "ModelUnknownError",
        "ModelInvocationInProgressError",
        "ModelAttemptOwnershipLostError",
        "ModelInvocationStaleError",
        "ToolTimeoutError",
        "ToolRateLimitedError",
        "ToolTemporaryError",
        "ToolAuthConfigError",
        "ToolInvalidRequestError",
        "ToolPermissionDeniedError",
        "ToolSsrfBlockedError",
        "ToolContentRejectedError",
        "ToolBudgetExhaustedError",
        "ToolUnknownError",
        "ToolInvocationInProgressError",
        "ToolAttemptOwnershipLostError",
        "ToolInvocationStaleError",
        "EvaluationValidationError",
        "EvaluationNotFoundError",
        "EvaluationConflictError",
        "EvaluationInProgressError",
        "EvaluationOwnershipLostError",
        "EvaluationTerminalError",
        "EvaluationAttemptCapError",
        "EvaluationStaleError",
        "EvaluationUnexpectedError",
        "EvaluationProfileMismatchError",
        "SemanticGraderConfigurationError",
        "PublishError",
        "EarlierEventPublishFailure",
        "EarlierEventOwnershipLost",
        "KafkaPublishError",
        "KafkaPublishTimeoutError",
        "KafkaFatalProducerError",
        "KafkaProducerConfigurationError",
        "EventPublishError",
        "FatalEventPublishError",
        "RelayOwnershipError",
        "RelayNotOwnerError",
    }
)

MissingSourceCode = Literal[
    "unknown_signal_dropped",
    "unsupported_evaluation_profile",
    "evaluation_profile_absent",
    "unsupported_continuation_mode",
    "unsupported_node_name",
    "unsupported_node_status",
    "unsupported_error_class",
    "unsupported_node_error",
    "unsupported_model_node",
    "unsupported_model_provider",
    "unsupported_model_status",
    "unsupported_retry_class",
    "unsupported_tool_id",
    "unsupported_tool_provider",
    "unsupported_tool_status",
    "unsupported_evaluation_status",
    "unsupported_disposition_hint",
    "unsupported_dimension_name",
    "unsupported_grader_method",
    "unsupported_failure_code",
    "unsupported_recovery_decision",
    "unsupported_failure_category",
    "unsupported_reason_code",
    "unsupported_review_decision",
    "unsupported_event_type",
    "unsupported_consumer_failure_code",
    "unsupported_replay_state",
    "unsupported_alert_name",
    "evaluation_absent",
    "evaluation_incomplete",
    "outbox_absent",
    "consumer_absent",
    "workflow_absent",
    "recovery_attempt_absent",
]

MISSING_SOURCE_CODES: Final[frozenset[str]] = frozenset(
    {
        "unknown_signal_dropped",
        "unsupported_evaluation_profile",
        "evaluation_profile_absent",
        "unsupported_continuation_mode",
        "unsupported_node_name",
        "unsupported_node_status",
        "unsupported_error_class",
        "unsupported_node_error",
        "unsupported_model_node",
        "unsupported_model_provider",
        "unsupported_model_status",
        "unsupported_retry_class",
        "unsupported_tool_id",
        "unsupported_tool_provider",
        "unsupported_tool_status",
        "unsupported_evaluation_status",
        "unsupported_disposition_hint",
        "unsupported_dimension_name",
        "unsupported_grader_method",
        "unsupported_failure_code",
        "unsupported_recovery_decision",
        "unsupported_failure_category",
        "unsupported_reason_code",
        "unsupported_review_decision",
        "unsupported_event_type",
        "unsupported_consumer_failure_code",
        "unsupported_replay_state",
        "unsupported_alert_name",
        "evaluation_absent",
        "evaluation_incomplete",
        "outbox_absent",
        "consumer_absent",
        "workflow_absent",
        "recovery_attempt_absent",
    }
)
