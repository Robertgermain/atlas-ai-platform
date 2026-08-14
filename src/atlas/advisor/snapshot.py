"""Deterministic assembly of closed advisory facts (Slice 15C2).

Unsupported stored strings are omitted. The raw value is never copied into
facts, prompts, logs, or metadata.

Validated signals that exceed ``MAX_SIGNALS`` fail closed with
``AdvisorySnapshotRejectedError``. The assembler never silently truncates
evidence after sorting.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from atlas.advisor.catalogs import (
    ADVISORY_ERROR_CLASSES,
    ADVISORY_FAILURE_CODES,
    ADVISORY_REASON_CODES,
    ALERT_NAMES,
    CONSUMER_FAILURE_CODES,
    CONTINUATION_MODES,
    DIMENSION_NAMES,
    DISPOSITION_HINTS,
    EVALUATION_PROFILES,
    EVALUATION_RUN_STATUSES,
    EVENT_TYPES,
    FAILURE_CATEGORIES,
    GRADER_METHODS,
    INVOCATION_STATUSES,
    JOB_STATUSES,
    MAX_DEAD_LETTERS,
    MAX_EVALUATION_DIMENSIONS,
    MAX_FACTS_UTF8_BYTES,
    MAX_FAILURE_CODES_PER_DIMENSION,
    MAX_MISSING_SOURCES,
    MAX_MODEL_GROUPS,
    MAX_OUTBOX_GROUPS,
    MAX_RECOVERY_DECISIONS,
    MAX_SIGNALS,
    MAX_TOOL_GROUPS,
    MAX_WORKFLOW_NODES,
    MISSING_SOURCE_CODES,
    MODEL_NODE_NAMES,
    MODEL_PROVIDERS,
    NODE_ERROR_PATTERN,
    RECOVERY_DECISIONS,
    REPLAY_STATES,
    RETRY_CLASSES,
    REVIEW_DECISIONS,
    TOOL_IDS,
    TOOL_PROVIDERS,
    TOOL_RETRY_CLASSES,
    WORKFLOW_NODE_NAMES,
    WORKFLOW_NODE_STATUSES,
    MissingSourceCode,
)
from atlas.advisor.contracts import (
    AdvisoryIncidentFacts,
    AdvisorySignal,
    AlertNameSignal,
    ConsumerDeadLetterSignal,
    ConsumerProjectionSignal,
    EvaluationDimensionSignal,
    EvaluationRunSignal,
    JobContinuationModeSignal,
    JobEvaluationAttemptCountSignal,
    JobEvaluationProfileSignal,
    JobRepairCountSignal,
    JobRetryCountSignal,
    JobStatusSignal,
    ModelOutcomeCountSignal,
    OutboxSummarySignal,
    RecoveryDecisionSignal,
    ReviewDecisionCountSignal,
    ToolOutcomeCountSignal,
    WorkflowNodeOutcomeSignal,
)
from atlas.advisor.errors import AdvisorySnapshotRejectedError

_NODE_ERROR_RE = re.compile(NODE_ERROR_PATTERN)


@dataclass(frozen=True, slots=True)
class JobRow:
    research_job_id: str
    status: str
    evaluation_profile: str | None
    continuation_mode: str
    repair_count: int
    job_retry_count: int
    evaluation_attempt_count: int


@dataclass(frozen=True, slots=True)
class NodeRow:
    node_name: str
    attempt: int
    status: str
    error: str | None


@dataclass(frozen=True, slots=True)
class CountGroup:
    keys: tuple[str, ...]
    count: int


@dataclass(frozen=True, slots=True)
class EvaluationRunRow:
    status: str
    profile: str
    passed: bool | None
    aggregate_score: float | None
    disposition_hint: str | None


@dataclass(frozen=True, slots=True)
class DimensionRow:
    name: str
    score: float
    passed: bool
    method: str
    failure_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RecoveryRow:
    decision: str
    failure_category: str
    reason_code: str
    attempt_number: int | None


@dataclass(frozen=True, slots=True)
class OutboxRow:
    event_type: str
    unpublished_count: int
    published_count: int
    max_publish_attempts: int
    last_publish_error_class: str | None


@dataclass(frozen=True, slots=True)
class DeadLetterRow:
    failure_code: str
    replay_state: str
    replay_eligible: bool
    count: int


@dataclass(frozen=True, slots=True)
class SnapshotLoad:
    """Approved columns loaded for one job. No question/result/payload text."""

    job: JobRow
    nodes: tuple[NodeRow, ...] = ()
    model_groups: tuple[CountGroup, ...] = ()
    tool_groups: tuple[CountGroup, ...] = ()
    evaluation_run: EvaluationRunRow | None = None
    dimensions: tuple[DimensionRow, ...] = ()
    recovery: tuple[RecoveryRow, ...] = ()
    review_counts: tuple[tuple[str, int], ...] = ()
    outbox: tuple[OutboxRow, ...] = ()
    projection_event_type: str | None = None
    dead_letters: tuple[DeadLetterRow, ...] = ()
    alert_names: tuple[str, ...] = ()


def canonical_facts_json(facts: AdvisoryIncidentFacts) -> str:
    """Deterministic UTF-8 JSON of model-visible facts."""
    payload = facts.model_dump(mode="json", exclude_none=True)
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def facts_fingerprint(facts: AdvisoryIncidentFacts) -> str:
    """Internal SHA-256 of canonical facts. Never sent to the model or metadata."""
    return hashlib.sha256(canonical_facts_json(facts).encode("utf-8")).hexdigest()


def assemble_facts(loaded: SnapshotLoad) -> AdvisoryIncidentFacts:
    """Map approved rows to closed signals. Unknown strings become missing_sources."""
    missing: list[MissingSourceCode] = []
    pending: list[AdvisorySignal] = []

    _add_job_signals(loaded.job, pending, missing)
    _add_node_signals(loaded.nodes, pending, missing)
    _add_model_signals(loaded.model_groups, pending, missing)
    _add_tool_signals(loaded.tool_groups, pending, missing)
    _add_evaluation_signals(loaded.evaluation_run, loaded.dimensions, pending, missing)
    _add_recovery_signals(loaded.recovery, pending, missing)
    _add_review_signals(loaded.review_counts, pending, missing)
    _add_outbox_signals(loaded.outbox, pending, missing)
    _add_consumer_signals(
        loaded.projection_event_type, loaded.dead_letters, pending, missing
    )
    _add_alert_signals(loaded.alert_names, pending, missing)

    pending.sort(key=_signal_sort_key)
    if len(pending) > MAX_SIGNALS:
        raise AdvisorySnapshotRejectedError("advisory snapshot exceeds signal bound")
    assigned: list[AdvisorySignal] = []
    for index, item in enumerate(pending, start=1):
        assigned.append(item.model_copy(update={"signal_id": f"sig:{index:02d}"}))

    unique_missing = sorted(set(missing))[:MAX_MISSING_SOURCES]
    facts = AdvisoryIncidentFacts(
        research_job_id=loaded.job.research_job_id,
        signals=assigned,
        missing_sources=unique_missing,
    )
    encoded = canonical_facts_json(facts)
    if len(encoded.encode("utf-8")) > MAX_FACTS_UTF8_BYTES:
        raise AdvisorySnapshotRejectedError("advisory snapshot exceeds byte bound")
    return facts


def _note(missing: list[MissingSourceCode], code: str) -> None:
    if code in MISSING_SOURCE_CODES and code not in missing:
        missing.append(code)  # type: ignore[arg-type]


def _allow(
    value: str | None,
    allowed: frozenset[str],
    missing: list[MissingSourceCode],
    code: str,
) -> str | None:
    if value is None:
        return None
    if value in allowed:
        return value
    _note(missing, code)
    return None


def _signal_sort_key(signal: AdvisorySignal) -> tuple[str, ...]:
    dumped = signal.model_dump(mode="json", exclude_none=True)
    dumped.pop("signal_id", None)
    return tuple(f"{key}={dumped[key]}" for key in sorted(dumped))


def _add_job_signals(
    job: JobRow,
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    status = _allow(job.status, JOB_STATUSES, missing, "unknown_signal_dropped")
    if status is None:
        raise AdvisorySnapshotRejectedError("job status is unsupported")
    pending.append(JobStatusSignal(signal_id="sig:01", status=status))  # type: ignore[arg-type]
    if job.evaluation_profile is None:
        _note(missing, "evaluation_profile_absent")
    else:
        profile = _allow(
            job.evaluation_profile,
            EVALUATION_PROFILES,
            missing,
            "unsupported_evaluation_profile",
        )
        if profile is not None:
            pending.append(
                JobEvaluationProfileSignal(signal_id="sig:01", profile=profile)  # type: ignore[arg-type]
            )
    mode = _allow(
        job.continuation_mode,
        CONTINUATION_MODES,
        missing,
        "unsupported_continuation_mode",
    )
    if mode is not None:
        pending.append(
            JobContinuationModeSignal(signal_id="sig:01", mode=mode)  # type: ignore[arg-type]
        )
    pending.append(JobRepairCountSignal(signal_id="sig:01", count=job.repair_count))
    pending.append(JobRetryCountSignal(signal_id="sig:01", count=job.job_retry_count))
    pending.append(
        JobEvaluationAttemptCountSignal(
            signal_id="sig:01", count=job.evaluation_attempt_count
        )
    )


def _add_node_signals(
    nodes: tuple[NodeRow, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    if not nodes:
        _note(missing, "workflow_absent")
        return
    kept: list[WorkflowNodeOutcomeSignal] = []
    for node in nodes:
        name = _allow(
            node.node_name, WORKFLOW_NODE_NAMES, missing, "unsupported_node_name"
        )
        status = _allow(
            node.status, WORKFLOW_NODE_STATUSES, missing, "unsupported_node_status"
        )
        if name is None or status is None:
            continue
        error_class: str | None = None
        if node.error:
            match = _NODE_ERROR_RE.fullmatch(node.error)
            if match is None:
                _note(missing, "unsupported_node_error")
            else:
                error_class = _allow(
                    match.group(1),
                    ADVISORY_ERROR_CLASSES,
                    missing,
                    "unsupported_error_class",
                )
        kept.append(
            WorkflowNodeOutcomeSignal(
                signal_id="sig:01",
                node_name=name,  # type: ignore[arg-type]
                attempt=node.attempt,
                status=status,  # type: ignore[arg-type]
                error_class=error_class,
            )
        )
    kept.sort(key=lambda item: (item.node_name, item.attempt))
    pending.extend(kept[:MAX_WORKFLOW_NODES])


def _add_model_signals(
    groups: tuple[CountGroup, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    kept: list[ModelOutcomeCountSignal] = []
    for group in groups:
        node_name, provider, status, retry_class, error_class = group.keys
        node = _allow(node_name, MODEL_NODE_NAMES, missing, "unsupported_model_node")
        prov = _allow(provider, MODEL_PROVIDERS, missing, "unsupported_model_provider")
        st = _allow(status, INVOCATION_STATUSES, missing, "unsupported_model_status")
        if node is None or prov is None or st is None:
            continue
        retry = None
        if retry_class:
            retry = _allow(
                retry_class, RETRY_CLASSES, missing, "unsupported_retry_class"
            )
        err = None
        if error_class:
            err = _allow(
                error_class,
                ADVISORY_ERROR_CLASSES,
                missing,
                "unsupported_error_class",
            )
        kept.append(
            ModelOutcomeCountSignal(
                signal_id="sig:01",
                node_name=node,  # type: ignore[arg-type]
                provider=prov,  # type: ignore[arg-type]
                status=st,  # type: ignore[arg-type]
                count=group.count,
                retry_class=retry,  # type: ignore[arg-type]
                error_class=err,
            )
        )
    pending.extend(kept[:MAX_MODEL_GROUPS])


def _add_tool_signals(
    groups: tuple[CountGroup, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    kept: list[ToolOutcomeCountSignal] = []
    for group in groups:
        tool_id, provider, status, retry_class = group.keys
        tid = _allow(tool_id, TOOL_IDS, missing, "unsupported_tool_id")
        prov = _allow(provider, TOOL_PROVIDERS, missing, "unsupported_tool_provider")
        st = _allow(status, INVOCATION_STATUSES, missing, "unsupported_tool_status")
        if tid is None or prov is None or st is None:
            continue
        retry = None
        if retry_class:
            retry = _allow(
                retry_class, TOOL_RETRY_CLASSES, missing, "unsupported_retry_class"
            )
        kept.append(
            ToolOutcomeCountSignal(
                signal_id="sig:01",
                tool_id=tid,  # type: ignore[arg-type]
                provider=prov,  # type: ignore[arg-type]
                status=st,  # type: ignore[arg-type]
                count=group.count,
                retry_class=retry,  # type: ignore[arg-type]
            )
        )
    pending.extend(kept[:MAX_TOOL_GROUPS])


def _add_evaluation_signals(
    run: EvaluationRunRow | None,
    dimensions: tuple[DimensionRow, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    if run is None:
        _note(missing, "evaluation_absent")
        return
    status = _allow(
        run.status, EVALUATION_RUN_STATUSES, missing, "unsupported_evaluation_status"
    )
    profile = _allow(
        run.profile, EVALUATION_PROFILES, missing, "unsupported_evaluation_profile"
    )
    hint = None
    if run.disposition_hint is not None:
        hint = _allow(
            run.disposition_hint,
            DISPOSITION_HINTS,
            missing,
            "unsupported_disposition_hint",
        )
    if status is None or profile is None:
        _note(missing, "evaluation_incomplete")
    elif status == "SUCCEEDED" and (run.passed is None or run.aggregate_score is None):
        _note(missing, "evaluation_incomplete")
    else:
        pending.append(
            EvaluationRunSignal(
                signal_id="sig:01",
                status=status,  # type: ignore[arg-type]
                profile=profile,  # type: ignore[arg-type]
                passed=run.passed,
                aggregate_score=run.aggregate_score,
                disposition_hint=hint,  # type: ignore[arg-type]
            )
        )
    kept: list[EvaluationDimensionSignal] = []
    for item in dimensions:
        name = _allow(item.name, DIMENSION_NAMES, missing, "unsupported_dimension_name")
        method = _allow(
            item.method, GRADER_METHODS, missing, "unsupported_grader_method"
        )
        if name is None or method is None:
            continue
        codes: list[str] = []
        dropped = False
        for code in item.failure_codes:
            if code in ADVISORY_FAILURE_CODES:
                if code not in codes:
                    codes.append(code)
            else:
                dropped = True
        if dropped:
            _note(missing, "unsupported_failure_code")
        kept.append(
            EvaluationDimensionSignal(
                signal_id="sig:01",
                name=name,  # type: ignore[arg-type]
                score=item.score,
                passed=item.passed,
                method=method,  # type: ignore[arg-type]
                failure_codes=codes[:MAX_FAILURE_CODES_PER_DIMENSION],
            )
        )
    pending.extend(kept[:MAX_EVALUATION_DIMENSIONS])


def _add_recovery_signals(
    rows: tuple[RecoveryRow, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    kept: list[RecoveryDecisionSignal] = []
    for row in rows:
        decision = _allow(
            row.decision, RECOVERY_DECISIONS, missing, "unsupported_recovery_decision"
        )
        category = _allow(
            row.failure_category,
            FAILURE_CATEGORIES,
            missing,
            "unsupported_failure_category",
        )
        reason = _allow(
            row.reason_code, ADVISORY_REASON_CODES, missing, "unsupported_reason_code"
        )
        if decision is None or category is None or reason is None:
            continue
        if decision == "retry":
            if row.attempt_number is None:
                _note(missing, "recovery_attempt_absent")
                continue
            kept.append(
                RecoveryDecisionSignal(
                    signal_id="sig:01",
                    decision=decision,  # type: ignore[arg-type]
                    failure_category=category,  # type: ignore[arg-type]
                    reason_code=reason,  # type: ignore[arg-type]
                    attempt_number=row.attempt_number,
                )
            )
            continue
        kept.append(
            RecoveryDecisionSignal(
                signal_id="sig:01",
                decision=decision,  # type: ignore[arg-type]
                failure_category=category,  # type: ignore[arg-type]
                reason_code=reason,  # type: ignore[arg-type]
                attempt_number=None,
            )
        )
    pending.extend(kept[:MAX_RECOVERY_DECISIONS])


def _add_review_signals(
    counts: tuple[tuple[str, int], ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    for decision, count in counts:
        allowed = _allow(
            decision, REVIEW_DECISIONS, missing, "unsupported_review_decision"
        )
        if allowed is None:
            continue
        pending.append(
            ReviewDecisionCountSignal(
                signal_id="sig:01",
                decision=allowed,  # type: ignore[arg-type]
                count=count,
            )
        )


def _add_outbox_signals(
    rows: tuple[OutboxRow, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    if not rows:
        _note(missing, "outbox_absent")
        return
    kept: list[OutboxSummarySignal] = []
    for row in rows:
        event_type = _allow(
            row.event_type, EVENT_TYPES, missing, "unsupported_event_type"
        )
        if event_type is None:
            continue
        error_class = None
        if row.last_publish_error_class:
            error_class = _allow(
                row.last_publish_error_class,
                ADVISORY_ERROR_CLASSES,
                missing,
                "unsupported_error_class",
            )
        kept.append(
            OutboxSummarySignal(
                signal_id="sig:01",
                event_type=event_type,  # type: ignore[arg-type]
                unpublished_count=row.unpublished_count,
                published_count=row.published_count,
                max_publish_attempts=row.max_publish_attempts,
                last_publish_error_class=error_class,
            )
        )
    pending.extend(kept[:MAX_OUTBOX_GROUPS])


def _add_consumer_signals(
    projection_event_type: str | None,
    dead_letters: tuple[DeadLetterRow, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    if projection_event_type is None and not dead_letters:
        _note(missing, "consumer_absent")
        return
    if projection_event_type is not None:
        event_type = _allow(
            projection_event_type, EVENT_TYPES, missing, "unsupported_event_type"
        )
        if event_type is not None:
            pending.append(
                ConsumerProjectionSignal(
                    signal_id="sig:01",
                    last_event_type=event_type,  # type: ignore[arg-type]
                )
            )
    kept: list[ConsumerDeadLetterSignal] = []
    for row in dead_letters:
        code = _allow(
            row.failure_code,
            CONSUMER_FAILURE_CODES,
            missing,
            "unsupported_consumer_failure_code",
        )
        state = _allow(
            row.replay_state, REPLAY_STATES, missing, "unsupported_replay_state"
        )
        if code is None or state is None:
            continue
        kept.append(
            ConsumerDeadLetterSignal(
                signal_id="sig:01",
                failure_code=code,  # type: ignore[arg-type]
                replay_state=state,  # type: ignore[arg-type]
                replay_eligible=row.replay_eligible,
                count=row.count,
            )
        )
    pending.extend(kept[:MAX_DEAD_LETTERS])


def _add_alert_signals(
    names: tuple[str, ...],
    pending: list[AdvisorySignal],
    missing: list[MissingSourceCode],
) -> None:
    for name in names:
        allowed = _allow(name, ALERT_NAMES, missing, "unsupported_alert_name")
        if allowed is None:
            continue
        pending.append(
            AlertNameSignal(signal_id="sig:01", alert_name=allowed)  # type: ignore[arg-type]
        )
