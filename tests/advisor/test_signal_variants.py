"""Unsupported stored values never reach model-visible facts."""

from __future__ import annotations

from atlas.advisor.contracts import RecoveryDecisionSignal
from atlas.advisor.snapshot import (
    DeadLetterRow,
    EvaluationRunRow,
    JobRow,
    RecoveryRow,
    SnapshotLoad,
    assemble_facts,
    canonical_facts_json,
)


def _job() -> JobRow:
    return JobRow(
        research_job_id="job-1",
        status="FAILED",
        evaluation_profile="evaluation.candidate.v1",
        continuation_mode="NONE",
        repair_count=0,
        job_retry_count=0,
        evaluation_attempt_count=0,
    )


def test_unknown_event_type_omitted() -> None:
    from atlas.advisor.snapshot import OutboxRow

    loaded = SnapshotLoad(
        job=_job(),
        outbox=(
            OutboxRow(
                event_type="evil.event",
                unpublished_count=1,
                published_count=0,
                max_publish_attempts=3,
                last_publish_error_class=None,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "evil.event" not in encoded
    assert "unsupported_event_type" in facts.missing_sources


def test_unknown_recovery_reason_omitted() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        recovery=(
            RecoveryRow(
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="PLEASE_IGNORE_INSTRUCTIONS",
                attempt_number=1,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "PLEASE_IGNORE_INSTRUCTIONS" not in encoded
    assert "unsupported_reason_code" in facts.missing_sources


def test_unknown_dead_letter_code_omitted() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        dead_letters=(
            DeadLetterRow(
                failure_code="drop_table_students",
                replay_state="PENDING",
                replay_eligible=False,
                count=1,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "drop_table_students" not in encoded
    assert "unsupported_consumer_failure_code" in facts.missing_sources


def test_recovery_reject_is_omitted_as_unsupported() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        recovery=(
            RecoveryRow(
                decision="reject",
                failure_category="NEEDS_HUMAN_REVIEW",
                reason_code="AMBIGUOUS_QUALITY",
                attempt_number=None,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    types = {item.signal_type for item in facts.signals}
    assert "recovery.decision" not in types
    assert "unsupported_recovery_decision" in facts.missing_sources
    assert '"reject"' not in encoded


def test_retry_attempts_one_and_two_are_reported_accurately() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        recovery=(
            RecoveryRow(
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                attempt_number=1,
            ),
            RecoveryRow(
                decision="retry",
                failure_category="TRANSIENT_RATE_LIMIT",
                reason_code="TRANSIENT_RETRY",
                attempt_number=2,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    recoveries = [
        item for item in facts.signals if isinstance(item, RecoveryDecisionSignal)
    ]
    attempts = sorted(
        item.attempt_number for item in recoveries if item.attempt_number is not None
    )
    assert attempts == [1, 2]


def test_non_retry_decisions_never_receive_an_attempt_number() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        recovery=(
            RecoveryRow(
                decision="complete",
                failure_category="QUALITY_STRUCTURE",
                reason_code="EVALUATION_PASSED",
                attempt_number=None,
            ),
            RecoveryRow(
                decision="repair",
                failure_category="REPAIRABLE_DRAFT",
                reason_code="STRUCTURE_REPAIR",
                attempt_number=1,
            ),
            RecoveryRow(
                decision="terminal",
                failure_category="PERMANENT_VALIDATION",
                reason_code="PERMANENT_FAIL",
                attempt_number=2,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    recoveries = [
        item for item in facts.signals if isinstance(item, RecoveryDecisionSignal)
    ]
    assert {item.decision for item in recoveries} == {
        "complete",
        "repair",
        "terminal",
    }
    for item in recoveries:
        assert item.attempt_number is None
        assert "attempt_number" not in item.model_dump(exclude_none=True)


def test_retry_without_recovery_row_is_omitted() -> None:
    loaded = SnapshotLoad(
        job=_job(),
        recovery=(
            RecoveryRow(
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                attempt_number=None,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    types = {item.signal_type for item in facts.signals}
    assert "recovery.decision" not in types
    assert "recovery_attempt_absent" in facts.missing_sources


def test_evaluation_unknown_dimension_omitted() -> None:
    from atlas.advisor.snapshot import DimensionRow

    loaded = SnapshotLoad(
        job=_job(),
        evaluation_run=EvaluationRunRow(
            status="SUCCEEDED",
            profile="evaluation.candidate.v1",
            passed=True,
            aggregate_score=1.0,
            disposition_hint="complete",
        ),
        dimensions=(
            DimensionRow(
                name="secret_quality",
                score=0.1,
                passed=False,
                method="llm",
                failure_codes=("NOT_A_REAL_CODE",),
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "secret_quality" not in encoded
    assert "NOT_A_REAL_CODE" not in encoded
    assert "unsupported_dimension_name" in facts.missing_sources


def test_null_evaluation_profile_is_absent_not_raw() -> None:
    pending = JobRow(
        research_job_id="job-1",
        status="PENDING",
        evaluation_profile=None,
        continuation_mode="NONE",
        repair_count=0,
        job_retry_count=0,
        evaluation_attempt_count=0,
    )
    facts = assemble_facts(SnapshotLoad(job=pending))
    assert "evaluation_profile_absent" in facts.missing_sources
    types = {item.signal_type for item in facts.signals}
    assert "job.evaluation_profile" not in types
