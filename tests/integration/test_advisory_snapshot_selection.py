"""PostgreSQL snapshot selection: recovery attempts and deterministic ties."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.contracts import (
    AdvisoryIncidentFacts,
    EvaluationDimensionSignal,
    EvaluationRunSignal,
    RecoveryDecisionSignal,
)
from atlas.advisor.db import advisory_read_only_scope
from atlas.advisor.snapshot import SnapshotLoad, assemble_facts
from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)
from atlas.persistence.models.recovery import (
    JobRecoveryAttemptModel,
    PolicyDecisionModel,
)
from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)
from atlas.persistence.repositories.advisory_snapshot import (
    SqlAlchemyAdvisorySnapshotReader,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 14, 14, 0, 0, tzinfo=UTC)
_TOKEN = "a" * 64
_REASONS = (
    "EVALUATION_PASSED",
    "EVALUATION_ATTEMPT_CAP",
    "HARD_QUALITY_FAIL",
    "STRUCTURE_REPAIR",
    "STRUCTURE_REPAIR_EXHAUSTED",
    "SOFT_QUALITY_REPAIR",
    "REPAIR_EXHAUSTED",
    "AMBIGUOUS_QUALITY",
    "OWNERSHIP_CONFLICT",
)


def _uid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def _fp(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _seed_job(session_factory: sessionmaker[Session], job_id: str) -> str:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create(job_id, "What is Atlas?", at=T0)
    with session_scope(session_factory) as session:
        repo.add(
            session,
            job,
            idempotency_key=f"key-{job_id}",
            request_fingerprint=_fp(f"req-{job_id}"),
        )
    return job_id


def _add_execution(
    session: Session, *, execution_id: str, job_id: str, node_name: str
) -> None:
    session.add(
        WorkflowExecutionModel(
            id=execution_id,
            research_job_id=job_id,
            thread_id=execution_id,
            status="RUNNING",
            started_at=T0,
            finished_at=None,
        )
    )
    session.add(
        WorkflowNodeExecutionModel(
            id=_fp(f"node-{execution_id}")[:36],
            workflow_execution_id=execution_id,
            node_name=node_name,
            attempt=1,
            status="STARTED",
            started_at=T0,
            finished_at=None,
            error=None,
        )
    )
    session.flush()


def _load_facts(
    session_factory: sessionmaker[Session], job_id: str
) -> tuple[SnapshotLoad, AdvisoryIncidentFacts]:
    reader = SqlAlchemyAdvisorySnapshotReader()
    with advisory_read_only_scope(session_factory) as session:
        loaded = reader.load(session, job_id)
        return loaded, assemble_facts(loaded)


def test_recovery_attempts_come_from_real_rows(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = _seed_job(session_factory, "advisory-recovery-attempts-1")
    exec_id = _uid(1)
    retry_one = _uid(11)
    retry_two = _uid(12)
    complete_id = _uid(13)
    orphan_retry = _uid(14)
    with session_scope(session_factory) as session:
        _add_execution(session, execution_id=exec_id, job_id=job_id, node_name="plan")
        session.add(
            PolicyDecisionModel(
                id=retry_one,
                research_job_id=job_id,
                workflow_execution_id=exec_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_TIMEOUT",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=_fp("retry-1"),
                created_at=T0,
            )
        )
        session.add(
            PolicyDecisionModel(
                id=retry_two,
                research_job_id=job_id,
                workflow_execution_id=exec_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_RATE_LIMIT",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=_fp("retry-2"),
                created_at=T0,
            )
        )
        session.add(
            PolicyDecisionModel(
                id=complete_id,
                research_job_id=job_id,
                workflow_execution_id=exec_id,
                evaluation_run_id=None,
                decision="complete",
                failure_category="QUALITY_STRUCTURE",
                reason_code="EVALUATION_PASSED",
                decision_fingerprint=_fp("complete"),
                created_at=T0,
            )
        )
        session.add(
            PolicyDecisionModel(
                id=orphan_retry,
                research_job_id=job_id,
                workflow_execution_id=exec_id,
                evaluation_run_id=None,
                decision="retry",
                failure_category="TRANSIENT_PROVIDER",
                reason_code="TRANSIENT_RETRY",
                decision_fingerprint=_fp("orphan-retry"),
                created_at=T0,
            )
        )
        session.flush()
        session.add(
            JobRecoveryAttemptModel(
                id=_uid(21),
                research_job_id=job_id,
                policy_decision_id=retry_one,
                abandoned_workflow_execution_id=exec_id,
                attempt_number=1,
                next_attempt_at=T0,
                created_at=T0,
            )
        )
        session.add(
            JobRecoveryAttemptModel(
                id=_uid(22),
                research_job_id=job_id,
                policy_decision_id=retry_two,
                abandoned_workflow_execution_id=exec_id,
                attempt_number=2,
                next_attempt_at=T0,
                created_at=T0,
            )
        )

    loaded, facts = _load_facts(session_factory, job_id)
    by_category = {row.failure_category: row.attempt_number for row in loaded.recovery}
    assert by_category["TRANSIENT_TIMEOUT"] == 1
    assert by_category["TRANSIENT_RATE_LIMIT"] == 2
    assert by_category["QUALITY_STRUCTURE"] is None
    assert by_category["TRANSIENT_PROVIDER"] is None

    recoveries = [
        item for item in facts.signals if isinstance(item, RecoveryDecisionSignal)
    ]
    attempts = {item.failure_category: item.attempt_number for item in recoveries}
    assert attempts["TRANSIENT_TIMEOUT"] == 1
    assert attempts["TRANSIENT_RATE_LIMIT"] == 2
    assert attempts["QUALITY_STRUCTURE"] is None
    assert "TRANSIENT_PROVIDER" not in attempts
    assert "recovery_attempt_absent" in facts.missing_sources
    complete = next(item for item in recoveries if item.decision == "complete")
    assert "attempt_number" not in complete.model_dump(exclude_none=True)


def test_tied_workflow_executions_select_highest_id_repeatedly(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = _seed_job(session_factory, "advisory-tie-workflow-1")
    low_id = _uid(1)
    high_id = _uid(2)
    with session_scope(session_factory) as session:
        _add_execution(session, execution_id=low_id, job_id=job_id, node_name="plan")
        _add_execution(session, execution_id=high_id, job_id=job_id, node_name="draft")

    names: list[tuple[str, ...]] = []
    for _ in range(3):
        loaded, _facts = _load_facts(session_factory, job_id)
        names.append(tuple(node.node_name for node in loaded.nodes))
    assert names == [("draft",), ("draft",), ("draft",)]


def test_tied_evaluation_runs_share_one_selected_id(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = _seed_job(session_factory, "advisory-tie-eval-1")
    exec_id = _uid(1)
    low_run = _uid(31)
    high_run = _uid(32)
    with session_scope(session_factory) as session:
        _add_execution(
            session, execution_id=exec_id, job_id=job_id, node_name="evaluate"
        )
        for run_id, attempt, score, _dimension in (
            (low_run, 1, 0.11, "citation_integrity"),
            (high_run, 2, 0.91, "coverage"),
        ):
            session.add(
                EvaluationRunModel(
                    id=run_id,
                    research_job_id=job_id,
                    workflow_execution_id=exec_id,
                    evaluation_profile="evaluation.candidate.v1",
                    evaluation_attempt=attempt,
                    status="SUCCEEDED",
                    ownership_token=_TOKEN,
                    input_fingerprint=_fp(f"input-{run_id}"),
                    job_claim_fingerprint=_fp(f"claim-{run_id}"),
                    passed=True,
                    aggregate_score=score,
                    disposition_hint="complete",
                    deadline_at=T0,
                    started_at=T0,
                    finished_at=T0,
                )
            )
        session.flush()
        for run_id, _attempt, score, dimension in (
            (low_run, 1, 0.11, "citation_integrity"),
            (high_run, 2, 0.91, "coverage"),
        ):
            session.add(
                EvaluationDimensionResultModel(
                    id=_fp(f"dim-{run_id}")[:36],
                    evaluation_run_id=run_id,
                    dimension_name=dimension,
                    score=score,
                    passed=True,
                    method="deterministic",
                    is_hard=False,
                    is_provisional=False,
                    failure_codes=[],
                    weight=1.0,
                )
            )

    first, facts = _load_facts(session_factory, job_id)
    second, _ = _load_facts(session_factory, job_id)
    assert first.evaluation_run is not None
    assert second.evaluation_run is not None
    assert first.evaluation_run.aggregate_score == 0.91
    assert second.evaluation_run.aggregate_score == 0.91
    assert tuple(item.name for item in first.dimensions) == ("coverage",)
    assert tuple(item.name for item in second.dimensions) == ("coverage",)
    eval_signals = [
        item for item in facts.signals if isinstance(item, EvaluationRunSignal)
    ]
    dim_signals = [
        item for item in facts.signals if isinstance(item, EvaluationDimensionSignal)
    ]
    assert eval_signals[0].aggregate_score == 0.91
    assert dim_signals[0].name == "coverage"


def test_tied_recovery_rows_keep_highest_eight_ids(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = _seed_job(session_factory, "advisory-tie-recovery-1")
    exec_id = _uid(100)
    with session_scope(session_factory) as session:
        _add_execution(session, execution_id=exec_id, job_id=job_id, node_name="policy")
        for index, reason in enumerate(_REASONS, start=1):
            session.add(
                PolicyDecisionModel(
                    id=_uid(index),
                    research_job_id=job_id,
                    workflow_execution_id=exec_id,
                    evaluation_run_id=None,
                    decision="terminal",
                    failure_category="TERMINAL_UNKNOWN",
                    reason_code=reason,
                    decision_fingerprint=_fp(f"term-{index}"),
                    created_at=T0,
                )
            )

    expected = frozenset(_REASONS[1:])
    for _ in range(3):
        loaded, _facts = _load_facts(session_factory, job_id)
        selected = tuple(row.reason_code for row in loaded.recovery)
        assert len(selected) == 8
        assert frozenset(selected) == expected
        assert "EVALUATION_PASSED" not in selected
