"""SQLAlchemy repository for evaluation runs and dimension rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from atlas.evaluation.contracts import (
    DimensionResult,
    DispositionHint,
    EvaluationRunResult,
)
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)


class EvaluationRunRecord:
    """Read model for an evaluation_runs row."""

    __slots__ = (
        "id",
        "research_job_id",
        "workflow_execution_id",
        "evaluation_profile",
        "evaluation_attempt",
        "status",
        "ownership_token",
        "input_fingerprint",
        "job_claim_fingerprint",
        "passed",
        "aggregate_score",
        "disposition_hint",
        "grader_versions_json",
        "deadline_at",
        "started_at",
        "finished_at",
    )

    def __init__(self, row: EvaluationRunModel) -> None:
        self.id = row.id
        self.research_job_id = row.research_job_id
        self.workflow_execution_id = row.workflow_execution_id
        self.evaluation_profile = row.evaluation_profile
        self.evaluation_attempt = row.evaluation_attempt
        self.status = row.status
        self.ownership_token = row.ownership_token
        self.input_fingerprint = row.input_fingerprint
        self.job_claim_fingerprint = row.job_claim_fingerprint
        self.passed = row.passed
        self.aggregate_score = row.aggregate_score
        self.disposition_hint = row.disposition_hint
        self.grader_versions_json = row.grader_versions_json
        self.deadline_at = row.deadline_at
        self.started_at = row.started_at
        self.finished_at = row.finished_at


class SqlAlchemyEvaluationRepository:
    """Persist fenced evaluation runs and normalized dimension results."""

    def get_by_execution_key(
        self,
        session: Session,
        *,
        workflow_execution_id: str,
        evaluation_profile: str,
        evaluation_attempt: int,
        for_update: bool = False,
    ) -> EvaluationRunRecord | None:
        stmt = select(EvaluationRunModel).where(
            EvaluationRunModel.workflow_execution_id == workflow_execution_id,
            EvaluationRunModel.evaluation_profile == evaluation_profile,
            EvaluationRunModel.evaluation_attempt == evaluation_attempt,
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = session.execute(stmt).scalar_one_or_none()
        return EvaluationRunRecord(row) if row is not None else None

    def get_by_id(
        self,
        session: Session,
        run_id: str,
        *,
        for_update: bool = False,
    ) -> EvaluationRunRecord | None:
        stmt = select(EvaluationRunModel).where(EvaluationRunModel.id == run_id)
        if for_update:
            stmt = stmt.with_for_update()
        row = session.execute(stmt).scalar_one_or_none()
        return EvaluationRunRecord(row) if row is not None else None

    def create_run(
        self,
        session: Session,
        *,
        run_id: str,
        research_job_id: str,
        workflow_execution_id: str,
        evaluation_profile: str,
        evaluation_attempt: int,
        ownership_token: str,
        input_fingerprint: str,
        job_claim_fingerprint: str,
        deadline_at: datetime,
        at: datetime,
    ) -> None:
        session.add(
            EvaluationRunModel(
                id=run_id,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                evaluation_profile=evaluation_profile,
                evaluation_attempt=evaluation_attempt,
                status="IN_PROGRESS",
                ownership_token=ownership_token,
                input_fingerprint=input_fingerprint,
                job_claim_fingerprint=job_claim_fingerprint,
                passed=None,
                aggregate_score=None,
                disposition_hint=None,
                grader_versions_json=None,
                deadline_at=deadline_at,
                started_at=at,
                finished_at=None,
            )
        )
        session.flush()

    def reclaim_run(
        self,
        session: Session,
        *,
        run_id: str,
        ownership_token: str,
        input_fingerprint: str,
        job_claim_fingerprint: str,
        deadline_at: datetime,
        at: datetime,
    ) -> None:
        """Reclaim a stale IN_PROGRESS or FAILED run with a new ownership token."""
        row = session.get(EvaluationRunModel, run_id)
        if row is None:
            raise LookupError("evaluation run not found")
        row.status = "IN_PROGRESS"
        row.ownership_token = ownership_token
        row.input_fingerprint = input_fingerprint
        row.job_claim_fingerprint = job_claim_fingerprint
        row.passed = None
        row.aggregate_score = None
        row.disposition_hint = None
        row.grader_versions_json = None
        row.deadline_at = deadline_at
        row.started_at = at
        row.finished_at = None
        self.clear_dimensions(session, evaluation_run_id=run_id)
        session.flush()

    def clear_dimensions(self, session: Session, *, evaluation_run_id: str) -> None:
        session.execute(
            delete(EvaluationDimensionResultModel).where(
                EvaluationDimensionResultModel.evaluation_run_id == evaluation_run_id
            )
        )
        session.flush()

    def replace_dimensions(
        self,
        session: Session,
        *,
        evaluation_run_id: str,
        dimensions: list[DimensionResult],
    ) -> None:
        self.clear_dimensions(session, evaluation_run_id=evaluation_run_id)
        for item in dimensions:
            session.add(
                EvaluationDimensionResultModel(
                    id=str(uuid4()),
                    evaluation_run_id=evaluation_run_id,
                    dimension_name=item.name,
                    score=item.score,
                    passed=item.passed,
                    method=item.method,
                    is_hard=item.is_hard,
                    is_provisional=item.is_provisional,
                    failure_codes=list(item.failure_codes),
                    weight=item.weight,
                )
            )
        session.flush()

    def finalize_success(
        self,
        session: Session,
        *,
        run_id: str,
        ownership_token: str,
        aggregate_score: float,
        passed: bool,
        disposition_hint: str,
        grader_versions: dict[str, str],
        at: datetime,
    ) -> bool:
        result = session.execute(
            update(EvaluationRunModel)
            .where(
                EvaluationRunModel.id == run_id,
                EvaluationRunModel.status == "IN_PROGRESS",
                EvaluationRunModel.ownership_token == ownership_token,
            )
            .values(
                status="SUCCEEDED",
                passed=passed,
                aggregate_score=aggregate_score,
                disposition_hint=disposition_hint,
                grader_versions_json=dict(grader_versions),
                finished_at=at,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def finalize_failure(
        self,
        session: Session,
        *,
        run_id: str,
        ownership_token: str,
        at: datetime,
        grader_versions: dict[str, Any] | None = None,
    ) -> bool:
        values: dict[str, Any] = {
            "status": "FAILED",
            "finished_at": at,
            "passed": None,
            "aggregate_score": None,
            "disposition_hint": "terminal",
        }
        if grader_versions is not None:
            values["grader_versions_json"] = dict(grader_versions)
        result = session.execute(
            update(EvaluationRunModel)
            .where(
                EvaluationRunModel.id == run_id,
                EvaluationRunModel.status == "IN_PROGRESS",
                EvaluationRunModel.ownership_token == ownership_token,
            )
            .values(**values)
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def list_dimensions(
        self,
        session: Session,
        *,
        evaluation_run_id: str,
    ) -> list[DimensionResult]:
        rows = session.execute(
            select(EvaluationDimensionResultModel)
            .where(
                EvaluationDimensionResultModel.evaluation_run_id == evaluation_run_id
            )
            .order_by(EvaluationDimensionResultModel.dimension_name.asc())
        ).scalars()
        return [
            DimensionResult(
                name=row.dimension_name,  # type: ignore[arg-type]
                score=row.score,
                passed=row.passed,
                method=row.method,  # type: ignore[arg-type]
                is_hard=row.is_hard,
                is_provisional=row.is_provisional,
                failure_codes=list(row.failure_codes or []),
                weight=row.weight,
            )
            for row in rows
        ]

    def to_result(
        self,
        session: Session,
        record: EvaluationRunRecord,
        *,
        include_ownership_token: bool = False,
    ) -> EvaluationRunResult:
        dimensions = self.list_dimensions(session, evaluation_run_id=record.id)
        hint: DispositionHint | None
        if record.disposition_hint in {"complete", "terminal"}:
            hint = record.disposition_hint  # type: ignore[assignment]
        else:
            hint = None
        return EvaluationRunResult(
            run_id=record.id,
            research_job_id=record.research_job_id,
            workflow_execution_id=record.workflow_execution_id,
            evaluation_profile=record.evaluation_profile,  # type: ignore[arg-type]
            evaluation_attempt=record.evaluation_attempt,
            status=record.status,  # type: ignore[arg-type]
            input_fingerprint=record.input_fingerprint,
            passed=record.passed,
            aggregate_score=record.aggregate_score,
            disposition_hint=hint,
            dimensions=dimensions,
            grader_versions=dict(record.grader_versions_json or {}),
            ownership_token=(
                record.ownership_token if include_ownership_token else None
            ),
        )

    def get_latest_for_job(
        self,
        session: Session,
        *,
        research_job_id: str,
    ) -> EvaluationRunRecord | None:
        row = session.execute(
            select(EvaluationRunModel)
            .where(EvaluationRunModel.research_job_id == research_job_id)
            .order_by(
                EvaluationRunModel.started_at.desc(),
                EvaluationRunModel.evaluation_attempt.desc(),
            )
            .limit(1)
        ).scalar_one_or_none()
        return EvaluationRunRecord(row) if row is not None else None

    def list_for_job(
        self,
        session: Session,
        *,
        research_job_id: str,
    ) -> list[EvaluationRunRecord]:
        rows = session.execute(
            select(EvaluationRunModel)
            .where(EvaluationRunModel.research_job_id == research_job_id)
            .order_by(
                EvaluationRunModel.started_at.asc(),
                EvaluationRunModel.evaluation_attempt.asc(),
            )
        ).scalars()
        return [EvaluationRunRecord(row) for row in rows]
