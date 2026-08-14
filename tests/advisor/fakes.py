"""Test-only advisory snapshot ports and fact builders. Not a production loader."""

from __future__ import annotations

from sqlalchemy.orm import Session

from atlas.advisor.catalogs import (
    DIMENSION_NAMES,
    MODEL_NODE_NAMES,
    MODEL_PROVIDERS,
    TOOL_IDS,
    TOOL_PROVIDERS,
    WORKFLOW_NODE_NAMES,
)
from atlas.advisor.contracts import AdvisoryIncidentFacts, JobStatusSignal
from atlas.advisor.snapshot import (
    CountGroup,
    DimensionRow,
    EvaluationRunRow,
    JobRow,
    NodeRow,
    SnapshotLoad,
)


def minimal_facts(
    *,
    research_job_id: str = "job-1",
    status: str = "FAILED",
) -> AdvisoryIncidentFacts:
    return AdvisoryIncidentFacts(
        research_job_id=research_job_id,
        signals=[
            JobStatusSignal(signal_id="sig:01", status=status)  # type: ignore[arg-type]
        ],
        missing_sources=["evaluation_absent", "outbox_absent", "consumer_absent"],
    )


class FakeAdvisorySnapshotPort:
    """In-memory snapshot port for unit tests."""

    def __init__(self, loaded: SnapshotLoad) -> None:
        self.loaded = loaded
        self.sessions: list[Session] = []

    def load(self, session: Session, research_job_id: str) -> SnapshotLoad:
        del research_job_id
        self.sessions.append(session)
        return self.loaded


def pending_job_load(research_job_id: str = "job-1") -> SnapshotLoad:
    return SnapshotLoad(
        job=JobRow(
            research_job_id=research_job_id,
            status="PENDING",
            evaluation_profile=None,
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
    )


def snapshot_load_with_signal_count(count: int) -> SnapshotLoad:
    """Build a SnapshotLoad whose assembler emits exactly ``count`` signals.

    Used to prove the fail-closed ``MAX_SIGNALS`` bound. Requires ``count >= 6``
    so the six always-present job signals fit.
    """
    if count < 6:
        raise ValueError("job signals already contribute 6")
    remaining = count - 6
    node_count = min(32, remaining)
    remaining -= node_count
    model_count = min(12, remaining)
    remaining -= model_count
    tool_count = min(8, remaining)
    remaining -= tool_count
    include_eval = remaining > 0
    if include_eval:
        remaining -= 1
    dim_count = min(7, remaining)
    remaining -= dim_count
    if remaining != 0:
        raise ValueError("requested signal count exceeds bounded category caps")

    nodes: list[NodeRow] = []
    attempt = 1
    names = tuple(sorted(WORKFLOW_NODE_NAMES))
    while len(nodes) < node_count:
        for name in names:
            if len(nodes) >= node_count:
                break
            nodes.append(
                NodeRow(
                    node_name=name,
                    attempt=attempt,
                    status="COMPLETED",
                    error=None,
                )
            )
        attempt += 1

    models: list[CountGroup] = []
    for node_name in sorted(MODEL_NODE_NAMES):
        for provider in sorted(MODEL_PROVIDERS):
            for status in ("FAILED", "SUCCEEDED", "IN_PROGRESS"):
                if len(models) >= model_count:
                    break
                models.append(
                    CountGroup(
                        keys=(node_name, provider, status, "", ""),
                        count=1,
                    )
                )
            if len(models) >= model_count:
                break
        if len(models) >= model_count:
            break

    tools: list[CountGroup] = []
    for tool_id in sorted(TOOL_IDS):
        for provider in sorted(TOOL_PROVIDERS):
            for status in ("FAILED", "SUCCEEDED", "IN_PROGRESS"):
                if len(tools) >= tool_count:
                    break
                tools.append(CountGroup(keys=(tool_id, provider, status, ""), count=1))
            if len(tools) >= tool_count:
                break
        if len(tools) >= tool_count:
            break

    evaluation_run = None
    dimensions: tuple[DimensionRow, ...] = ()
    if include_eval:
        evaluation_run = EvaluationRunRow(
            status="SUCCEEDED",
            profile="evaluation.candidate.v1",
            passed=True,
            aggregate_score=1.0,
            disposition_hint="complete",
        )
        dim_names = tuple(sorted(DIMENSION_NAMES))
        dimensions = tuple(
            DimensionRow(
                name=dim_names[index],
                score=1.0,
                passed=True,
                method="deterministic",
                failure_codes=(),
            )
            for index in range(dim_count)
        )

    return SnapshotLoad(
        job=JobRow(
            research_job_id="job-1",
            status="FAILED",
            evaluation_profile="evaluation.candidate.v1",
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=1,
        ),
        nodes=tuple(nodes),
        model_groups=tuple(models),
        tool_groups=tuple(tools),
        evaluation_run=evaluation_run,
        dimensions=dimensions,
    )
