"""Orchestrate snapshot assembly then analyst invocation.

The READ ONLY database scope is closed before the analyst port runs.
Snapshot assembly metrics cover that database phase. The advisory
analysis duration starts only after ``_load_facts()`` returns.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from uuid import uuid4

from sqlalchemy.orm import Session

from atlas.advisor.contracts import (
    AdvisoryIncidentFacts,
    AdvisoryStdoutEnvelope,
)
from atlas.advisor.errors import AdvisoryOutputRejectedError
from atlas.advisor.output_policy import validate_advisory_output
from atlas.advisor.ports import AdvisoryAnalystPort, AdvisorySnapshotPort
from atlas.advisor.snapshot import assemble_facts
from atlas.observability.metrics import AtlasMetrics, default_metrics

ReadScope = Callable[[], AbstractContextManager[Session]]


class AdvisoryService:
    """Load facts inside a short READ ONLY scope, then analyze without a session."""

    def __init__(
        self,
        *,
        read_scope: ReadScope,
        snapshot: AdvisorySnapshotPort,
        analyst: AdvisoryAnalystPort,
        metrics: AtlasMetrics | None = None,
        analysis_id_factory: Callable[[], str] = lambda: str(uuid4()),
        monotonic: Callable[[], float] = time.monotonic,
        mode: str = "fake",
    ) -> None:
        self._read_scope = read_scope
        self._snapshot = snapshot
        self._analyst = analyst
        self._metrics = metrics if metrics is not None else default_metrics()
        self._analysis_id_factory = analysis_id_factory
        self._monotonic = monotonic
        self._mode = mode

    def analyze_job(self, research_job_id: str) -> AdvisoryStdoutEnvelope:
        try:
            facts = self._load_facts(research_job_id)
        except Exception:
            self._metrics.observe_advisory_snapshot_assembly(
                source="research_job", outcome="rejected"
            )
            raise
        self._metrics.observe_advisory_snapshot_assembly(
            source="research_job", outcome="succeeded"
        )
        analysis_started = self._monotonic()
        analysis_id = self._analysis_id_factory()
        try:
            analysis = self._analyst.analyze(facts, analysis_id=analysis_id)
            validate_advisory_output(facts, analysis)
        except Exception as exc:
            outcome = _analysis_outcome(exc)
            self._metrics.observe_advisory_analysis(
                mode=self._mode,
                outcome=outcome,
                duration_seconds=self._monotonic() - analysis_started,
            )
            raise
        self._metrics.observe_advisory_analysis(
            mode=self._mode,
            outcome="succeeded",
            duration_seconds=self._monotonic() - analysis_started,
        )
        return AdvisoryStdoutEnvelope(
            analysis_id=analysis_id,
            research_job_id=facts.research_job_id,
            analysis=analysis,
        )

    def _load_facts(self, research_job_id: str) -> AdvisoryIncidentFacts:
        with self._read_scope() as session:
            loaded = self._snapshot.load(session, research_job_id)
            return assemble_facts(loaded)


def _analysis_outcome(exc: BaseException) -> str:
    name = type(exc).__name__
    if name == "AdvisoryAnalysisTimeoutError":
        return "timeout"
    if name == "ModelRefusalError":
        return "refusal"
    if name in {"ModelInvalidStructuredOutputError", "AdvisoryOutputRejectedError"}:
        return "malformed"
    if name.startswith("Model"):
        return "model_failed"
    if isinstance(exc, AdvisoryOutputRejectedError):
        return "malformed"
    return "other"
