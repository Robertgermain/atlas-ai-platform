"""Deterministic offline advisory analyst. No network, no database."""

from __future__ import annotations

from atlas.advisor.catalogs import ADVISORY_FAKE_IDENTITY
from atlas.advisor.contracts import (
    AdvisoryAnalysis,
    AdvisoryHypothesis,
    AdvisoryIncidentFacts,
    AdvisoryRecommendation,
)


class DeterministicAdvisoryAnalyst:
    """Map sanitized facts to a valid analysis citing real signal IDs."""

    identity = ADVISORY_FAKE_IDENTITY

    def analyze(
        self,
        facts: AdvisoryIncidentFacts,
        *,
        analysis_id: str | None = None,
    ) -> AdvisoryAnalysis:
        del analysis_id
        signal_ids = [item.signal_id for item in facts.signals]
        first = signal_ids[0]
        cited = signal_ids[:2] if len(signal_ids) > 1 else [first]
        missing = ", ".join(facts.missing_sources) if facts.missing_sources else "none"
        return AdvisoryAnalysis(
            incident_summary=(
                f"Sanitized job {facts.research_job_id} has "
                f"{len(facts.signals)} closed signals. Missing sources: {missing}."
            ),
            hypotheses=[
                AdvisoryHypothesis(
                    statement=(
                        "Likely cause is described by the cited sanitized signals; "
                        "this is a hypothesis, not an executed action."
                    ),
                    likelihood="medium",
                    signal_ids=cited,
                )
            ],
            recommendations=[
                AdvisoryRecommendation(
                    step=(
                        "Inspect the cited sanitized job and node signals in "
                        "operator tooling before taking any human-approved action."
                    ),
                    action_kind="inspect_state",
                    signal_ids=[first],
                )
            ],
            confidence="medium",
            limitations=[
                "Analysis is non-authoritative and uses sanitized facts only."
            ],
            unknowns=list(facts.missing_sources[:8]),
        )
