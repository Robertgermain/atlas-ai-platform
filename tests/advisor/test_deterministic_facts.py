"""Deterministic facts fingerprint is internal and not model-visible."""

from __future__ import annotations

from atlas.advisor.prompt import render_advisory_prompts
from atlas.advisor.snapshot import (
    JobRow,
    SnapshotLoad,
    assemble_facts,
    canonical_facts_json,
    facts_fingerprint,
)


def test_fingerprint_is_not_in_model_prompt() -> None:
    loaded = SnapshotLoad(
        job=JobRow(
            research_job_id="job-1",
            status="FAILED",
            evaluation_profile="evaluation.candidate.v1",
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
    )
    facts = assemble_facts(loaded)
    digest = facts_fingerprint(facts)
    _system, user = render_advisory_prompts(facts)
    assert digest not in user
    assert digest not in canonical_facts_json(facts)
    assert len(digest) == 64
