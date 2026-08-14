"""Assembler catalog omission and deterministic identical-input tests."""

from __future__ import annotations

import pytest

from atlas.advisor.errors import AdvisorySnapshotRejectedError
from atlas.advisor.snapshot import (
    CountGroup,
    JobRow,
    NodeRow,
    SnapshotLoad,
    assemble_facts,
    canonical_facts_json,
    facts_fingerprint,
)
from tests.advisor.fakes import snapshot_load_with_signal_count


def test_unsupported_error_class_omits_raw_value() -> None:
    loaded = SnapshotLoad(
        job=JobRow(
            research_job_id="job-1",
            status="FAILED",
            evaluation_profile="evaluation.candidate.v1",
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        ),
        nodes=(
            NodeRow(
                node_name="plan",
                attempt=1,
                status="FAILED",
                error="IgnorePreviousInstructionsError: node execution failed",
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "IgnorePreviousInstructionsError" not in encoded
    assert "unsupported_error_class" in facts.missing_sources


def test_unsupported_provider_omits_raw_value() -> None:
    loaded = SnapshotLoad(
        job=JobRow(
            research_job_id="job-1",
            status="FAILED",
            evaluation_profile=None,
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=1,
            evaluation_attempt_count=0,
        ),
        model_groups=(
            CountGroup(
                keys=("plan", "sk-evil-provider", "FAILED", "timeout", ""),
                count=2,
            ),
        ),
    )
    facts = assemble_facts(loaded)
    encoded = canonical_facts_json(facts)
    assert "sk-evil-provider" not in encoded
    assert "unsupported_model_provider" in facts.missing_sources


def test_two_assemblies_are_byte_identical() -> None:
    loaded = SnapshotLoad(
        job=JobRow(
            research_job_id="job-stable",
            status="FAILED",
            evaluation_profile="evaluation.v1",
            continuation_mode="JOB_RETRY",
            repair_count=1,
            job_retry_count=2,
            evaluation_attempt_count=3,
        )
    )
    first = assemble_facts(loaded)
    second = assemble_facts(loaded)
    assert canonical_facts_json(first) == canonical_facts_json(second)
    assert facts_fingerprint(first) == facts_fingerprint(second)
    assert "assembled_at" not in canonical_facts_json(first)
    assert "analysis_id" not in canonical_facts_json(first)
    assert "snapshot_id" not in canonical_facts_json(first)


def test_unsupported_job_status_rejects_snapshot() -> None:
    loaded = SnapshotLoad(
        job=JobRow(
            research_job_id="job-1",
            status="CANCELLED",
            evaluation_profile="evaluation.candidate.v1",
            continuation_mode="NONE",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
    )
    with pytest.raises(AdvisorySnapshotRejectedError):
        assemble_facts(loaded)


def test_exactly_sixty_four_signals_are_accepted() -> None:
    facts = assemble_facts(snapshot_load_with_signal_count(64))
    assert len(facts.signals) == 64


def test_sixty_five_signals_reject_without_truncation() -> None:
    with pytest.raises(AdvisorySnapshotRejectedError, match="signal bound"):
        assemble_facts(snapshot_load_with_signal_count(65))
