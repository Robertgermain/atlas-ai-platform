"""Fail-closed provenance port behavior for evaluation."""

from __future__ import annotations

import pytest

from atlas.evaluation.errors import EvaluationTerminalError
from atlas.evidence.contracts import ClaimStructured
from atlas.workflow.processor import _BoundEvaluationRunner


class _StubRunner:
    def run(self, **kwargs: object) -> object:
        raise AssertionError("not used")


def test_missing_citation_validator_fails_closed_when_claims_exist() -> None:
    bound = _BoundEvaluationRunner(
        runner=_StubRunner(),  # type: ignore[arg-type]
        citation_validator=None,
        evidence_ingest=object(),  # type: ignore[arg-type]
    )
    ok = bound.provenance_ok_for_claims(
        job_id="job-1",
        claims=[ClaimStructured(text="c", evidence_item_ids=["ev-1"])],
    )
    assert ok is False


def test_missing_evidence_resolver_fails_closed_when_claims_exist() -> None:
    class _Validator:
        def validate(self, **kwargs: object) -> None:
            return None

    bound = _BoundEvaluationRunner(
        runner=_StubRunner(),  # type: ignore[arg-type]
        citation_validator=_Validator(),  # type: ignore[arg-type]
        evidence_ingest=None,
    )
    ok = bound.provenance_ok_for_claims(
        job_id="job-1",
        claims=[ClaimStructured(text="c", evidence_item_ids=["ev-1"])],
    )
    assert ok is False


def test_unexpected_resolver_failure_is_sanitized_terminal() -> None:
    class _Validator:
        def validate(self, **kwargs: object) -> None:
            raise RuntimeError("provider secret leaked")

    bound = _BoundEvaluationRunner(
        runner=_StubRunner(),  # type: ignore[arg-type]
        citation_validator=_Validator(),  # type: ignore[arg-type]
        evidence_ingest=object(),  # type: ignore[arg-type]
    )
    with pytest.raises(EvaluationTerminalError) as exc_info:
        bound.provenance_ok_for_claims(
            job_id="job-1",
            claims=[ClaimStructured(text="c", evidence_item_ids=["ev-1"])],
        )
    assert "secret" not in str(exc_info.value)
