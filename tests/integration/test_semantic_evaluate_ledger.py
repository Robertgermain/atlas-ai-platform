"""Malformed-only semantic evaluate retry against the model ledger (Slice 15C1)."""

from __future__ import annotations

import json
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel, ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

import atlas.models.service as model_service
from atlas.domain.research_job import ResearchJob
from atlas.evaluation.llm_grader import dimension_from_semantic_output
from atlas.evaluation.semantic_contracts import (
    SemanticClaimInput,
    SemanticClaimSupport,
    SemanticExcerptInput,
    SemanticGradeRequest,
    SemanticGroundednessOutput,
)
from atlas.models.contracts import (
    FinishOutcome,
    ModelCallMeta,
    ProviderId,
)
from atlas.models.errors import (
    ModelAttemptOwnershipLostError,
    ModelAuthConfigError,
    ModelInvalidStructuredOutputError,
    ModelRateLimitedError,
    ModelRefusalError,
    ModelTimeoutError,
    ModelUnknownError,
)
from atlas.models.service import ModelInvocationService
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository


def _seed_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    repo = SqlAlchemyResearchJobRepository()
    workflow = SqlAlchemyWorkflowRepository()
    now = datetime.now(UTC)
    with session_scope(session_factory) as session:
        job = ResearchJob.create(id=job_id, question="semantic ledger", at=now)
        job.start(at=now)
        repo.add(
            session,
            job,
            idempotency_key=f"idem-{job_id}",
            request_fingerprint="a" * 64,
        )
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET claim_token = :token, lease_expires_at = :lease
                WHERE id = :id
                """
            ),
            {
                "token": "b" * 64,
                "lease": now + timedelta(seconds=90),
                "id": job_id,
            },
        )
        return workflow.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=now,
        )


def _request(job_id: str, *, claim_count: int = 1) -> SemanticGradeRequest:
    claims = [
        SemanticClaimInput(
            claim_ordinal=index,
            text=f"Claim {index}",
            evidence_item_ids=["ev-1"],
        )
        for index in range(1, claim_count + 1)
    ]
    return SemanticGradeRequest(
        job_id=job_id,
        claims=claims,
        excerpts=[
            SemanticExcerptInput(
                evidence_item_id="ev-1",
                trust_label="[untrusted_source]",
                text="The sky is blue today.",
            )
        ],
    )


def _valid_output() -> SemanticGroundednessOutput:
    return SemanticGroundednessOutput(
        claims=[SemanticClaimSupport(claim_ordinal=1, score=1.0)],
    )


def _meta() -> ModelCallMeta:
    return ModelCallMeta(
        provider=ProviderId.OPENAI,
        model="gpt-4o-mini",
        prompt_version="semantic_groundedness.v1",
        latency_ms=10,
        input_tokens=4,
        output_tokens=2,
        estimated_cost_usd=0.0,
        finish_outcome=FinishOutcome.COMPLETED,
    )


def _service(
    session_factory: sessionmaker[Session],
) -> ModelInvocationService:
    return ModelInvocationService(
        session_factory=session_factory,
        chat_model=MagicMock(),
        provider=ProviderId.OPENAI,
        model_name="gpt-4o-mini",
        call_timeout_seconds=25.0,
    )


def _attempt_rows(session_factory: sessionmaker[Session]) -> Sequence[Any]:
    with session_scope(session_factory) as session:
        return (
            session.execute(
                text(
                    """
                    SELECT a.status, a.error_class, i.node_name
                    FROM model_invocation_attempts a
                    JOIN model_invocations i ON i.id = a.invocation_id
                    ORDER BY a.attempt
                    """
                )
            )
            .mappings()
            .all()
        )


def test_first_malformed_second_valid(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-retry-ok-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ModelInvalidStructuredOutputError()
        return _valid_output(), _meta()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    output, _call_meta = service.evaluate_semantic(
        _request(job_id), workflow_execution_id=execution_id
    )
    assert output.claims[0].score == 1.0
    assert calls["n"] == 2
    rows = _attempt_rows(session_factory)
    assert [row["status"] for row in rows] == ["FAILED", "SUCCEEDED"]
    assert rows[0]["error_class"] == "ModelInvalidStructuredOutputError"
    assert rows[0]["node_name"] == "evaluate"
    assert rows[1]["node_name"] == "evaluate"


def test_two_malformed_prevent_third_provider_call(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-retry-cap-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    rows = _attempt_rows(session_factory)
    assert len(rows) == 2
    assert all(row["status"] == "FAILED" for row in rows)


def test_crash_after_first_malformed_permits_only_remaining_attempt(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-crash-one-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    orig = service._execute
    execute_calls = {"n": 0}

    def _crash_before_second(*args: Any, **kwargs: Any) -> Any:
        execute_calls["n"] += 1
        if execute_calls["n"] >= 2:
            raise RuntimeError("simulated crash before second attempt")
        return orig(*args, **kwargs)

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    monkeypatch.setattr(service, "_execute", _crash_before_second)
    with pytest.raises(RuntimeError, match="simulated crash"):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    monkeypatch.setattr(service, "_execute", orig)
    invokes = {"n": 0}

    def _second_process(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        invokes["n"] += 1
        return _valid_output(), _meta()

    monkeypatch.setattr(model_service, "invoke_structured", _second_process)
    output, _meta_out = service.evaluate_semantic(
        _request(job_id), workflow_execution_id=execution_id
    )
    assert output.claims[0].score == 1.0
    assert invokes["n"] == 1


def test_crash_after_two_malformed_permits_no_call(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-crash-two-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2


def test_concurrent_callers_cannot_exceed_malformed_cap(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-conc-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    lock = threading.Lock()
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        with lock:
            calls["n"] += 1
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    errors: list[BaseException] = []

    def _worker() -> None:
        try:
            service.evaluate_semantic(
                _request(job_id), workflow_execution_id=execution_id
            )
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
        assert not thread.is_alive()
    assert calls["n"] <= 2
    rows = _attempt_rows(session_factory)
    malformed = [
        row for row in rows if row["error_class"] == "ModelInvalidStructuredOutputError"
    ]
    assert len(malformed) <= 2
    assert errors


@pytest.mark.parametrize(
    "error",
    [
        ModelTimeoutError(),
        ModelRateLimitedError(),
        ModelAuthConfigError(),
        ModelRefusalError(),
    ],
)
def test_non_malformed_errors_do_not_consume_retry(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    job_id = f"sem-other-{type(error).__name__}-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        raise error

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(type(error)):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 1
    rows = _attempt_rows(session_factory)
    assert len(rows) == 1
    assert rows[0]["error_class"] == type(error).__name__


def test_lost_ownership_cannot_finalize(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-own-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    service._repository.fail_attempt = (  # type: ignore[method-assign]
        lambda *args, **kwargs: False
    )

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        raise ModelInvalidStructuredOutputError()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(ModelAttemptOwnershipLostError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    rows = _attempt_rows(session_factory)
    assert len(rows) == 1
    assert rows[0]["status"] == "STARTED"


def test_provider_aggregate_score_is_malformed_and_consumes_cap(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-agg-extra-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        schema_type = kwargs["schema"]
        parsed = {
            "claims": [
                {"claim_ordinal": 1, "score": 0.0},
            ],
            "aggregate_score": 1.0,
        }
        if not (isinstance(schema_type, type) and issubclass(schema_type, BaseModel)):
            raise TypeError("structured schema is required")
        try:
            validated = schema_type.model_validate(parsed)
        except ValidationError:
            raise ModelInvalidStructuredOutputError() from None
        return validated, _meta()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    rows = _attempt_rows(session_factory)
    assert len(rows) == 2
    assert all(row["status"] == "FAILED" for row in rows)
    assert all(
        row["error_class"] == "ModelInvalidStructuredOutputError" for row in rows
    )


def test_provider_support_field_is_malformed_and_consumes_cap(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-support-extra-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        schema_type = kwargs["schema"]
        parsed = {
            "claims": [
                {
                    "claim_ordinal": 1,
                    "support": "unsupported",
                    "score": 1.0,
                }
            ],
        }
        if not (isinstance(schema_type, type) and issubclass(schema_type, BaseModel)):
            raise TypeError("structured schema is required")
        try:
            validated = schema_type.model_validate(parsed)
        except ValidationError:
            raise ModelInvalidStructuredOutputError() from None
        return validated, _meta()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    with pytest.raises(ModelInvalidStructuredOutputError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 2
    rows = _attempt_rows(session_factory)
    assert len(rows) == 2
    assert all(row["status"] == "FAILED" for row in rows)
    assert all(
        row["error_class"] == "ModelInvalidStructuredOutputError" for row in rows
    )


def test_ledger_replay_recomputes_atlas_mean_and_rejects_stored_aggregate(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-replay-agg-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        return (
            SemanticGroundednessOutput(
                claims=[SemanticClaimSupport(claim_ordinal=1, score=0.0)],
            ),
            _meta(),
        )

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    output, _call_meta = service.evaluate_semantic(
        _request(job_id), workflow_execution_id=execution_id
    )
    assert "aggregate_score" not in output.model_dump()
    mapped = dimension_from_semantic_output(output)
    assert mapped.passed is False
    assert mapped.score == 0.0
    assert calls["n"] == 1

    replayed, _replay_meta = service.evaluate_semantic(
        _request(job_id), workflow_execution_id=execution_id
    )
    assert calls["n"] == 1
    assert "aggregate_score" not in replayed.model_dump()
    replay_mapped = dimension_from_semantic_output(replayed)
    assert replay_mapped.passed is False
    assert replay_mapped.score == 0.0

    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE model_invocations
                SET output_json = CAST(:payload AS jsonb)
                WHERE research_job_id = :job_id AND node_name = 'evaluate'
                """
            ),
            {
                "payload": json.dumps(
                    {
                        "claims": [
                            {
                                "claim_ordinal": 1,
                                "score": 0.0,
                            }
                        ],
                        "aggregate_score": 1.0,
                    }
                ),
                "job_id": job_id,
            },
        )

    with pytest.raises(ModelUnknownError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 1


def test_ledger_replay_preserves_passing_mean_with_unsupported_claim(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-replay-pass-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        return (
            SemanticGroundednessOutput(
                claims=[
                    SemanticClaimSupport(claim_ordinal=1, score=1.0),
                    SemanticClaimSupport(claim_ordinal=2, score=1.0),
                    SemanticClaimSupport(claim_ordinal=3, score=1.0),
                    SemanticClaimSupport(claim_ordinal=4, score=0.0),
                ],
            ),
            _meta(),
        )

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    request = _request(job_id, claim_count=4)
    output, _call_meta = service.evaluate_semantic(
        request, workflow_execution_id=execution_id
    )
    mapped = dimension_from_semantic_output(output)
    assert mapped.score == 0.75
    assert mapped.passed is True
    assert mapped.failure_codes == []
    assert calls["n"] == 1

    replayed, _replay_meta = service.evaluate_semantic(
        request, workflow_execution_id=execution_id
    )
    assert calls["n"] == 1
    replay_mapped = dimension_from_semantic_output(replayed)
    assert replay_mapped.score == mapped.score
    assert replay_mapped.passed is True
    assert replay_mapped.failure_codes == []


def test_ledger_replay_rejects_stored_provider_support(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = f"sem-replay-support-{uuid4()}"
    execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
    service = _service(session_factory)
    calls = {"n": 0}

    def _invoke(**_kwargs: object) -> tuple[object, ModelCallMeta]:
        calls["n"] += 1
        return _valid_output(), _meta()

    monkeypatch.setattr(model_service, "invoke_structured", _invoke)
    service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 1

    with session_scope(session_factory) as session:
        session.execute(
            text(
                """
                UPDATE model_invocations
                SET output_json = CAST(:payload AS jsonb)
                WHERE research_job_id = :job_id AND node_name = 'evaluate'
                """
            ),
            {
                "payload": json.dumps(
                    {
                        "claims": [
                            {
                                "claim_ordinal": 1,
                                "support": "supported",
                                "score": 1.0,
                            }
                        ],
                    }
                ),
                "job_id": job_id,
            },
        )

    with pytest.raises(ModelUnknownError):
        service.evaluate_semantic(_request(job_id), workflow_execution_id=execution_id)
    assert calls["n"] == 1
