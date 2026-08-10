"""API contract tests for research-job routes."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError, OperationalError

from atlas.api.deps import provide_evaluation_service, provide_research_job_service
from atlas.application.exceptions import (
    IdempotencyConflictError,
    ResearchJobLookupError,
)
from atlas.domain import ResearchJob
from atlas.evaluation.contracts import EVALUATION_PROFILE, EvaluationRunResult
from atlas.main import app

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
client = TestClient(app, raise_server_exceptions=True)


class _FakeService:
    def __init__(self) -> None:
        self.jobs: dict[str, ResearchJob] = {}
        self.by_key: dict[str, tuple[ResearchJob, str]] = {}
        self.submit_error: Exception | None = None
        self.get_error: Exception | None = None

    def submit(self, question: str, *, idempotency_key: str) -> ResearchJob:
        if self.submit_error is not None:
            raise self.submit_error
        existing = self.by_key.get(idempotency_key)
        if existing is not None:
            job, fingerprint = existing
            # Fingerprint comparison is owned by the real service; this fake
            # only models success/conflict outcomes for HTTP mapping tests.
            if job.question != question.strip():
                raise IdempotencyConflictError()
            return job
        job = ResearchJob.create(
            "11111111-1111-4111-8111-111111111111", question, at=T0
        )
        self.jobs[job.id] = job
        self.by_key[idempotency_key] = (job, "fingerprint")
        return job

    def get(self, job_id: str) -> ResearchJob:
        if self.get_error is not None:
            raise self.get_error
        job = self.jobs.get(job_id)
        if job is None:
            raise ResearchJobLookupError(job_id)
        return job


class _FakeEvaluationService:
    """In-memory evaluation reads for isolated API unit tests (no Postgres)."""

    def __init__(self) -> None:
        self.by_job: dict[str, list[EvaluationRunResult]] = {}
        self.get_by_job_error: Exception | None = None

    def get_by_job(self, job_id: str) -> list[EvaluationRunResult]:
        if self.get_by_job_error is not None:
            raise self.get_by_job_error
        return list(self.by_job.get(job_id, []))

    def seed_succeeded(
        self,
        *,
        job_id: str,
        run_id: str = "eval-run-1",
        aggregate_score: float = 0.91,
    ) -> EvaluationRunResult:
        result = EvaluationRunResult(
            run_id=run_id,
            research_job_id=job_id,
            workflow_execution_id="exec-1",
            evaluation_profile=EVALUATION_PROFILE,
            evaluation_attempt=1,
            status="SUCCEEDED",
            input_fingerprint="a" * 64,
            passed=True,
            aggregate_score=aggregate_score,
            disposition_hint="complete",
            dimensions=[],
            grader_versions={"citation_integrity": "deterministic.v1"},
        )
        self.by_job[job_id] = [result]
        return result


@pytest.fixture
def api_fakes() -> Iterator[tuple[_FakeService, _FakeEvaluationService]]:
    service = _FakeService()
    evaluation = _FakeEvaluationService()
    app.dependency_overrides[provide_research_job_service] = lambda: service
    app.dependency_overrides[provide_evaluation_service] = lambda: evaluation
    yield service, evaluation
    app.dependency_overrides.clear()


@pytest.fixture
def fake_service(
    api_fakes: tuple[_FakeService, _FakeEvaluationService],
) -> _FakeService:
    return api_fakes[0]


@pytest.fixture
def fake_evaluation(
    api_fakes: tuple[_FakeService, _FakeEvaluationService],
) -> _FakeEvaluationService:
    return api_fakes[1]


def test_create_research_job_returns_202(fake_service: _FakeService) -> None:
    response = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-1"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["id"] == "11111111-1111-4111-8111-111111111111"
    assert body["question"] == "What is Atlas?"
    assert body["status"] == "PENDING"
    assert body["result"] is None
    assert body["failure_reason"] is None
    assert "idempotency_key" not in body
    assert "request_fingerprint" not in body
    assert "key-1" not in response.text


def test_get_research_job_returns_200(fake_service: _FakeService) -> None:
    created = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-1"},
    ).json()

    response = client.get(f"/v1/research-jobs/{created['id']}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == created["id"]
    assert body["evaluation_summary"] is None


def test_get_research_job_includes_evaluation_summary(
    fake_service: _FakeService,
    fake_evaluation: _FakeEvaluationService,
) -> None:
    created = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-eval-summary"},
    ).json()
    fake_evaluation.seed_succeeded(
        job_id=created["id"],
        aggregate_score=0.91,
    )

    response = client.get(f"/v1/research-jobs/{created['id']}")

    assert response.status_code == 200
    summary = response.json()["evaluation_summary"]
    assert summary is not None
    assert summary["passed"] is True
    assert summary["aggregate_score"] == pytest.approx(0.91)
    assert summary["profile"] == EVALUATION_PROFILE
    assert summary["disposition_hint"] == "complete"
    assert "input_fingerprint" not in summary
    assert "job_claim_fingerprint" not in summary


def test_get_evaluation_returns_404_when_missing(fake_service: _FakeService) -> None:
    created = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-eval-missing"},
    ).json()

    response = client.get(f"/v1/research-jobs/{created['id']}/evaluation")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "evaluation_not_found"


def test_get_evaluation_returns_200_when_present(
    fake_service: _FakeService,
    fake_evaluation: _FakeEvaluationService,
) -> None:
    created = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-eval-present"},
    ).json()
    seeded = fake_evaluation.seed_succeeded(
        job_id=created["id"],
        run_id="eval-detail-1",
        aggregate_score=0.88,
    )

    response = client.get(f"/v1/research-jobs/{created['id']}/evaluation")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == seeded.run_id
    assert body["research_job_id"] == created["id"]
    assert body["status"] == "SUCCEEDED"
    assert body["passed"] is True
    assert body["aggregate_score"] == pytest.approx(0.88)
    assert body["evaluation_profile"] == EVALUATION_PROFILE
    assert "input_fingerprint" not in body
    assert "job_claim_fingerprint" not in body
    assert "ownership_token" not in body


def test_get_unknown_job_returns_structured_404(fake_service: _FakeService) -> None:
    response = client.get("/v1/research-jobs/missing-id")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "research_job_not_found"
    assert body["error"]["details"]["job_id"] == "missing-id"


def test_get_evaluation_unknown_job_returns_404(fake_service: _FakeService) -> None:
    response = client.get("/v1/research-jobs/missing-id/evaluation")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "research_job_not_found"


def test_evaluation_operational_error_returns_503(
    fake_service: _FakeService,
    fake_evaluation: _FakeEvaluationService,
) -> None:
    """Production mapping: evaluation DB failures remain controlled 503."""
    created = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "key-eval-503"},
    ).json()
    secret = "postgresql+psycopg://atlas:super-secret@127.0.0.1:5432/atlas"
    fake_evaluation.get_by_job_error = OperationalError(
        f"could not connect using {secret}",
        params=None,
        orig=Exception("connection refused"),
    )

    response = client.get(f"/v1/research-jobs/{created['id']}")

    assert response.status_code == 503
    body = response.json()
    assert body["error"]["code"] == "service_unavailable"
    assert "super-secret" not in response.text


def test_blank_question_returns_structured_422(fake_service: _FakeService) -> None:
    response = client.post(
        "/v1/research-jobs",
        json={"question": "   "},
        headers={"Idempotency-Key": "key-1"},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "request_validation_failed"
    assert "issues" in body["error"]["details"]


def test_question_is_trimmed_before_submit(fake_service: _FakeService) -> None:
    response = client.post(
        "/v1/research-jobs",
        json={"question": "  What is Atlas?  "},
        headers={"Idempotency-Key": "trim-key"},
    )

    assert response.status_code == 202
    assert response.json()["question"] == "What is Atlas?"


def test_question_length_boundaries(fake_service: _FakeService) -> None:
    too_long = client.post(
        "/v1/research-jobs",
        json={"question": "x" * 8001},
        headers={"Idempotency-Key": "key-long"},
    )
    assert too_long.status_code == 422

    ok = client.post(
        "/v1/research-jobs",
        json={"question": "x" * 8000},
        headers={"Idempotency-Key": "key-ok"},
    )
    assert ok.status_code == 202


def test_missing_idempotency_key_returns_structured_422(
    fake_service: _FakeService,
) -> None:
    response = client.post("/v1/research-jobs", json={"question": "What is Atlas?"})

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "request_validation_failed"


def test_whitespace_only_idempotency_key_returns_structured_422(
    fake_service: _FakeService,
) -> None:
    response = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "   "},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "request_validation_failed"
    assert "   " not in response.text


def test_idempotency_key_length_boundaries(fake_service: _FakeService) -> None:
    accepted_key = "k" * 128
    accepted = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": accepted_key},
    )
    assert accepted.status_code == 202
    assert accepted_key not in accepted.text

    rejected_key = "k" * 129
    rejected = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": rejected_key},
    )
    assert rejected.status_code == 422
    body = rejected.json()
    assert body["error"]["code"] == "request_validation_failed"
    assert rejected_key not in rejected.text


def test_idempotent_replay_returns_202(fake_service: _FakeService) -> None:
    first = client.post(
        "/v1/research-jobs",
        json={"question": "same"},
        headers={"Idempotency-Key": "replay-key"},
    )
    second = client.post(
        "/v1/research-jobs",
        json={"question": "same"},
        headers={"Idempotency-Key": "replay-key"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["id"] == second.json()["id"]


def test_idempotency_conflict_returns_409(fake_service: _FakeService) -> None:
    client.post(
        "/v1/research-jobs",
        json={"question": "one"},
        headers={"Idempotency-Key": "conflict-key"},
    )
    response = client.post(
        "/v1/research-jobs",
        json={"question": "two"},
        headers={"Idempotency-Key": "conflict-key"},
    )

    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == "idempotency_key_conflict"
    assert "conflict-key" not in response.text


def test_operational_error_returns_503(fake_service: _FakeService) -> None:
    secret = "postgresql+psycopg://atlas:super-secret@127.0.0.1:5432/atlas"
    fake_service.submit_error = OperationalError(
        f"could not connect using {secret}",
        params=None,
        orig=Exception("connection refused"),
    )

    response = client.post(
        "/v1/research-jobs",
        json={"question": "What is Atlas?"},
        headers={"Idempotency-Key": "db-down"},
    )

    assert response.status_code == 503
    body = response.json()
    assert body["error"]["code"] == "service_unavailable"
    assert "super-secret" not in response.text
    assert secret not in response.text


def test_integrity_error_is_not_mapped_to_503(fake_service: _FakeService) -> None:
    fake_service.submit_error = IntegrityError(
        "INSERT",
        {},
        Exception("check constraint"),
    )

    with pytest.raises(IntegrityError):
        client.post(
            "/v1/research-jobs",
            json={"question": "What is Atlas?"},
            headers={"Idempotency-Key": "integrity"},
        )


def test_unexpected_programming_error_is_not_hidden(
    fake_service: _FakeService,
) -> None:
    fake_service.submit_error = TypeError("unexpected bug")

    with pytest.raises(TypeError, match="unexpected bug"):
        client.post(
            "/v1/research-jobs",
            json={"question": "What is Atlas?"},
            headers={"Idempotency-Key": "boom"},
        )


def test_openapi_includes_research_job_paths(fake_service: _FakeService) -> None:
    del fake_service
    schema = client.get("/openapi.json").json()
    assert "/v1/research-jobs" in schema["paths"]
    assert "/v1/research-jobs/{job_id}" in schema["paths"]
    assert "/v1/research-jobs/{job_id}/evaluation" in schema["paths"]

    request_schema = schema["components"]["schemas"]["CreateResearchJobRequest"]
    question = request_schema["properties"]["question"]
    assert question["minLength"] == 1
    assert question["maxLength"] == 8000
