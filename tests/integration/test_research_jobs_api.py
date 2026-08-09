"""PostgreSQL integration tests for the research-job HTTP API."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.main import app
from atlas.persistence.db import reset_engine_cache


def test_create_get_and_idempotent_replay_against_postgres(
    session_factory: sessionmaker[Session],
) -> None:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    client = TestClient(app)

    try:
        create = client.post(
            "/v1/research-jobs",
            json={"question": "Integrate Atlas persistence"},
            headers={"Idempotency-Key": "integration-key-1"},
        )
        assert create.status_code == 202
        body = create.json()
        assert body["status"] == "PENDING"
        assert body["question"] == "Integrate Atlas persistence"
        assert "idempotency_key" not in body

        replay = client.post(
            "/v1/research-jobs",
            json={"question": "Integrate Atlas persistence"},
            headers={"Idempotency-Key": "integration-key-1"},
        )
        assert replay.status_code == 202
        assert replay.json()["id"] == body["id"]

        conflict = client.post(
            "/v1/research-jobs",
            json={"question": "Different question"},
            headers={"Idempotency-Key": "integration-key-1"},
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "idempotency_key_conflict"
        assert "integration-key-1" not in conflict.text

        fetched = client.get(f"/v1/research-jobs/{body['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["id"] == body["id"]

        missing = client.get("/v1/research-jobs/does-not-exist")
        assert missing.status_code == 404
    finally:
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_concurrent_duplicate_submissions_share_one_job(
    session_factory: sessionmaker[Session],
) -> None:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory

    def _submit() -> tuple[int, str]:
        local_client = TestClient(app)
        response = local_client.post(
            "/v1/research-jobs",
            json={"question": "concurrent question"},
            headers={"Idempotency-Key": "concurrent-key"},
        )
        return response.status_code, response.json()["id"]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(_submit)
            second = executor.submit(_submit)
            status_a, id_a = first.result()
            status_b, id_b = second.result()

        assert status_a == 202
        assert status_b == 202
        assert id_a == id_b
    finally:
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_concurrent_conflicting_payloads_yield_one_winner(
    session_factory: sessionmaker[Session],
) -> None:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    key = "concurrent-conflict-key"

    def _submit(question: str) -> tuple[int, dict[str, Any]]:
        local_client = TestClient(app)
        response = local_client.post(
            "/v1/research-jobs",
            json={"question": question},
            headers={"Idempotency-Key": key},
        )
        payload = response.json()
        assert isinstance(payload, dict)
        return response.status_code, payload

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(_submit, "payload-a")
            second = executor.submit(_submit, "payload-b")
            status_a, body_a = first.result()
            status_b, body_b = second.result()

        statuses = {status_a, status_b}
        assert statuses == {202, 409}

        winner = body_a if status_a == 202 else body_b
        loser = body_b if status_a == 202 else body_a
        assert winner["status"] == "PENDING"
        assert winner["question"] in {"payload-a", "payload-b"}
        assert loser["error"]["code"] == "idempotency_key_conflict"
        assert key not in str(body_a)
        assert key not in str(body_b)

        with session_factory() as session:
            count = session.execute(
                text("SELECT COUNT(*) FROM research_jobs WHERE idempotency_key = :key"),
                {"key": key},
            ).scalar_one()
        assert count == 1
    finally:
        app.dependency_overrides.clear()
        reset_engine_cache()
