"""Ready/health endpoint behavior without requiring a live database."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlalchemy.exc import OperationalError

from atlas.main import app

client = TestClient(app)


def test_health_works_when_database_check_would_fail(monkeypatch: MonkeyPatch) -> None:
    def boom() -> None:
        raise AssertionError("health must not open a database connection")

    monkeypatch.setattr("atlas.persistence.db.get_engine", boom)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_without_exposing_connection_details(
    monkeypatch: MonkeyPatch,
) -> None:
    secret_url = "postgresql+psycopg://atlas:super-secret@127.0.0.1:5432/atlas"

    def fake_engine() -> object:
        return object()

    def fail_ready(_engine: object) -> None:
        raise OperationalError(
            f"could not connect using {secret_url}",
            params=None,
            orig=Exception("connection refused"),
        )

    monkeypatch.setattr("atlas.persistence.db.get_engine", fake_engine)
    monkeypatch.setattr("atlas.persistence.readiness.check_postgres_ready", fail_ready)

    response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert "super-secret" not in response.text
    assert secret_url not in response.text


def test_ready_does_not_hide_unexpected_programming_errors(
    monkeypatch: MonkeyPatch,
) -> None:
    def boom() -> object:
        raise TypeError("unexpected readiness bug")

    monkeypatch.setattr("atlas.persistence.db.get_engine", boom)

    with pytest.raises(TypeError, match="unexpected readiness bug"):
        client.get("/ready")


def test_importing_main_does_not_create_engine(monkeypatch: MonkeyPatch) -> None:
    calls: list[str] = []

    def tracking_get_engine(*_args: object, **_kwargs: object) -> object:
        calls.append("get_engine")
        raise AssertionError("import/use of health must remain lazy")

    monkeypatch.setattr("atlas.persistence.db.get_engine", tracking_get_engine)
    response = client.get("/health")
    assert response.status_code == 200
    assert calls == []
