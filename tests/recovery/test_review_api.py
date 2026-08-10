"""Unit tests for the operator review API (disabled by default)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def _disabled_review_client() -> TestClient:
    """Client with review API disabled (default)."""
    import os

    os.environ.pop("ATLAS_REVIEW_API_ENABLED", None)
    from atlas.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_review_api_returns_404_when_disabled(
    _disabled_review_client: TestClient,
) -> None:
    """POST /v1/research-jobs/{id}/review-decisions returns 404 when disabled."""
    response = _disabled_review_client.post(
        "/v1/research-jobs/nonexistent-job/review-decisions",
        json={"decision": "approve", "actor_id": "operator"},
        headers={"Idempotency-Key": "test-key-1"},
    )
    assert response.status_code == 404
