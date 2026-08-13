"""``/metrics`` exposition, HTTP middleware labels, and readiness-failure metric.

These tests exercise the real module-level ``atlas.main.app`` (and therefore
the process-wide ``default_metrics()`` singleton bound to
``prometheus_client.REGISTRY``), so assertions look for the *presence* of an
expected sample rather than an exact count -- other tests in the same process
share this registry and may also increment ``atlas_http_requests_total``.
"""

from __future__ import annotations

import re

from fastapi.testclient import TestClient
from prometheus_client import CONTENT_TYPE_LATEST
from pytest import MonkeyPatch
from sqlalchemy.exc import OperationalError

from atlas.api.deps import provide_evaluation_service, provide_research_job_service
from atlas.application.exceptions import ResearchJobLookupError
from atlas.main import app
from atlas.observability.events import Event
from atlas.observability.testing import capture_logs

client = TestClient(app)

#: Greedy up to the *last* ``}`` on the line, not the first: a route-label
#: value can itself contain ``{``/``}`` (e.g. ``/v1/research-jobs/{job_id}``),
#: so naively splitting on the first ``}`` would truncate the label section
#: mid-value. Label values here never contain a literal ``"``, so this is
#: unambiguous.
_SAMPLE_LINE_PATTERN = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.*)\}\s+(\S+)$")


def _http_request_samples() -> list[tuple[dict[str, str], float]]:
    body = client.get("/metrics").text
    samples: list[tuple[dict[str, str], float]] = []
    for line in body.splitlines():
        if not line.startswith("atlas_http_requests_total{"):
            continue
        match = _SAMPLE_LINE_PATTERN.match(line)
        assert match is not None, line
        _metric_name, labels_part, value = match.groups()
        labels: dict[str, str] = {}
        for pair in labels_part.split(","):
            key, _, raw_value = pair.partition("=")
            labels[key] = raw_value.strip('"')
        samples.append((labels, float(value)))
    return samples


def test_metrics_endpoint_returns_prometheus_exposition_format() -> None:
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST
    assert "atlas_http_requests_total" in response.text
    assert "atlas_http_request_duration_seconds" in response.text


def test_metrics_endpoint_returns_sanitized_503_when_exposition_fails(
    monkeypatch: MonkeyPatch,
) -> None:
    """A ``generate_latest()`` failure must return a controlled 503, not a
    raw 500/traceback, and must not affect any other route."""

    def _boom(_metrics: object) -> tuple[bytes, str]:
        raise RuntimeError("registry-secret-corruption")

    monkeypatch.setattr("atlas.observability.metrics.exposition.render_metrics", _boom)

    with capture_logs("atlas.observability.metrics.exposition") as captured:
        response = client.get("/metrics")

    assert response.status_code == 503
    assert response.headers["content-type"] != CONTENT_TYPE_LATEST
    assert "registry-secret-corruption" not in response.text
    assert Event.METRIC_EXPOSITION_FAILED.value in captured.events

    # Ordinary application processing is unaffected by the exposition failure.
    health_response = client.get("/health")
    assert health_response.status_code == 200


def test_health_request_recorded_with_approved_route_and_status_labels() -> None:
    client.get("/health")

    samples = _http_request_samples()
    matching = [
        labels
        for labels, _ in samples
        if labels.get("route") == "/health" and labels.get("method") == "GET"
    ]
    assert matching, samples
    assert matching[0]["status"] == "200"


def test_nested_v1_router_route_recorded_with_its_full_mounted_path() -> None:
    """Regression guard: FastAPI's ``_IncludedRouter`` does not fold the
    parent router's ``/v1`` prefix into a matched sub-route's own
    ``path_format`` -- a real request's ``scope["route"].path_format`` for
    this endpoint is the prefix-stripped ``"/research-jobs/{job_id}"``, not
    ``"/v1/research-jobs/{job_id}"``. The emitted ``route`` label must still
    be the full, human-readable mounted path, not the stripped template and
    not ``"other"``.

    The route is DB-backed in production; this isolated test overrides the
    application services so a missing job returns 404 without PostgreSQL.
    """

    class _MissingJobService:
        def get(self, job_id: str) -> None:
            raise ResearchJobLookupError(job_id)

    class _EmptyEvaluationService:
        def get_by_job(self, job_id: str) -> list[object]:
            return []

    app.dependency_overrides[provide_research_job_service] = lambda: (
        _MissingJobService()
    )
    app.dependency_overrides[provide_evaluation_service] = lambda: (
        _EmptyEvaluationService()
    )
    try:
        response = client.get("/v1/research-jobs/does-not-exist")
        assert response.status_code == 404  # job not found, but the route matched

        samples = _http_request_samples()
        matching = [
            labels
            for labels, _ in samples
            if labels.get("route") == "/v1/research-jobs/{job_id}"
            and labels.get("method") == "GET"
            and labels.get("status") == "404"
        ]
        assert matching, samples
        assert all(
            labels.get("route") != "/research-jobs/{job_id}" for labels, _ in samples
        )
    finally:
        app.dependency_overrides.pop(provide_research_job_service, None)
        app.dependency_overrides.pop(provide_evaluation_service, None)


def test_unmatched_route_recorded_as_unmatched_not_raw_path() -> None:
    response = client.get("/this-path-does-not-exist")
    assert response.status_code == 404

    samples = _http_request_samples()
    matching = [
        labels
        for labels, _ in samples
        if labels.get("route") == "unmatched" and labels.get("method") == "GET"
    ]
    assert matching, samples
    assert matching[0]["status"] == "404"
    assert all(
        "this-path-does-not-exist" not in labels.get("route", "")
        for labels, _ in samples
    )


def _readiness_failure_total() -> float:
    for line in client.get("/metrics").text.splitlines():
        if line.startswith("atlas_database_readiness_failures_total "):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def test_ready_failure_increments_database_readiness_failure_metric(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_engine() -> object:
        return object()

    def fail_ready(_engine: object) -> None:
        raise OperationalError(
            "could not connect", params=None, orig=Exception("connection refused")
        )

    monkeypatch.setattr("atlas.persistence.db.get_engine", fake_engine)
    monkeypatch.setattr("atlas.persistence.readiness.check_postgres_ready", fail_ready)

    before = _readiness_failure_total()

    response = client.get("/ready")
    assert response.status_code == 503

    after = _readiness_failure_total()
    assert after == before + 1
