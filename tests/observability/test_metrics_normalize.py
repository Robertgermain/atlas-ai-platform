"""Closed HTTP label normalization and exact histogram bucket contracts."""

from __future__ import annotations

import pytest

from atlas.observability.metrics.catalog import (
    HTTP_REQUEST_DURATION_BUCKETS,
    JOB_PROCESSING_DURATION_BUCKETS,
    MODEL_TOOL_ATTEMPT_DURATION_BUCKETS,
    WORKFLOW_NODE_DURATION_BUCKETS,
)
from atlas.observability.metrics.normalize import (
    normalize_http_method,
    normalize_http_route,
    normalize_http_status,
)


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("GET", "GET"),
        ("get", "GET"),
        ("POST", "POST"),
        ("PATCH", "PATCH"),
        ("DELETE", "DELETE"),
        ("TRACE", "other"),
        ("", "other"),
    ],
)
def test_normalize_http_method(method: str, expected: str) -> None:
    assert normalize_http_method(method) == expected


@pytest.mark.parametrize(
    ("route_template", "expected"),
    [
        (None, "unmatched"),
        # ``/health``/``/ready``/``/metrics`` are mounted directly on the
        # app (not under the ``/v1`` sub-router), so their raw
        # ``path_format`` already equals the canonical label.
        ("/health", "/health"),
        ("/ready", "/ready"),
        ("/metrics", "/metrics"),
        # Every route below is mounted under ``atlas.api.v1.router``'s
        # ``/v1`` prefix, but FastAPI's ``_IncludedRouter`` does not fold
        # that prefix into each sub-route's own ``path_format`` -- the raw
        # template a real request's ``scope["route"]`` carries is always
        # prefix-*stripped* (verified against the installed FastAPI
        # version). ``normalize_http_route`` restores the full mounted
        # path for the emitted label; see ``_APPROVED_HTTP_ROUTES``.
        ("/research-jobs", "/v1/research-jobs"),
        ("/research-jobs/{job_id}", "/v1/research-jobs/{job_id}"),
        (
            "/research-jobs/{job_id}/evaluation",
            "/v1/research-jobs/{job_id}/evaluation",
        ),
        (
            "/research-jobs/{job_id}/citations",
            "/v1/research-jobs/{job_id}/citations",
        ),
        (
            "/research-jobs/{job_id}/review-decisions",
            "/v1/research-jobs/{job_id}/review-decisions",
        ),
        ("/evidence/documents", "/v1/evidence/documents"),
        (
            "/evidence/items/{evidence_item_id}",
            "/v1/evidence/items/{evidence_item_id}",
        ),
        # A raw template still carrying the ``/v1`` prefix is exactly what
        # a real matched route never produces (see above) -- it is not a
        # key in the allowlist and must fail safe to ``"other"``, not be
        # silently accepted as if it were the canonical label.
        ("/v1/research-jobs", "other"),
        ("/v1/some-future-unreviewed-route", "other"),
        ("/secret/{token}", "other"),
    ],
)
def test_normalize_http_route(route_template: str | None, expected: str) -> None:
    assert normalize_http_route(route_template) == expected


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (200, "200"),
        (202, "202"),
        (404, "404"),
        (409, "409"),
        (422, "422"),
        (429, "429"),
        (500, "500"),
        (503, "503"),
        (201, "2xx_other"),
        (301, "3xx_other"),
        (401, "4xx_other"),
        (502, "5xx_other"),
        (150, "1xx_other"),
        (0, "other"),
        (-1, "other"),
        (999, "other"),
    ],
)
def test_normalize_http_status(status_code: int, expected: str) -> None:
    assert normalize_http_status(status_code) == expected


@pytest.mark.parametrize(
    "buckets",
    [
        HTTP_REQUEST_DURATION_BUCKETS,
        MODEL_TOOL_ATTEMPT_DURATION_BUCKETS,
        WORKFLOW_NODE_DURATION_BUCKETS,
        JOB_PROCESSING_DURATION_BUCKETS,
    ],
)
def test_bucket_arrays_are_strictly_increasing(buckets: tuple[float, ...]) -> None:
    assert list(buckets) == sorted(set(buckets))
    assert len(buckets) == len(set(buckets))


def test_job_processing_top_finite_bucket_is_300_seconds() -> None:
    assert JOB_PROCESSING_DURATION_BUCKETS[-1] == 300.0


def test_model_tool_attempt_top_finite_bucket_covers_both_configured_timeouts() -> None:
    # Defaults: model_call_timeout_seconds=25.0, tool_attempt_timeout_seconds=8.0.
    assert MODEL_TOOL_ATTEMPT_DURATION_BUCKETS[-1] >= 25.0


def test_workflow_node_top_finite_bucket_covers_research_node_deadline() -> None:
    # Default research_node_tool_deadline_seconds=45.0.
    assert WORKFLOW_NODE_DURATION_BUCKETS[-1] >= 45.0
