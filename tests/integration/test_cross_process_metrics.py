"""Focused cross-process Prometheus metrics scenario (Slice 15A2).

Every Atlas process (API, worker, outbox relay, Kafka consumer) constructs
its own :class:`~atlas.observability.metrics.catalog.AtlasMetrics` bound to
that process's own ``prometheus_client`` default registry -- there is no
shared/global metrics store across processes. This module proves that
end-to-end, against the real Docker Compose application stack, by:

1. Scraping each of the four roles' own ``/metrics`` endpoint independently
   and confirming a metric family only that role can produce (a *labeled*
   metric, so it carries no sample line until actually observed -- see
   module note below) never carries a sample in another role's output.
2. Submitting one real research job through the API and confirming the
   resulting claim/processing, publication, and consumer-apply activity is
   each independently visible on exactly the role that produced it.

Every Atlas process constructs the identical, complete
:class:`AtlasMetrics` catalog (one shared class, not four role-specific
subclasses) -- so every metric family's ``# HELP``/``# TYPE`` lines appear
in every role's exposition text regardless of role, and an *unlabeled*
Counter/Gauge (e.g. ``atlas_research_job_submissions_total``,
``atlas_outbox_backlog_size``) always carries a permanent ``0.0``/unset
sample everywhere. Neither is a leak: no label value or observation ever
appears in a process that did not produce it. This module's assertions
therefore key off a *labeled* metric's actual sample line (e.g.
``atlas_worker_claims_total{outcome="..."``), which only exists once that
specific process has called the corresponding ``observe_*`` method.

Requires the full Compose stack (postgres/redis/kafka plus the
api/worker/outbox-relay/consumer application services) already running via
``docker compose up -d`` with an image built from the current source tree.
This module never starts, stops, or rebuilds the stack itself -- that is an
explicit developer/CI step, since picking up code changes requires an image
rebuild. The whole module is skipped if Compose is unavailable or any of
the four application containers is not currently running.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
API_BASE_URL = "http://127.0.0.1:8000"
_ROLE_SERVICES = ("api", "worker", "outbox-relay", "consumer")


def _docker_compose_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _running_services() -> frozenset[str]:
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--status", "running", "--services"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            cwd=REPO_ROOT,
        )
    except (OSError, subprocess.TimeoutExpired):
        return frozenset()
    if result.returncode != 0:
        return frozenset()
    return frozenset(result.stdout.split())


_COMPOSE_AVAILABLE = _docker_compose_available()
_RUNNING_SERVICES = _running_services() if _COMPOSE_AVAILABLE else frozenset()
_STACK_UP = _COMPOSE_AVAILABLE and set(_ROLE_SERVICES) <= _RUNNING_SERVICES

pytestmark = pytest.mark.skipif(
    not _STACK_UP,
    reason=(
        "Full docker compose application stack "
        "(api/worker/outbox-relay/consumer) is not running"
    ),
)


def _metrics_in_container(service: str) -> str:
    """Scrape ``/metrics`` from inside ``service``'s own internal-only port 9464.

    Uses the stdlib ``urllib`` (no curl in the minimal runtime image),
    matching the existing API container healthcheck's own approach.
    """
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            service,
            "python",
            "-c",
            "import urllib.request as u; "
            "print(u.urlopen('http://127.0.0.1:9464/metrics', timeout=5)"
            ".read().decode())",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, (
        f"docker compose exec into {service!r} to scrape its internal metrics "
        f"port failed (exit {result.returncode})"
    )
    return result.stdout


def _api_metrics() -> str:
    with urllib.request.urlopen(f"{API_BASE_URL}/metrics", timeout=5) as response:
        body: str = response.read().decode()
        return body


def test_each_role_exposes_a_reachable_prometheus_endpoint() -> None:
    api_body = _api_metrics()
    worker_body = _metrics_in_container("worker")
    outbox_body = _metrics_in_container("outbox-relay")
    consumer_body = _metrics_in_container("consumer")

    for body in (api_body, worker_body, outbox_body, consumer_body):
        assert "# HELP atlas_" in body
        assert "# TYPE atlas_" in body


def test_role_owned_labeled_metrics_never_appear_with_a_sample_in_another_role() -> (
    None
):
    """Proves independent process-local registries, not one shared registry.

    Each of these four metrics is labeled, so -- per this module's own
    docstring -- it carries zero sample lines anywhere it has never been
    observed, even though its inert ``# HELP``/``# TYPE`` lines appear in
    every role's exposition text (all four roles construct the same
    complete ``AtlasMetrics`` catalog).
    """
    api_body = _api_metrics()
    worker_body = _metrics_in_container("worker")
    outbox_body = _metrics_in_container("outbox-relay")
    consumer_body = _metrics_in_container("consumer")

    # A prior run of this suite/stack may have already produced samples for
    # any of these on their owning role -- only cross-role *absence* matters
    # here, never same-role presence (that is covered by the end-to-end test
    # below, which drives a fresh, uniquely identifiable observation).
    assert "atlas_worker_claims_total{" not in api_body
    assert "atlas_outbox_relay_runs_total{" not in api_body
    assert "atlas_consumer_messages_total{" not in api_body

    assert "atlas_http_requests_total{" not in worker_body
    assert "atlas_outbox_relay_runs_total{" not in worker_body
    assert "atlas_consumer_messages_total{" not in worker_body

    assert "atlas_http_requests_total{" not in outbox_body
    assert "atlas_worker_claims_total{" not in outbox_body
    assert "atlas_consumer_messages_total{" not in outbox_body

    assert "atlas_http_requests_total{" not in consumer_body
    assert "atlas_worker_claims_total{" not in consumer_body
    assert "atlas_outbox_relay_runs_total{" not in consumer_body


def _submit_research_job(question: str) -> str:
    body = json.dumps({"question": question}).encode()
    request = urllib.request.Request(
        f"{API_BASE_URL}/v1/research-jobs",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": uuid.uuid4().hex,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        pytest.fail(f"POST /v1/research-jobs failed: HTTP {exc.code}")
    job_id: str = payload["id"]
    return job_id


def _poll_job_terminal(job_id: str, *, timeout_seconds: float = 60.0) -> str:
    deadline = time.monotonic() + timeout_seconds
    last_status = "unknown"
    while time.monotonic() < deadline:
        with urllib.request.urlopen(
            f"{API_BASE_URL}/v1/research-jobs/{job_id}", timeout=5
        ) as response:
            payload = json.loads(response.read().decode())
        last_status = str(payload["status"])
        if last_status in ("COMPLETED", "FAILED"):
            return last_status
        time.sleep(1.0)
    pytest.fail(f"Job {job_id} never reached a terminal status (last: {last_status})")


def test_submitting_a_research_job_advances_metrics_on_every_role() -> None:
    """One real submission exercises all four roles, each observed independently.

    API: the submission itself. Worker: claim + processing outcome. Outbox
    relay: publication of the resulting domain event. Kafka consumer:
    projection apply. This does not assert exact counts (other activity may
    be occurring against the same shared stack concurrently) -- only that
    each role's own registry advanced past its value from immediately
    before this test's submission.
    """

    def _sample_total(body: str, metric_name: str) -> float:
        total = 0.0
        for line in body.splitlines():
            if line.startswith(f"{metric_name}{{") or line.startswith(
                f"{metric_name} "
            ):
                total += float(line.rsplit(" ", 1)[1])
        return total

    before_worker = _sample_total(
        _metrics_in_container("worker"), "atlas_worker_claims_total"
    )
    before_outbox = _sample_total(
        _metrics_in_container("outbox-relay"), "atlas_outbox_relay_runs_total"
    )
    before_consumer = _sample_total(
        _metrics_in_container("consumer"), "atlas_consumer_messages_total"
    )
    before_submissions = _sample_total(
        _api_metrics(), "atlas_research_job_submissions_total"
    )

    job_id = _submit_research_job(
        "Slice 15A2 cross-process metrics verification question"
    )
    _poll_job_terminal(job_id)
    # Give the outbox relay and consumer's own poll intervals a moment to
    # observe and commit past the job's terminal write.
    time.sleep(2.0)

    after_worker = _sample_total(
        _metrics_in_container("worker"), "atlas_worker_claims_total"
    )
    after_outbox = _sample_total(
        _metrics_in_container("outbox-relay"), "atlas_outbox_relay_runs_total"
    )
    after_consumer = _sample_total(
        _metrics_in_container("consumer"), "atlas_consumer_messages_total"
    )
    after_submissions = _sample_total(
        _api_metrics(), "atlas_research_job_submissions_total"
    )

    assert after_submissions > before_submissions
    assert after_worker > before_worker
    assert after_outbox > before_outbox
    assert after_consumer > before_consumer
