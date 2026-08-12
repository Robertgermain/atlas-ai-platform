"""Static structural assertions over ``docker-compose.yml`` (Milestone 14 Slice 14B).

These tests render the real Compose configuration with ``docker compose
config`` (no daemon/network access -- the Compose CLI only parses and merges
the YAML file, resolving anchors/merge keys and ``${...}`` interpolation) and
assert the single-image-build/shared-image-tag/no-``ENTRYPOINT``-override/
Redis-dependency invariants described in docs/TECHNICAL_DESIGN.md ("Compose
application topology, Milestone 14 Slice 14B").

The whole module is skipped only when the Docker Compose CLI/plugin is
genuinely unavailable (checked once via ``docker compose version``, not
merely by checking whether a `docker` binary exists on PATH). Once Compose is
available, a nonzero exit code or invalid JSON from ``docker compose config``
is a test *failure*, not a skip -- those outcomes mean the tool is present
but the configuration itself is broken, which is exactly what these tests
exist to catch. Failure messages are kept sanitized: they never include the
rendered configuration, environment values, credentials, or the complete raw
subprocess output.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# The six Atlas application/one-shot services that must share one image
# (postgres/redis/kafka are third-party infrastructure and are intentionally
# excluded from every assertion below).
ATLAS_SERVICES = (
    "db-migrate",
    "kafka-topic-init",
    "api",
    "worker",
    "outbox-relay",
    "consumer",
)


def _docker_compose_available() -> bool:
    """Probe Compose CLI/plugin availability without touching this repo's config."""
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


_DOCKER_COMPOSE_AVAILABLE = _docker_compose_available()

pytestmark = pytest.mark.skipif(
    not _DOCKER_COMPOSE_AVAILABLE,
    reason="docker compose CLI/plugin is not available in this environment",
)


def _load_rendered_config() -> dict[str, Any]:
    """Render docker-compose.yml via the real Compose CLI. Fails, never skips.

    Compose availability was already confirmed by the module-level skip
    check above, so a nonzero exit code or unparseable output here means
    the configuration itself is broken, not that the tool is missing.
    """
    result = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            "docker compose config exited with a nonzero status "
            f"(exit code {result.returncode}) even though the Compose "
            "CLI/plugin is available; docker-compose.yml likely has a "
            "syntax or reference error. Rerun `docker compose config` "
            "locally for full, unsanitized output."
        )

    try:
        config: dict[str, Any] = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        pytest.fail(
            "docker compose config did not produce valid JSON "
            f"({exc.__class__.__name__} at line {exc.lineno}, column "
            f"{exc.colno}). Rerun `docker compose config --format json` "
            "locally for full, unsanitized output."
        )

    return config


def test_exactly_one_atlas_service_declares_build() -> None:
    config = _load_rendered_config()
    services = config["services"]

    services_with_build = [
        name for name in ATLAS_SERVICES if services[name].get("build") is not None
    ]

    assert services_with_build == ["api"], (
        "Expected exactly one Atlas service ('api') to declare a Compose "
        f"'build:' section; found: {services_with_build!r}. Every other "
        "Atlas service must only reference the already-built image."
    )


def test_all_atlas_services_resolve_to_the_same_image_tag() -> None:
    config = _load_rendered_config()
    services = config["services"]

    image_tags = {name: services[name]["image"] for name in ATLAS_SERVICES}
    resolved = set(image_tags.values())

    assert len(resolved) == 1, (
        "Expected all six Atlas services to resolve to exactly one image "
        f"tag; found: {image_tags!r}."
    )
    (only_tag,) = resolved
    assert only_tag.startswith("atlas-backend:"), only_tag


def test_no_atlas_service_overrides_entrypoint() -> None:
    config = _load_rendered_config()
    services = config["services"]

    services_with_entrypoint = [
        name for name in ATLAS_SERVICES if services[name].get("entrypoint")
    ]

    assert services_with_entrypoint == [], (
        "No Atlas service may override the image's Tini ENTRYPOINT via "
        f"Compose 'entrypoint:'; found overrides on: "
        f"{services_with_entrypoint!r}."
    )


def test_api_and_worker_depend_on_healthy_redis() -> None:
    config = _load_rendered_config()
    services = config["services"]

    for name in ("api", "worker"):
        depends_on = services[name].get("depends_on", {})
        assert "redis" in depends_on, (
            f"Expected '{name}' to declare a Compose dependency on 'redis' "
            "(startup sequencing only, matching its explicit "
            "ATLAS_COORDINATION_PROVIDER=redis setting)."
        )
        assert depends_on["redis"]["condition"] == "service_healthy", (
            f"Expected '{name}' to wait for redis: service_healthy, found "
            f"{depends_on['redis']!r}."
        )
