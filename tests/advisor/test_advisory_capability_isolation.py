"""Object-graph and import isolation for the advisory package."""

from __future__ import annotations

import ast
from pathlib import Path

from atlas.advisor.service import AdvisoryService

_BANNED_MODULES = frozenset(
    {
        "atlas.consumer.replay",
        "atlas.api.v1.review",
        "atlas.tools.runner",
        "atlas.application.worker",
        "atlas.outbox.relay",
        "subprocess",
    }
)


def test_advisor_package_does_not_import_mutation_modules() -> None:
    root = Path("src/atlas/advisor")
    imported: set[str] = set()
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported.add(alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
    assert imported & _BANNED_MODULES == set()


def test_service_has_no_mutation_methods() -> None:
    names = set(dir(AdvisoryService))
    for banned in (
        "replay",
        "finalize_failure",
        "save",
        "claim_next",
        "enqueue",
        "acknowledge",
        "invoke",
    ):
        assert banned not in names
    assert "analyze_job" in names


def test_api_package_has_no_advisory_route() -> None:
    root = Path("src/atlas/api")
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "atlas.advisor" not in source
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert "/advisor" not in node.value
                assert "--research-job-id" not in node.value


def test_compose_has_no_advisor_service() -> None:
    source = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "python -m atlas.advisor" not in source
    assert "atlas-advisor:" not in source


def test_no_advisory_analyses_migration() -> None:
    versions = Path("alembic/versions")
    assert not list(versions.glob("*advisory*"))
    for path in versions.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "advisory_analyses" not in text
        assert 'create_table("advisory' not in text
        assert "create_table('advisory" not in text
