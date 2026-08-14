"""Advisory composition is CLI-only and must not be imported by the worker."""

from __future__ import annotations

import ast
from pathlib import Path


def test_worker_package_does_not_import_advisory_composition() -> None:
    roots = (Path("src/atlas/worker"), Path("src/atlas/application"))
    for root in roots:
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert not alias.name.startswith("atlas.advisor")
                elif isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith("atlas.advisor")
                    for alias in node.names:
                        assert alias.name != "require_advisory_composition"
