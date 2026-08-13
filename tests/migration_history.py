"""Helpers for asserting historical Alembic migrations are unchanged vs a base ref.

Test-only module. Not imported by production application code.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

# Remote-tracking base used by CI (PR checkouts often lack a local ``main`` branch).
DEFAULT_MIGRATION_BASE_REF = "origin/main"

_PRIOR_MIGRATION_MARKERS: tuple[str, ...] = (
    "0001_",
    "0002_",
    "0003_",
    "0004_",
    "0005_",
    "0006_",
    "0007_",
    "0008_",
    "0009_",
    "0010_",
    "0011_",
    "0012_",
    "0013_",
)


class _CompletedProcess(Protocol):
    returncode: int
    stdout: str
    stderr: str


Runner = Callable[..., _CompletedProcess]


def list_prior_migration_paths(repo_root: Path) -> list[str]:
    """Return repo-relative paths for Alembic migrations ``0001``–``0013``."""
    versions = repo_root / "alembic" / "versions"
    files = sorted(versions.glob("2026080*_00*.py"))
    return [
        str(path.relative_to(repo_root))
        for path in files
        if any(marker in path.name for marker in _PRIOR_MIGRATION_MARKERS)
    ]


def resolve_git_ref(
    repo_root: Path,
    ref: str,
    *,
    runner: Runner = subprocess.run,
) -> str:
    """Resolve ``ref`` to a commit SHA.

    Raises:
        AssertionError: If the ref cannot be resolved. The invariant must not be
            silently skipped when the base reference is unavailable.
    """
    result = runner(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    sha = (result.stdout or "").strip()
    if result.returncode != 0 or not sha:
        detail = (result.stderr or result.stdout or "").strip()
        raise AssertionError(
            f"Base reference {ref!r} is unavailable for migration history "
            "comparison. Ensure the remote-tracking ref exists (GitHub Actions "
            "must check out with fetch-depth: 0 so origin/main is present). "
            f"git rev-parse detail: {detail or 'no output'}"
        )
    return sha


def assert_prior_migrations_unchanged_versus_ref(
    repo_root: Path,
    *,
    base_ref: str = DEFAULT_MIGRATION_BASE_REF,
    runner: Runner = subprocess.run,
) -> None:
    """Assert migrations ``0001``–``0013`` match ``base_ref``.

    Default ``base_ref`` is ``origin/main``.
    """
    prior = list_prior_migration_paths(repo_root)
    assert len(prior) == 13, (
        f"Expected exactly 13 prior migrations (0001–0013); found {len(prior)}: {prior}"
    )
    resolved = resolve_git_ref(repo_root, base_ref, runner=runner)
    result = runner(
        ["git", "diff", "--exit-code", resolved, "--", *prior],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Migrations 0001–0013 differ from {base_ref} (resolved {resolved}):\n"
        f"{result.stdout}{result.stderr}"
    )


def assert_prior_migrations_unchanged_versus_origin_main(
    repo_root: Path,
    *,
    runner: Runner = subprocess.run,
) -> None:
    """Convenience wrapper locked to the CI-stable remote-tracking base."""
    assert_prior_migrations_unchanged_versus_ref(
        repo_root,
        base_ref=DEFAULT_MIGRATION_BASE_REF,
        runner=runner,
    )
