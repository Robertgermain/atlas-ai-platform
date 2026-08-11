"""Offline unit tests for migration-history base-ref resolution (no network)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.integration.db_support import REPO_ROOT
from tests.migration_history import (
    DEFAULT_MIGRATION_BASE_REF,
    assert_prior_migrations_unchanged_versus_ref,
    list_prior_migration_paths,
    resolve_git_ref,
)


@dataclass
class _FakeResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


def test_default_base_ref_is_origin_main() -> None:
    assert DEFAULT_MIGRATION_BASE_REF == "origin/main"


def test_list_prior_migration_paths_finds_exactly_eleven() -> None:
    prior = list_prior_migration_paths(REPO_ROOT)
    assert len(prior) == 11
    for index, relative in enumerate(prior, start=1):
        assert f"{index:04d}_" in Path(relative).name


def test_resolve_git_ref_returns_sha_when_rev_parse_succeeds(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def runner(args: list[str], **_kwargs: object) -> _FakeResult:
        calls.append(list(args))
        return _FakeResult(returncode=0, stdout="abc123def456\n")

    sha = resolve_git_ref(tmp_path, "origin/main", runner=runner)
    assert sha == "abc123def456"
    assert calls == [["git", "rev-parse", "--verify", "origin/main^{commit}"]]


def test_resolve_git_ref_fails_clearly_when_unavailable(tmp_path: Path) -> None:
    def runner(args: list[str], **_kwargs: object) -> _FakeResult:
        return _FakeResult(
            returncode=128,
            stderr="fatal: Needed a single revision",
        )

    with pytest.raises(
        AssertionError, match="Base reference 'origin/main' is unavailable"
    ):
        resolve_git_ref(tmp_path, "origin/main", runner=runner)


def test_resolve_git_ref_fails_when_stdout_empty_even_if_returncode_zero(
    tmp_path: Path,
) -> None:
    def runner(args: list[str], **_kwargs: object) -> _FakeResult:
        return _FakeResult(returncode=0, stdout="  \n")

    with pytest.raises(
        AssertionError, match="Base reference 'missing-ref' is unavailable"
    ):
        resolve_git_ref(tmp_path, "missing-ref", runner=runner)


def test_assert_prior_migrations_unchanged_uses_resolved_sha_not_local_main(
    tmp_path: Path,
) -> None:
    """Comparison must target the resolved remote-tracking SHA, never bare ``main``."""
    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    for index in range(1, 12):
        (versions / f"20260808_{index:04d}_placeholder.py").write_text(
            "# stub\n", encoding="utf-8"
        )

    calls: list[list[str]] = []

    def runner(args: list[str], **_kwargs: object) -> _FakeResult:
        calls.append(list(args))
        if args[1] == "rev-parse":
            return _FakeResult(returncode=0, stdout="deadbeef\n")
        return _FakeResult(returncode=0, stdout="")

    assert_prior_migrations_unchanged_versus_ref(
        tmp_path,
        base_ref="origin/main",
        runner=runner,
    )

    assert calls[0] == ["git", "rev-parse", "--verify", "origin/main^{commit}"]
    assert calls[1][0:4] == ["git", "diff", "--exit-code", "deadbeef"]
    assert "main" not in calls[1][3]  # resolved SHA only; not local branch name
    assert all(arg != "main" for call in calls for arg in call)


def test_assert_prior_migrations_propagates_unavailable_base_ref(
    tmp_path: Path,
) -> None:
    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    for index in range(1, 12):
        (versions / f"20260808_{index:04d}_placeholder.py").write_text(
            "# stub\n", encoding="utf-8"
        )

    def runner(args: list[str], **_kwargs: object) -> _FakeResult:
        return _FakeResult(returncode=128, stderr="fatal: bad revision 'origin/main'")

    with pytest.raises(
        AssertionError, match="Base reference 'origin/main' is unavailable"
    ):
        assert_prior_migrations_unchanged_versus_ref(
            tmp_path,
            base_ref="origin/main",
            runner=runner,
        )
