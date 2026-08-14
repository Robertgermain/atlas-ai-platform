"""Subprocess-level advisory CLI stream contract (no PostgreSQL)."""

from __future__ import annotations

from pathlib import Path

from tests.advisor.cli_contract import (
    FIXTURE_CANARY,
    SECRET_CANARY,
    assert_canaries_absent,
    assert_failure_streams,
    run_advisor_cli,
)


def test_subprocess_missing_required_argument_is_structured(
    tmp_path: Path,
) -> None:
    result = run_advisor_cli([], cwd=tmp_path)
    assert result.returncode == 1
    assert_failure_streams(result.stdout, result.stderr)
    assert_canaries_absent(result.stdout, result.stderr)


def test_subprocess_unknown_fixture_flag_is_structured(tmp_path: Path) -> None:
    result = run_advisor_cli(
        ["--fixture", FIXTURE_CANARY, SECRET_CANARY],
        cwd=tmp_path,
    )
    assert result.returncode == 1
    assert_failure_streams(result.stdout, result.stderr)
    assert_canaries_absent(result.stdout, result.stderr)


def test_subprocess_path_like_job_id_is_structured(tmp_path: Path) -> None:
    result = run_advisor_cli(
        ["--research-job-id", f"../tmp/{FIXTURE_CANARY}"],
        cwd=tmp_path,
    )
    assert result.returncode == 1
    assert_failure_streams(result.stdout, result.stderr)
    assert_canaries_absent(result.stdout, result.stderr)
