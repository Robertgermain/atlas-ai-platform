"""CLI argv surface, stream contract, and fake-default composition."""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import SecretStr

from atlas.advisor.__main__ import _validate_job_id, main
from atlas.advisor.catalogs import FROZEN_LIVE_ADVISORY_TEMPERATURE
from atlas.advisor.composition import require_advisory_composition
from atlas.advisor.errors import AdvisoryConfigurationError, AdvisoryInputRejectedError
from atlas.config.settings import Settings
from atlas.evaluation.semantic_contracts import FROZEN_LIVE_SEMANTIC_TEMPERATURE
from atlas.observability.langsmith.errors import LangSmithConfigurationError
from atlas.observability.logging import configure_logging
from tests.advisor.cli_contract import (
    FIXTURE_CANARY,
    SECRET_CANARY,
    assert_canaries_absent,
    assert_failure_streams,
)


@pytest.fixture(autouse=True)
def _restore_logging_stream() -> Iterator[None]:
    yield
    configure_logging(service_role="worker")


def _isolate_advisory_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ATLAS_ADVISORY_MODE", raising=False)
    monkeypatch.delenv("ATLAS_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("ATLAS_MODEL_NAME", raising=False)
    monkeypatch.chdir(tmp_path)


def test_validate_job_id_rejects_path_like_values() -> None:
    with pytest.raises(AdvisoryInputRejectedError):
        _validate_job_id("../etc/passwd")
    with pytest.raises(AdvisoryInputRejectedError):
        _validate_job_id("job id with spaces")


def test_cli_rejects_unknown_fixture_flag(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--fixture", FIXTURE_CANARY, SECRET_CANARY])
    assert code == 1
    captured = capsys.readouterr()
    assert_failure_streams(captured.out, captured.err)
    assert_canaries_absent(captured.out, captured.err)


def test_cli_requires_research_job_id(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 1
    captured = capsys.readouterr()
    assert_failure_streams(captured.out, captured.err)
    assert_canaries_absent(captured.out, captured.err)


def test_cli_rejects_path_like_job_id(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--research-job-id", f"../tmp/{FIXTURE_CANARY}"]) == 1
    captured = capsys.readouterr()
    assert_failure_streams(captured.out, captured.err)
    assert_canaries_absent(captured.out, captured.err)


def test_cli_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "--research-job-id" in captured.out
    assert captured.err == ""


def test_fake_composition_requires_no_keys(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_advisory_settings(monkeypatch, tmp_path)
    settings = Settings(
        advisory_mode="fake",
        model_provider="fake",
        openai_api_key=None,
        langsmith_api_key=None,
    )
    require_advisory_composition(settings)


def test_live_without_langsmith_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_advisory_settings(monkeypatch, tmp_path)
    settings = Settings(
        advisory_mode="live",
        model_provider="openai",
        model_name="gpt-4o-mini",
        openai_api_key=SecretStr("sk-test"),
        langsmith_api_key=None,
    )
    with pytest.raises(LangSmithConfigurationError):
        require_advisory_composition(settings)


def test_live_wrong_provider_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_advisory_settings(monkeypatch, tmp_path)
    settings = Settings(
        advisory_mode="live",
        model_provider="anthropic",
        openai_api_key=SecretStr("sk-test"),
        langsmith_api_key=SecretStr("ls-test"),
    )
    with pytest.raises(AdvisoryConfigurationError):
        require_advisory_composition(settings)


def test_live_temperature_pin_is_zero() -> None:
    assert FROZEN_LIVE_ADVISORY_TEMPERATURE == 0.0
    assert FROZEN_LIVE_SEMANTIC_TEMPERATURE == 0.0


def test_main_module_has_no_fixture_argument() -> None:
    source = Path("src/atlas/advisor/__main__.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    texts: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            texts.append(node.value)
        elif isinstance(node, ast.Name) and node.id == "start_metrics_http_server":
            raise AssertionError("advisory CLI must not start a metrics HTTP server")
    assert "--fixture" not in texts
    assert "--actor-id" not in texts
    assert "--research-job-id" in texts
    assert "start_metrics_http_server" not in source
