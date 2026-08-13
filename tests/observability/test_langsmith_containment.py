"""LangSmith failures never replace success or mask the original exception."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import CollectorRegistry
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.observability.events import Event
from atlas.observability.langsmith import (
    LangSmithHandle,
    configure_langsmith,
    reset_langsmith_for_tests,
    trace_ai,
    trace_research_job,
)
from atlas.observability.langsmith.client import FLUSH_BOUND_SECONDS
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.observability.testing import capture_logs
from tests.observability.langsmith_fakes import isolate_langsmith_settings_environment

_DUMMY_LANGSMITH_KEY = "lsv2_test_not_a_real_key"


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    reset_langsmith_for_tests()
    yield
    reset_langsmith_for_tests()


def _metrics() -> AtlasMetrics:
    return AtlasMetrics(CollectorRegistry())


def _sample(metrics: AtlasMetrics, operation: str, outcome: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != "atlas_langsmith_operations_total":
                continue
            if (
                sample.labels.get("operation") == operation
                and sample.labels.get("outcome") == outcome
            ):
                total += sample.value
    return total


def test_missing_key_disables_export_without_network(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    metrics = _metrics()
    handle = configure_langsmith(Settings(), metrics=metrics)
    assert handle.enabled is False
    assert _sample(metrics, "initialize", "disabled") == 1.0
    assert trace_ai(name="x", run_type="chain", fn=lambda: 7) == 7
    assert _sample(metrics, "enqueue", "disabled") == 1.0


def test_client_constructor_failure_disables_export_and_logs_class_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    metrics = _metrics()

    def _boom(**_kwargs: object) -> object:
        raise RuntimeError("https://api.smith.langchain.com/secret-key")

    monkeypatch.setattr("atlas.observability.langsmith.client.Client", _boom)
    with capture_logs("atlas.observability.langsmith.client") as captured:
        handle = configure_langsmith(
            Settings(langsmith_api_key=SecretStr(_DUMMY_LANGSMITH_KEY)),
            metrics=metrics,
        )
    assert handle.enabled is False
    assert _sample(metrics, "initialize", "error") == 1.0
    assert captured.events == [Event.LANGSMITH_INIT_FAILED.value]
    assert captured.json(0)["error_class"] == "RuntimeError"
    assert "smith.langchain" not in captured.text
    assert "secret-key" not in captured.text


def test_tracing_context_enter_failure_still_runs_fn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=_metrics())

    def _boom(**_kwargs: object) -> object:
        raise RuntimeError("context-enter-failed")

    monkeypatch.setattr("atlas.observability.langsmith.tracing.tracing_context", _boom)
    seen = {"ran": False}

    def _fn() -> str:
        seen["ran"] = True
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert trace_research_job(job_id="j", workflow_execution_id="e", fn=_fn) == "ok"
    assert seen["ran"] is True
    assert Event.LANGSMITH_EXPORT_FAILED.value in captured.events
    handle.close()


def test_trace_enter_failure_still_runs_fn_and_preserves_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=_metrics())

    class _Boom:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def __enter__(self) -> None:
            raise RuntimeError("run-create-failed")

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.langsmith_trace",
        lambda *_args, **_kwargs: _Boom(),
    )
    assert (
        trace_ai(name="model.plan", run_type="llm", fn=lambda: "planned") == "planned"
    )
    handle.close()


def test_trace_exit_failure_does_not_replace_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=_metrics())

    class _ExitBoom:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def __enter__(self) -> _ExitBoom:
            return self

        def __exit__(self, *_args: object) -> None:
            raise RuntimeError("exit-failed")

    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.langsmith_trace",
        lambda *_args, **_kwargs: _ExitBoom(),
    )
    assert (
        trace_ai(name="model.draft", run_type="llm", fn=lambda: "drafted") == "drafted"
    )
    handle.close()


def test_trace_exit_failure_does_not_mask_original_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=_metrics())

    class _ExitBoom:
        def __enter__(self) -> _ExitBoom:
            return self

        def __exit__(self, *_args: object) -> None:
            raise RuntimeError("exit-failed")

    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.langsmith_trace",
        lambda *_args, **_kwargs: _ExitBoom(),
    )

    def _fn() -> None:
        raise ValueError("business-failure")

    with pytest.raises(ValueError, match="business-failure"):
        trace_ai(name="retrieval", run_type="retriever", fn=_fn)
    handle.close()


def test_export_error_callback_is_contained(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    metrics = _metrics()
    recorded: dict[str, Any] = {}

    class _Client:
        def __init__(self, **kwargs: object) -> None:
            recorded["callback"] = kwargs.get("tracing_error_callback")

        def flush(self, timeout: float | None = None) -> None:
            del timeout

    monkeypatch.setattr("atlas.observability.langsmith.client.Client", _Client)
    handle = configure_langsmith(
        Settings(langsmith_api_key=SecretStr(_DUMMY_LANGSMITH_KEY)),
        metrics=metrics,
    )
    callback = recorded["callback"]
    assert callable(callback)
    with capture_logs("atlas.observability.langsmith.client") as captured:
        callback(TimeoutError())
    assert captured.events == [Event.LANGSMITH_EXPORT_FAILED.value]
    assert captured.json(0)["error_class"] == "TimeoutError"
    assert captured.json(0)["outcome"] == "export"
    assert _sample(metrics, "export", "timeout") == 1.0
    handle.close()


def test_flush_timeout_is_contained_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = _metrics()
    monkeypatch.setattr("atlas.observability.langsmith.client.FLUSH_BOUND_SECONDS", 0.2)

    class _Slow:
        def flush(self, timeout: float | None = None) -> None:
            del timeout
            time.sleep(2)

    handle = LangSmithHandle(
        _Slow(),  # type: ignore[arg-type]
        project="atlas-local",
        metrics=metrics,
        bound=True,
    )
    started = time.perf_counter()
    with capture_logs("atlas.observability.langsmith.client") as captured:
        handle.close()
    elapsed = time.perf_counter() - started
    assert elapsed < 1.5
    assert Event.LANGSMITH_SHUTDOWN_FAILED.value in captured.events
    assert _sample(metrics, "flush", "timeout") == 1.0
    assert FLUSH_BOUND_SECONDS == 5.0


def test_attach_run_metadata_broken_mapping_does_not_affect_business(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from collections.abc import Mapping

    from atlas.observability.langsmith import attach_run_metadata
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=_metrics())

    class _BrokenMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            raise RuntimeError("malicious-getitem")

        def __iter__(self) -> Iterator[str]:
            raise RuntimeError("malicious-iter")

        def __len__(self) -> int:
            raise RuntimeError("malicious-len")

    def _fn() -> str:
        attach_run_metadata(_BrokenMapping())
        return "ok"

    assert trace_research_job(job_id="j", workflow_execution_id="e", fn=_fn) == "ok"
    handle.close()
