"""LangSmith failures never replace success or mask the original exception."""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping, Sequence
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
    run_in_tracing_context,
    trace_ai,
    trace_research_job,
)
from atlas.observability.langsmith.client import FLUSH_BOUND_SECONDS
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.observability.testing import CapturedLogs, capture_logs
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


_PLANTED_SECRET = "sk-planted-secret"


class _TrackingContext:
    """Fake ``tracing_context`` manager with an explicit active flag."""

    def __init__(
        self,
        *,
        enter_error: Exception | None = None,
        exit_error: Exception | None = None,
    ) -> None:
        self.active = False
        self.enter_count = 0
        self.exit_count = 0
        self._enter_error = enter_error
        self._exit_error = exit_error

    def __enter__(self) -> _TrackingContext:
        self.active = True
        self.enter_count += 1
        if self._enter_error is not None:
            raise self._enter_error
        return self

    def __exit__(self, *_args: object) -> None:
        self.active = False
        self.exit_count += 1
        if self._exit_error is not None:
            raise self._exit_error


def test_run_in_tracing_context_success_enters_and_exits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    tracker = _TrackingContext()
    recorded: dict[str, object] = {}

    def _factory(**kwargs: object) -> _TrackingContext:
        recorded.update(kwargs)
        return tracker

    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.tracing_context", _factory
    )
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        assert tracker.active is True
        return "ok"

    assert (
        run_in_tracing_context(
            client=handle.client,
            project=handle.project,
            metadata={
                "atlas.node_name": "advise",
                "prompt": "should-not-export",
            },
            fn=_fn,
        )
        == "ok"
    )
    assert calls["n"] == 1
    assert tracker.enter_count == 1
    assert tracker.exit_count == 1
    assert tracker.active is False
    assert recorded["enabled"] is True
    assert recorded["client"] is handle.client
    assert recorded["project_name"] == handle.project
    assert "tags" not in recorded
    metadata = recorded["metadata"]
    assert isinstance(metadata, dict)
    assert metadata.get("atlas.node_name") == "advise"
    assert "prompt" not in metadata
    assert _sample(metrics, "enqueue", "success") == 1.0
    handle.close()


def test_run_in_tracing_context_enter_failure_still_runs_fn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)

    def _boom(**_kwargs: object) -> object:
        raise RuntimeError(_PLANTED_SECRET)

    monkeypatch.setattr("atlas.observability.langsmith.tracing.tracing_context", _boom)
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project="atlas-secret-project",
                metadata={"atlas.node_name": "advise"},
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    assert captured.events == [Event.LANGSMITH_EXPORT_FAILED.value]
    payload = captured.json(0)
    assert payload["error_class"] == "RuntimeError"
    assert payload["outcome"] == "enqueue"
    assert _PLANTED_SECRET not in captured.text
    assert "atlas-secret-project" not in captured.text
    assert _sample(metrics, "enqueue", "error") == 1.0
    handle.close()


def test_run_in_tracing_context_partial_enter_does_not_remain_active(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    tracker = _TrackingContext(enter_error=RuntimeError(_PLANTED_SECRET))
    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.tracing_context",
        lambda **_kwargs: tracker,
    )
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project=handle.project,
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    assert tracker.enter_count == 1
    assert tracker.exit_count == 1
    assert tracker.active is False
    assert captured.json(0)["error_class"] == "RuntimeError"
    assert _PLANTED_SECRET not in captured.text
    assert _sample(metrics, "enqueue", "error") == 1.0
    handle.close()


def test_run_in_tracing_context_exit_failure_preserves_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    tracker = _TrackingContext(exit_error=RuntimeError(_PLANTED_SECRET))
    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.tracing_context",
        lambda **_kwargs: tracker,
    )
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project=handle.project,
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    assert tracker.active is False
    assert captured.events == [Event.LANGSMITH_EXPORT_FAILED.value]
    assert captured.json(0)["error_class"] == "RuntimeError"
    assert captured.json(0)["outcome"] == "enqueue"
    assert _PLANTED_SECRET not in captured.text
    assert _sample(metrics, "enqueue", "success") == 1.0
    assert _sample(metrics, "enqueue", "error") == 1.0
    handle.close()


def test_run_in_tracing_context_exit_failure_preserves_original_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    tracker = _TrackingContext(exit_error=RuntimeError(_PLANTED_SECRET))
    monkeypatch.setattr(
        "atlas.observability.langsmith.tracing.tracing_context",
        lambda **_kwargs: tracker,
    )
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        raise ValueError("business-failure")

    with (
        capture_logs("atlas.observability.langsmith.tracing") as captured,
        pytest.raises(ValueError, match="business-failure"),
    ):
        run_in_tracing_context(
            client=handle.client,
            project=handle.project,
            fn=_fn,
        )
    assert calls["n"] == 1
    assert tracker.active is False
    assert captured.events == [Event.LANGSMITH_EXPORT_FAILED.value]
    assert captured.json(0)["error_class"] == "RuntimeError"
    assert _PLANTED_SECRET not in captured.text
    assert "business-failure" not in captured.text
    assert _sample(metrics, "enqueue", "error") == 1.0
    handle.close()


_PLANTED_TAG = "planted-tag-secret"
_SETUP_LOG_KEYS = frozenset(
    {
        "timestamp",
        "severity",
        "service",
        "event",
        "trace_id",
        "span_id",
        "research_job_id",
        "workflow_execution_id",
        "node_name",
        "model_invocation_id",
        "tool_invocation_id",
        "evaluation_run_id",
        "outbox_event_id",
        "consumer_event_id",
        "error_class",
        "duration_ms",
        "outcome",
    }
)


class _BoomMapping(Mapping[str, object]):
    def __getitem__(self, key: str) -> object:
        raise RuntimeError(_PLANTED_SECRET)

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError(_PLANTED_SECRET)

    def __len__(self) -> int:
        raise RuntimeError(_PLANTED_SECRET)


class _BoomTags(Sequence[str]):
    def __getitem__(self, index: int | slice) -> Any:
        raise RuntimeError(_PLANTED_TAG)

    def __len__(self) -> int:
        return 1


def _assert_contained_setup_failure(
    captured: CapturedLogs, metrics: AtlasMetrics
) -> None:
    assert captured.events == [Event.LANGSMITH_EXPORT_FAILED.value]
    payload = captured.json(0)
    assert set(payload) <= _SETUP_LOG_KEYS
    assert payload["event"] == Event.LANGSMITH_EXPORT_FAILED.value
    assert payload["error_class"] == "RuntimeError"
    assert payload["outcome"] == "enqueue"
    assert _PLANTED_SECRET not in captured.text
    assert _PLANTED_TAG not in captured.text
    assert "atlas-secret-project" not in captured.text
    assert _sample(metrics, "enqueue", "error") == 1.0


def test_run_in_tracing_context_broken_mapping_still_runs_fn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project="atlas-secret-project",
                metadata=_BoomMapping(),
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    _assert_contained_setup_failure(captured, metrics)
    handle.close()


def test_run_in_tracing_context_broken_mapping_preserves_business_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        raise ValueError("business-failure")

    with (
        capture_logs("atlas.observability.langsmith.tracing") as captured,
        pytest.raises(ValueError, match="business-failure"),
    ):
        run_in_tracing_context(
            client=handle.client,
            project="atlas-secret-project",
            metadata=_BoomMapping(),
            fn=_fn,
        )
    assert calls["n"] == 1
    _assert_contained_setup_failure(captured, metrics)
    assert "business-failure" not in captured.text
    handle.close()


def test_run_in_tracing_context_filter_failure_still_runs_fn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)

    def _boom(_raw: object) -> dict[str, str]:
        raise RuntimeError(_PLANTED_SECRET)

    monkeypatch.setattr("atlas.observability.langsmith.tracing.filter_metadata", _boom)
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project="atlas-secret-project",
                metadata={"atlas.node_name": "advise"},
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    _assert_contained_setup_failure(captured, metrics)
    handle.close()


def test_run_in_tracing_context_tags_conversion_failure_still_runs_fn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.observability.langsmith_fakes import arm_dummy_langsmith

    metrics = _metrics()
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path, metrics=metrics)
    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        return "ok"

    with capture_logs("atlas.observability.langsmith.tracing") as captured:
        assert (
            run_in_tracing_context(
                client=handle.client,
                project="atlas-secret-project",
                tags=_BoomTags(),
                fn=_fn,
            )
            == "ok"
        )
    assert calls["n"] == 1
    _assert_contained_setup_failure(captured, metrics)
    handle.close()


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
