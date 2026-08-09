"""Focused tests for node failure audit and safe error persistence."""

from __future__ import annotations

import pytest
from langgraph.runtime import Runtime

from atlas.workflow.graph import (
    ModelRuntimeContext,
    NodeAuditHooks,
    ResearchGraphState,
    _wrap_node,
    default_fake_runtime_context,
    initial_graph_state,
)
from atlas.workflow.processor import sanitize_node_error


class _RecordingHooks(NodeAuditHooks):
    def __init__(self) -> None:
        self.begin_calls: list[str] = []
        self.complete_calls: list[tuple[str, int]] = []
        self.fail_calls: list[tuple[str, int, Exception]] = []

    def begin(self, node_name: str) -> int:
        self.begin_calls.append(node_name)
        return 1

    def complete(self, node_name: str, attempt: int) -> None:
        self.complete_calls.append((node_name, attempt))

    def fail(self, node_name: str, attempt: int, error: Exception) -> None:
        self.fail_calls.append((node_name, attempt, error))


def _runtime_with_hooks(hooks: NodeAuditHooks) -> Runtime[ModelRuntimeContext]:
    return Runtime(context=default_fake_runtime_context(hooks=hooks))


def test_sanitize_node_error_is_class_only_and_omits_secrets() -> None:
    secret = "sk-secret-value"
    sanitized = sanitize_node_error(RuntimeError(f"token={secret}"))
    assert sanitized == "RuntimeError: node execution failed"
    assert secret not in sanitized
    assert "token=" not in sanitized


def test_node_wrapper_marks_ordinary_exception_failed() -> None:
    hooks = _RecordingHooks()
    secret = "sk-secret-value"

    def boom(
        _state: ResearchGraphState,
        _runtime: Runtime[ModelRuntimeContext],
    ) -> dict[str, object]:
        raise ValueError(f"provider said {secret}")

    wrapped = _wrap_node("plan", boom)
    with pytest.raises(ValueError, match=secret):
        wrapped(
            initial_graph_state(job_id="job-1", question="q"),
            _runtime_with_hooks(hooks),
        )

    assert hooks.begin_calls == ["plan"]
    assert hooks.complete_calls == []
    assert len(hooks.fail_calls) == 1
    node_name, attempt, error = hooks.fail_calls[0]
    assert node_name == "plan"
    assert attempt == 1
    assert isinstance(error, ValueError)
    assert secret in str(error)


def test_node_wrapper_propagates_keyboard_interrupt_without_fail_audit() -> None:
    hooks = _RecordingHooks()

    def boom(
        _state: ResearchGraphState,
        _runtime: Runtime[ModelRuntimeContext],
    ) -> dict[str, object]:
        raise KeyboardInterrupt

    wrapped = _wrap_node("plan", boom)
    with pytest.raises(KeyboardInterrupt):
        wrapped(
            initial_graph_state(job_id="job-1", question="q"),
            _runtime_with_hooks(hooks),
        )

    assert hooks.begin_calls == ["plan"]
    assert hooks.fail_calls == []
    assert hooks.complete_calls == []


def test_node_wrapper_propagates_system_exit_without_fail_audit() -> None:
    hooks = _RecordingHooks()

    def boom(
        _state: ResearchGraphState,
        _runtime: Runtime[ModelRuntimeContext],
    ) -> dict[str, object]:
        raise SystemExit(2)

    wrapped = _wrap_node("plan", boom)
    with pytest.raises(SystemExit):
        wrapped(
            initial_graph_state(job_id="job-1", question="q"),
            _runtime_with_hooks(hooks),
        )

    assert hooks.begin_calls == ["plan"]
    assert hooks.fail_calls == []
    assert hooks.complete_calls == []
