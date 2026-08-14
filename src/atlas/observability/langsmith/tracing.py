"""Fail-open wrappers around public ``langsmith.trace`` / ``tracing_context``.

Business callables always run. A LangSmith enter/exit/enqueue failure is
logged (class name only) and metriced; it never replaces a successful
result or masks the application's original exception.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Literal

from langsmith import Client, get_current_run_tree, tracing_context
from langsmith import trace as langsmith_trace

from atlas.observability.events import Event
from atlas.observability.langsmith.metadata import correlation_metadata
from atlas.observability.langsmith.redaction import filter_metadata
from atlas.observability.logging import log_exception_boundary

logger = logging.getLogger(__name__)

RunType = Literal["tool", "chain", "llm", "retriever"]


def attach_run_metadata(metadata: Mapping[str, object]) -> None:
    """Best-effort allowlisted metadata update on the current LangSmith run.

    Never raises. Used after a boundary finishes so counts/scores can be
    recorded without putting bodies on the run. Conversion, filtering, and
    RunTree mutation all stay inside this containment boundary.
    """
    try:
        filtered = filter_metadata(dict(metadata))
        if not filtered:
            return
        tree = get_current_run_tree()
        if tree is None:
            return
        existing = tree.metadata
        if isinstance(existing, dict):
            existing.update(filtered)
        else:
            tree.add_metadata(filtered)
    except Exception:
        return


def _handle() -> object:
    from atlas.observability.langsmith.client import current_langsmith

    return current_langsmith()


def _observe(operation: str, outcome: str) -> None:
    handle = _handle()
    observe = getattr(handle, "observe", None)
    if callable(observe):
        observe(operation=operation, outcome=outcome)


def _log_enqueue_failure(exc: BaseException) -> None:
    log_exception_boundary(
        logger,
        Event.LANGSMITH_EXPORT_FAILED,
        exc,
        level=logging.WARNING,
        outcome="enqueue",
    )


def run_in_tracing_context[T](
    *,
    client: Client | None,
    project: str,
    fn: Callable[[], T],
    metadata: Mapping[str, object] | None = None,
    tags: Sequence[str] | None = None,
) -> T:
    """Execute ``fn`` once under LangSmith ``tracing_context``.

    ``fn`` always runs exactly once. Mapping conversion, allowlist
    filtering, tags conversion, context construction, and context enter
    all stay inside the fail-open setup boundary. A failure there or on
    exit is logged (class name only) and metriced as
    ``operation="enqueue"`` / ``outcome="error"``; it never prevents
    ``fn``, replaces a successful result, or masks the original
    exception. A failed enter attempts best-effort ``__exit__`` so a
    partially entered fake context does not remain active. Project
    names, credentials, prompts, metadata, tags, and exception text are
    not logged.
    """
    context_cm = None
    entered = False
    try:
        filtered = filter_metadata(dict(metadata or {}))
        if tags is None:
            context_cm = tracing_context(
                enabled=True,
                client=client,
                project_name=project,
                metadata=filtered,
            )
        else:
            context_cm = tracing_context(
                enabled=True,
                client=client,
                project_name=project,
                metadata=filtered,
                tags=list(tags),
            )
        context_cm.__enter__()
        entered = True
        _observe("enqueue", "success")
    except Exception as exc:
        _log_enqueue_failure(exc)
        _observe("enqueue", "error")
        if context_cm is not None and not entered:
            try:
                context_cm.__exit__(type(exc), exc, exc.__traceback__)
            except Exception:
                pass
        context_cm = None

    raised: BaseException | None = None
    try:
        return fn()
    except BaseException as exc:
        raised = exc
        raise
    finally:
        if entered and context_cm is not None:
            try:
                if raised is None:
                    context_cm.__exit__(None, None, None)
                else:
                    context_cm.__exit__(type(raised), raised, raised.__traceback__)
            except Exception as exit_exc:
                _log_enqueue_failure(exit_exc)
                _observe("enqueue", "error")


def trace_ai[T](
    *,
    name: str,
    run_type: RunType,
    fn: Callable[[], T],
    metadata: Mapping[str, object] | None = None,
) -> T:
    """Run ``fn`` nested under a LangSmith run when the process handle is armed.

    ``fn`` always executes. ``langsmith.trace`` is entered best-effort and
    never given input/output bodies (visibility is metadata-only).
    """
    handle = _handle()
    enabled = bool(getattr(handle, "enabled", False))
    client = getattr(handle, "client", None)
    if not enabled or client is None:
        _observe("enqueue", "disabled")
        return fn()

    merged = correlation_metadata(**(metadata or {}))
    filtered = filter_metadata(merged)
    tracer = None
    try:
        tracer = langsmith_trace(
            name,
            run_type,
            client=client,
            metadata=filtered,
            inputs=None,
        )
        tracer.__enter__()
        _observe("enqueue", "success")
    except Exception as exc:
        _log_enqueue_failure(exc)
        _observe("enqueue", "error")
        tracer = None

    try:
        result = fn()
    except BaseException as raised:
        if tracer is not None:
            try:
                tracer.__exit__(type(raised), raised, raised.__traceback__)
            except Exception as exit_exc:
                _log_enqueue_failure(exit_exc)
                _observe("enqueue", "error")
        raise
    if tracer is not None:
        try:
            tracer.__exit__(None, None, None)
        except Exception as exit_exc:
            _log_enqueue_failure(exit_exc)
            _observe("enqueue", "error")
    return result


def trace_research_job[T](
    *,
    job_id: str,
    workflow_execution_id: str,
    fn: Callable[[], T],
) -> T:
    """Enter process tracing context and one ``atlas.research_job`` root run."""
    handle = _handle()
    enabled = bool(getattr(handle, "enabled", False))
    client = getattr(handle, "client", None)
    project = getattr(handle, "project", "atlas-local")
    metadata = correlation_metadata(
        **{
            "atlas.research_job_id": job_id,
            "atlas.workflow_execution_id": workflow_execution_id,
        }
    )
    if not enabled or client is None:
        _observe("enqueue", "disabled")
        return fn()

    return run_in_tracing_context(
        client=client,
        project=project,
        metadata=metadata,
        tags=("atlas", "research-job"),
        fn=lambda: trace_ai(
            name="atlas.research_job",
            run_type="chain",
            fn=fn,
            metadata=metadata,
        ),
    )
