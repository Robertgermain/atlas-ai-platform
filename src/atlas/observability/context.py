"""``contextvars``-based Atlas business correlation context (Slice 15A1).

This module binds a small, fixed set of opaque correlation identifiers
(``research_job_id``, ``workflow_execution_id``, ``node_name``,
``model_invocation_id``, ``tool_invocation_id``, ``evaluation_run_id``,
``outbox_event_id``, ``consumer_event_id``, ``trace_id``, ``span_id``) so
that any log line emitted while a context is bound automatically carries
them, without every call site having to thread the values through
explicitly. :func:`atlas.observability.logging.log_event` reads the
ambient context (see its own module docstring) to fill in any of these
fields the caller did not pass explicitly.

Concurrency model, precisely:

- ``contextvars.ContextVar`` values are copied into each new
  :class:`asyncio.Task` at creation time (CPython's own ``asyncio``
  machinery does this), so two concurrently running tasks that each call
  :func:`bind_context` independently never observe each other's bound
  values -- this is the standard library's own guarantee, not something
  this module implements itself.
- ``contextvars.ContextVar`` values are **not** propagated across
  ``threading.Thread`` boundaries: a new native thread always starts from
  the default (empty) context, even if the thread that spawned it had a
  context bound. Anything that binds context and then hands work to a
  plain thread (e.g. a ``ThreadPoolExecutor`` worker callable) must call
  :func:`bind_context` again from inside that thread's own callable if it
  wants the correlation fields to apply there too -- this module does not
  and cannot propagate them automatically across that boundary.
- Nesting merges with, and does not replace, the enclosing context: a
  nested :func:`bind_context` call inherits every field already bound by
  an outer call, and its own fields (or explicit ``None`` overrides) take
  precedence only for the lifetime of the nested block. Exiting the nested
  block restores exactly the enclosing context, via
  ``contextvars.Token``-based reset -- not a plain reassignment -- so
  restoration is correct regardless of how many levels are nested.
- :func:`current_context` returns a ``types.MappingProxyType`` view, never
  the mutable ``dict`` this module stores internally. Attempting to mutate
  the returned object (``current_context()["x"] = "y"``) raises
  :class:`TypeError`; the only supported way to change what a later
  :func:`current_context` call inside this task/thread observes is
  :func:`bind_context` itself.

No production call site in Slice 15A1 invokes :func:`bind_context` yet:
none of the entrypoint-level boundaries converted this slice
(startup/shutdown/signal-handling/poll-loop-level errors in
``atlas.main``, ``atlas.worker.__main__``, ``atlas.outbox.__main__``,
``atlas.consumer.__main__``, ``atlas.outbox.topic_admin``) has a
per-job/per-message business identifier available at the point they run --
that only exists one layer deeper (``atlas.application.worker``,
``atlas.consumer.runner``), which is out of this slice's explicit
call-site-conversion scope. This module is complete, tested
infrastructure; its first production caller arrives in a later slice
(worker job-processing, consumer message-processing, and API request
boundaries) once those files are touched for tracing.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from types import MappingProxyType

#: The complete, closed set of identifiers this module will bind. Matches
#: the correlation-shaped subset of the approved JSON field list in
#: ``atlas.observability.logging`` (i.e. every approved field except
#: ``error_class``/``duration_ms``/``outcome``, which are per-log-call
#: outcomes rather than ambient business context).
ALLOWED_CONTEXT_FIELDS: frozenset[str] = frozenset(
    {
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
    }
)

#: Opaque values are bounded, matching ``atlas.observability.logging``'s
#: own per-field truncation limit -- context and direct ``log_event``
#: fields share the same bound so neither path is a way to bypass it.
MAX_CONTEXT_VALUE_LENGTH = 256

_TRUNCATION_SUFFIX = "...<truncated>"

_context: ContextVar[Mapping[str, str]] = ContextVar("atlas_correlation_context")


def current_context() -> Mapping[str, str]:
    """Return the currently bound correlation context (empty if none).

    Returns an immutable ``types.MappingProxyType`` view over the
    internally stored ``dict``, not the mutable ``dict`` itself and not a
    defensive copy pretending to be immutable only by convention: a caller
    cannot mutate ambient context through the returned object (attempting
    to do so raises :class:`TypeError`) without going through
    :func:`bind_context`. Safe to wrap the live stored ``dict`` directly
    (rather than copying it first) because :func:`bind_context` never
    mutates an already-``_context.set()`` dict in place -- each nested
    call builds and installs a brand-new ``dict``.
    """
    return MappingProxyType(_context.get({}))


@contextmanager
def bind_context(**fields: str | None) -> Iterator[None]:
    """Bind approved correlation identifiers for the duration of this block.

    Merges with (does not replace) any enclosing context: fields not
    passed here keep whatever value the enclosing context already had.
    Passing a field explicitly as ``None`` unsets it for the nested block
    only; the enclosing context's value (if any) is restored on exit.

    Raises :class:`ValueError` -- naming only the offending field, never
    its value -- if any keyword is not in :data:`ALLOWED_CONTEXT_FIELDS`.
    This is a development-time contract violation: it is validated, and
    therefore raised, before anything is merged into the context or
    logged, so an unsupported field's value is never rendered anywhere.
    """
    unsupported = sorted(set(fields) - ALLOWED_CONTEXT_FIELDS)
    if unsupported:
        raise ValueError(f"unsupported correlation-context field(s): {unsupported!r}")

    merged = dict(current_context())
    for name, value in fields.items():
        if value is None:
            merged.pop(name, None)
        else:
            merged[name] = _bound(value)

    token: Token[Mapping[str, str]] = _context.set(merged)
    try:
        yield
    finally:
        _context.reset(token)


def _bound(value: str) -> str:
    """Truncate an opaque identifier to the shared maximum length.

    Context values are treated as opaque bounded strings, never parsed or
    format-validated here -- only their length is constrained, so an
    oversized value is truncated rather than rejected outright.
    """
    if len(value) <= MAX_CONTEXT_VALUE_LENGTH:
        return value
    keep = MAX_CONTEXT_VALUE_LENGTH - len(_TRUNCATION_SUFFIX)
    return value[:keep] + _TRUNCATION_SUFFIX
