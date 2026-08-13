"""``contextvars``-based correlation context (Slice 15A1).

Covers nested restoration, concurrent ``asyncio`` task isolation, native
thread isolation, cleanup after the bound block exits, unsupported-field
rejection without leaking the field's value, opaque-value bounding, and
external immutability of the object ``current_context()`` returns.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest

from atlas.observability.context import (
    MAX_CONTEXT_VALUE_LENGTH,
    bind_context,
    current_context,
)

# Sensitive-looking fixture values -- never real secrets.
_FAKE_SECRET = "sk-fake-abc123hunter2"


def test_current_context_defaults_to_empty() -> None:
    assert current_context() == {}


# --- external immutability of the returned context -----------------------


def test_current_context_returned_object_cannot_be_mutated() -> None:
    """``current_context()`` returns a ``types.MappingProxyType`` view.

    Both overwriting an existing key and inserting a new one must raise
    ``TypeError`` -- there is no supported way to change what a later
    ``current_context()`` call observes other than ``bind_context()``.
    """
    with bind_context(research_job_id="job-1"):
        snapshot = current_context()
        with pytest.raises(TypeError):
            snapshot["research_job_id"] = "tampered"  # type: ignore[index]
        with pytest.raises(TypeError):
            snapshot["node_name"] = "tampered"  # type: ignore[index]
        # The stored context is unaffected by the failed mutation attempts.
        assert current_context() == {"research_job_id": "job-1"}


def test_ignored_mutation_attempt_never_leaks_into_a_later_call() -> None:
    """Even if a caller ignores the ``TypeError`` entirely, one call's

    returned object can never become what a *later* call returns --
    each call wraps whatever is currently stored, and this module never
    mutates a stored context in place after installing it.
    """
    with bind_context(research_job_id="job-1"):
        first = current_context()
        with contextlib.suppress(TypeError):
            first["research_job_id"] = "tampered"  # type: ignore[index]
        assert current_context()["research_job_id"] == "job-1"


def test_bind_context_sets_and_clears() -> None:
    with bind_context(research_job_id="job-1"):
        assert current_context() == {"research_job_id": "job-1"}
    assert current_context() == {}


def test_nested_context_merges_and_restores_exactly() -> None:
    with bind_context(research_job_id="job-1"):
        assert current_context() == {"research_job_id": "job-1"}
        with bind_context(node_name="planner"):
            assert current_context() == {
                "research_job_id": "job-1",
                "node_name": "planner",
            }
            with bind_context(research_job_id="job-2"):
                assert current_context() == {
                    "research_job_id": "job-2",
                    "node_name": "planner",
                }
            # Restores the middle level exactly, not the innermost.
            assert current_context() == {
                "research_job_id": "job-1",
                "node_name": "planner",
            }
        assert current_context() == {"research_job_id": "job-1"}
    assert current_context() == {}


def test_nested_context_explicit_none_unsets_for_the_nested_block_only() -> None:
    with bind_context(research_job_id="job-1"):
        with bind_context(research_job_id=None):
            assert current_context() == {}
        assert current_context() == {"research_job_id": "job-1"}


def test_context_clears_after_exception_inside_the_block() -> None:
    with pytest.raises(RuntimeError):
        with bind_context(research_job_id="job-1"):
            raise RuntimeError("boom")
    assert current_context() == {}


def test_unsupported_field_is_rejected_without_leaking_its_value() -> None:
    with pytest.raises(ValueError) as exc_info:
        with bind_context(not_an_approved_field=_FAKE_SECRET):
            pass
    assert _FAKE_SECRET not in str(exc_info.value)
    assert "not_an_approved_field" in str(exc_info.value)
    # The context must be left untouched by the rejected call.
    assert current_context() == {}


def test_long_value_is_truncated_not_rejected() -> None:
    oversized = "x" * (MAX_CONTEXT_VALUE_LENGTH * 2)
    with bind_context(research_job_id=oversized):
        bound = current_context()["research_job_id"]
        assert len(bound) <= MAX_CONTEXT_VALUE_LENGTH
        assert bound.endswith("...<truncated>")


def test_threads_without_copied_context_start_clean() -> None:
    """A plain ``threading.Thread`` never inherits the caller's context."""
    observed: dict[str, object] = {}

    def _in_thread() -> None:
        observed["context"] = current_context()

    with bind_context(research_job_id="job-main-thread"):
        thread = threading.Thread(target=_in_thread)
        thread.start()
        thread.join()

    assert observed["context"] == {}


def test_concurrent_async_tasks_do_not_leak_context() -> None:
    """Two tasks bind different ids concurrently; each only ever sees its own.

    Wrapped in ``asyncio.run`` rather than an ``async def`` test (matching
    this codebase's existing convention -- see
    ``tests/integration/test_mcp_client_contract.py``) so this needs no new
    third-party test dependency (e.g. ``pytest-asyncio``), consistent with
    this slice's "no new third-party dependencies" constraint.
    """
    results: dict[str, str | None] = {}
    started = asyncio.Event()
    release_a = asyncio.Event()

    async def _task(name: str, job_id: str, *, first: bool) -> None:
        with bind_context(research_job_id=job_id):
            if first:
                started.set()
                await release_a.wait()
            else:
                await started.wait()
                release_a.set()
            # Yield control back to the event loop at least once more so
            # both tasks are genuinely interleaved before either reads its
            # own context back out.
            await asyncio.sleep(0)
            results[name] = current_context().get("research_job_id")

    async def _run() -> None:
        await asyncio.gather(
            _task("a", "job-a", first=True),
            _task("b", "job-b", first=False),
        )

    asyncio.run(_run())

    assert results == {"a": "job-a", "b": "job-b"}


def test_allowed_context_fields_match_the_approved_correlation_id_set() -> None:
    from atlas.observability.context import ALLOWED_CONTEXT_FIELDS
    from atlas.observability.logging import _ALLOWED_STRUCTURED_FIELDS

    non_context_fields = {"error_class", "duration_ms", "outcome"}
    assert ALLOWED_CONTEXT_FIELDS == _ALLOWED_STRUCTURED_FIELDS - non_context_fields
