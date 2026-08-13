"""Closed metadata allowlist and hide callbacks for LangSmith export.

Bodies are never exported: the Atlas ``Client`` is constructed with
``hide_inputs=True`` and ``hide_outputs=True``. This module is the sole
place that decides which metadata keys may leave the process.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

#: Closed set of metadata keys Atlas will export to LangSmith. Anything
#: else present on a native LangGraph/LangChain run (including graph state
#: keys, URLs, and SDK bookkeeping) is dropped at the Client hide callback.
ALLOWED_METADATA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "atlas.research_job_id",
        "atlas.workflow_execution_id",
        "atlas.node_name",
        "atlas.model_invocation_id",
        "atlas.tool_invocation_id",
        "atlas.evaluation_run_id",
        "atlas.model.provider",
        "atlas.model_name",
        "atlas.prompt_version",
        "atlas.tool_id",
        "atlas.tool.provider",
        "atlas.tool_policy_version",
        "atlas.evaluation_profile",
        "atlas.evaluation_dimension",
        "atlas.evaluation_passed",
        "atlas.evaluation_score",
        "atlas.disposition_hint",
        "atlas.grader_version",
        "atlas.embedding_profile",
        "atlas.retrieval_k",
        "atlas.retrieval_hit_count",
        "atlas.continuation_mode",
        "atlas.repair_count",
        "atlas.evaluation_attempt",
        "atlas.otel_trace_id",
        "atlas.otel_span_id",
        "atlas.live_test_id",
        "error.class",
    }
)

_MAX_METADATA_VALUE_LENGTH: Final[int] = 256
_TRUNCATION_SUFFIX: Final[str] = "...<truncated>"


def hide_metadata(raw: dict[str, Any]) -> dict[str, str]:
    """Return only allowlisted metadata, stringified and length-bounded.

    Never raises: a failure returns an empty dict rather than forwarding
    the original mapping (which may contain graph state or secrets).
    """
    try:
        return filter_metadata(raw)
    except Exception:
        return {}


def filter_metadata(raw: Mapping[str, Any] | None) -> dict[str, str]:
    """Filter ``raw`` to :data:`ALLOWED_METADATA_KEYS` (may raise)."""
    if not raw:
        return {}
    filtered: dict[str, str] = {}
    for key, value in raw.items():
        if key not in ALLOWED_METADATA_KEYS:
            continue
        if value is None:
            continue
        rendered = _stringify(value)
        if rendered is None:
            continue
        filtered[key] = rendered
    return filtered


def _stringify(value: Any) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            return None
        return f"{value:.6g}"
    if isinstance(value, str):
        if len(value) <= _MAX_METADATA_VALUE_LENGTH:
            return value
        keep = _MAX_METADATA_VALUE_LENGTH - len(_TRUNCATION_SUFFIX)
        return value[:keep] + _TRUNCATION_SUFFIX
    return None
