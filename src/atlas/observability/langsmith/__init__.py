"""Mandatory LangSmith AI observability (Milestone 15 Slice 15B).

Worker-owned Client, metadata-only export, fail-open enqueue/flush.
Dataset/experiment orchestration is test-only and does not live here.
"""

from __future__ import annotations

from atlas.observability.langsmith.adapters import (
    TracedResearchDrafter,
    TracedResearchPlanner,
)
from atlas.observability.langsmith.client import (
    FLUSH_BOUND_SECONDS,
    LangSmithHandle,
    configure_langsmith,
    current_langsmith,
    reset_langsmith_for_tests,
)
from atlas.observability.langsmith.composition import (
    live_ai_provider_configured,
    require_langsmith_for_live_ai,
)
from atlas.observability.langsmith.errors import LangSmithConfigurationError
from atlas.observability.langsmith.metadata import correlation_metadata
from atlas.observability.langsmith.redaction import (
    ALLOWED_METADATA_KEYS,
    filter_metadata,
    hide_metadata,
)
from atlas.observability.langsmith.tracing import (
    attach_run_metadata,
    run_in_tracing_context,
    trace_ai,
    trace_research_job,
)

__all__ = [
    "ALLOWED_METADATA_KEYS",
    "FLUSH_BOUND_SECONDS",
    "LangSmithConfigurationError",
    "LangSmithHandle",
    "TracedResearchDrafter",
    "TracedResearchPlanner",
    "attach_run_metadata",
    "configure_langsmith",
    "correlation_metadata",
    "current_langsmith",
    "filter_metadata",
    "hide_metadata",
    "live_ai_provider_configured",
    "require_langsmith_for_live_ai",
    "reset_langsmith_for_tests",
    "run_in_tracing_context",
    "trace_ai",
    "trace_research_job",
]
