"""Deterministic research-job processing for the background worker."""

from __future__ import annotations

from collections.abc import Callable

ResearchJobProcessor = Callable[[str], str]


def process_research_question(question: str) -> str:
    """Return the Milestone 6 deterministic research result."""
    return f"Research completed for: {question}"
