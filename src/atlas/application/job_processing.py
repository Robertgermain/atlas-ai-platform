"""Research-job processing boundary for the background worker."""

from __future__ import annotations

from typing import Protocol


class ResearchJobProcessor(Protocol):
    """Process a claimed research question for a durable job id.

    Implementations must accept ``job_id`` so durable workflows can use it as a
    stable LangGraph ``thread_id``. Processors must not finalize ResearchJob
    lifecycle state; the worker owns claim fencing and completion/failure.
    """

    def __call__(self, question: str, *, job_id: str) -> str: ...
