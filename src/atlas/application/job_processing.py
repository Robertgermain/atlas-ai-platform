"""Typed processor outcomes for research-job workers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CompletedProcessing:
    """Graph produced an accepted result; worker finalizes job COMPLETED."""

    result: str
    workflow_execution_id: str


@dataclass(frozen=True, slots=True)
class PausedForReview:
    """Job moved to AWAITING_REVIEW; worker must not finalize."""

    workflow_execution_id: str


@dataclass(frozen=True, slots=True)
class RetryScheduled:
    """Job delayed PENDING with JOB_RETRY; worker must not finalize."""

    workflow_execution_id: str
    next_attempt_at: datetime
    attempt_number: int


@dataclass(frozen=True, slots=True)
class TerminalFailed:
    """Execution FAILED; job still RUNNING under claim; worker finalizes FAILED."""

    reason_code: str
    workflow_execution_id: str | None


ProcessingOutcome = (
    CompletedProcessing | PausedForReview | RetryScheduled | TerminalFailed
)


class ContinuationMode(StrEnum):
    """Durable continuation modes for claimed research jobs."""

    NONE = "NONE"
    JOB_RETRY = "JOB_RETRY"
    REVIEW_COMPLETE = "REVIEW_COMPLETE"


class ResearchJobProcessor(Protocol):
    """Process a claimed research question for a durable job id.

    Implementations must accept ``job_id``, ``claim_token``, and the consumed
    continuation mode carried by the claim. Processors must not finalize
    ResearchJob COMPLETED/FAILED for pause/retry outcomes; the worker owns
    claim-fenced completion and terminal job failure only.
    """

    def __call__(
        self,
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: ContinuationMode,
        active_workflow_execution_id: str | None,
    ) -> ProcessingOutcome: ...
