"""Worst-case timing bound and runtime processing-deadline helpers (Slice 13C2B).

The formula here intentionally mirrors ``atlas.config.settings.Settings.
_validate_consumer_retry_timing_margin`` exactly. It is duplicated rather
than shared via import to avoid a config -> consumer import cycle (see that
validator's docstring) -- ``test_consumer_retry_timing.py`` proves both
copies agree for the approved defaults. The one piece that *is* shared
(safe, since it has no dependency direction problem) is the effective-
timeout ceiling conversion in ``atlas.config.timeout_math``, used by both
this module and ``atlas.consumer.db.build_consumer_engine`` so the timing
proof and the real runtime engine always agree on what a configured
connect/statement timeout actually becomes.

Safety comes entirely from PostgreSQL/connection/pool-level timeout bounds
plus this conservative statement-count accounting -- never from an assumed
Python-side interrupt of an already-running synchronous database call. The
deadline checks below only ever run *between* attempts/backoffs, never
during one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from atlas.config.timeout_math import (
    effective_connect_timeout_seconds,
    effective_statement_timeout_seconds,
)


@dataclass(frozen=True, slots=True)
class RetryTimingParameters:
    """The subset of ``Settings`` this module's calculations depend on."""

    max_attempts: int
    base_seconds: float
    max_backoff_seconds: float
    jitter_max_seconds: float
    safety_margin_seconds: float
    db_connect_timeout_seconds: float
    db_pool_timeout_seconds: float
    db_statement_timeout_seconds: float
    processing_overhead_seconds: float
    max_db_round_trips_per_attempt: int


def worst_case_attempt_seconds(params: RetryTimingParameters) -> float:
    """Conservative upper bound on one processing attempt's wall-clock time.

    Uses ``effective_connect_timeout_seconds``/``effective_statement_
    timeout_seconds`` (ceiling-rounded, floored at 1) rather than the raw
    configured floats -- the exact same conversion ``atlas.consumer.db.
    build_consumer_engine`` applies to the real engine -- so this bound is
    never smaller than what runtime actually enforces.
    """
    return (
        params.db_pool_timeout_seconds
        + effective_connect_timeout_seconds(params.db_connect_timeout_seconds)
        + params.max_db_round_trips_per_attempt
        * effective_statement_timeout_seconds(params.db_statement_timeout_seconds)
        + params.processing_overhead_seconds
    )


def deterministic_backoff_sum_seconds(params: RetryTimingParameters) -> float:
    """Sum of every backoff delay between attempts (exact: jitter is a max bound)."""
    return sum(
        backoff_delay_seconds(params, attempt_index=index)
        for index in range(params.max_attempts - 1)
    )


def worst_case_total_processing_seconds(params: RetryTimingParameters) -> float:
    """Conservative upper bound on the entire bounded-retry episode."""
    return (
        params.max_attempts * worst_case_attempt_seconds(params)
        + deterministic_backoff_sum_seconds(params)
        + params.safety_margin_seconds
    )


def backoff_delay_seconds(
    params: RetryTimingParameters, *, attempt_index: int
) -> float:
    """Exponential backoff before the ``attempt_index``-th retry (0-based).

    Deterministic when ``jitter_max_seconds == 0`` (the approved default);
    otherwise this is the maximum possible delay, consistent with using
    the maximum for every worst-case timing sum above.
    """
    exponential = params.base_seconds * (2**attempt_index)
    return float(
        min(exponential, params.max_backoff_seconds) + params.jitter_max_seconds
    )


class ProcessingDeadline:
    """Per-message admission-control deadline, independent of the static validator.

    Starts the instant Kafka returns the message. Every check below is a
    between-attempts/between-backoffs admission gate -- never a preemption
    of an in-flight synchronous call.
    """

    def __init__(
        self,
        *,
        params: RetryTimingParameters,
        max_poll_interval_seconds: float,
        message_received_at: datetime,
    ) -> None:
        self._params = params
        self._deadline_at = message_received_at + timedelta(
            seconds=max_poll_interval_seconds - params.safety_margin_seconds
        )

    @property
    def deadline_at(self) -> datetime:
        return self._deadline_at

    def can_start_attempt(self, *, now: datetime) -> bool:
        """Require the entire conservative attempt bound to fit before the deadline."""
        bound = worst_case_attempt_seconds(self._params)
        return now + timedelta(seconds=bound) <= self._deadline_at

    def can_afford_backoff(self, *, now: datetime, backoff_seconds: float) -> bool:
        """Require the backoff plus another full attempt to fit before the deadline."""
        bound = backoff_seconds + worst_case_attempt_seconds(self._params)
        return now + timedelta(seconds=bound) <= self._deadline_at
