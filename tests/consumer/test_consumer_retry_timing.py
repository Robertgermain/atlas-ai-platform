"""Network-free unit tests for ``atlas.consumer.timing`` (Slice 13C2B)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from atlas.consumer.timing import (
    ProcessingDeadline,
    RetryTimingParameters,
    backoff_delay_seconds,
    deterministic_backoff_sum_seconds,
    worst_case_attempt_seconds,
    worst_case_total_processing_seconds,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)

_APPROVED_DEFAULTS = RetryTimingParameters(
    max_attempts=3,
    base_seconds=1.0,
    max_backoff_seconds=30.0,
    jitter_max_seconds=0.0,
    safety_margin_seconds=60.0,
    db_connect_timeout_seconds=5.0,
    db_pool_timeout_seconds=5.0,
    db_statement_timeout_seconds=5.0,
    processing_overhead_seconds=2.0,
    max_db_round_trips_per_attempt=8,
)


def test_worst_case_attempt_seconds_matches_approved_defaults() -> None:
    assert worst_case_attempt_seconds(_APPROVED_DEFAULTS) == pytest.approx(52.0)


def test_backoff_delay_is_deterministic_exponential_with_zero_jitter() -> None:
    assert backoff_delay_seconds(_APPROVED_DEFAULTS, attempt_index=0) == pytest.approx(
        1.0
    )
    assert backoff_delay_seconds(_APPROVED_DEFAULTS, attempt_index=1) == pytest.approx(
        2.0
    )


def test_backoff_delay_is_capped_at_max_backoff_seconds() -> None:
    params = RetryTimingParameters(
        max_attempts=10,
        base_seconds=1.0,
        max_backoff_seconds=5.0,
        jitter_max_seconds=0.0,
        safety_margin_seconds=0.0,
        db_connect_timeout_seconds=1.0,
        db_pool_timeout_seconds=1.0,
        db_statement_timeout_seconds=1.0,
        processing_overhead_seconds=0.0,
        max_db_round_trips_per_attempt=1,
    )
    # 2**5 = 32, far above the 5.0 cap.
    assert backoff_delay_seconds(params, attempt_index=5) == pytest.approx(5.0)


def test_backoff_delay_adds_jitter_as_a_maximum_bound() -> None:
    params = RetryTimingParameters(
        max_attempts=3,
        base_seconds=1.0,
        max_backoff_seconds=30.0,
        jitter_max_seconds=2.5,
        safety_margin_seconds=0.0,
        db_connect_timeout_seconds=1.0,
        db_pool_timeout_seconds=1.0,
        db_statement_timeout_seconds=1.0,
        processing_overhead_seconds=0.0,
        max_db_round_trips_per_attempt=1,
    )
    assert backoff_delay_seconds(params, attempt_index=0) == pytest.approx(1.0 + 2.5)


def test_deterministic_backoff_sum_covers_max_attempts_minus_one_delays() -> None:
    # Two retries after the initial attempt (max_attempts=3): backoffs of 1s, 2s.
    assert deterministic_backoff_sum_seconds(_APPROVED_DEFAULTS) == pytest.approx(3.0)


def test_worst_case_total_processing_seconds_matches_approved_calculation() -> None:
    total = worst_case_total_processing_seconds(_APPROVED_DEFAULTS)
    assert total == pytest.approx(219.0)
    assert total < 300.0


def test_worst_case_total_with_a_single_attempt_has_no_backoff_sum() -> None:
    params = RetryTimingParameters(
        max_attempts=1,
        base_seconds=1.0,
        max_backoff_seconds=30.0,
        jitter_max_seconds=0.0,
        safety_margin_seconds=10.0,
        db_connect_timeout_seconds=1.0,
        db_pool_timeout_seconds=1.0,
        db_statement_timeout_seconds=1.0,
        processing_overhead_seconds=0.0,
        max_db_round_trips_per_attempt=1,
    )
    assert deterministic_backoff_sum_seconds(params) == pytest.approx(0.0)
    assert worst_case_total_processing_seconds(params) == pytest.approx(3.0 + 10.0)


class TestProcessingDeadline:
    def test_deadline_at_starts_from_message_received_at_minus_safety_margin(
        self,
    ) -> None:
        deadline = ProcessingDeadline(
            params=_APPROVED_DEFAULTS,
            max_poll_interval_seconds=300.0,
            message_received_at=T0,
        )
        assert deadline.deadline_at == T0 + timedelta(seconds=300.0 - 60.0)

    def test_can_start_attempt_is_true_with_ample_remaining_time(self) -> None:
        deadline = ProcessingDeadline(
            params=_APPROVED_DEFAULTS,
            max_poll_interval_seconds=300.0,
            message_received_at=T0,
        )
        assert deadline.can_start_attempt(now=T0) is True

    def test_can_start_attempt_is_false_once_inside_the_attempt_bound(self) -> None:
        deadline = ProcessingDeadline(
            params=_APPROVED_DEFAULTS,
            max_poll_interval_seconds=300.0,
            message_received_at=T0,
        )
        # deadline_at = T0 + 240s; worst_case_attempt = 52s.
        # At T0 + 189s, 189 + 52 = 241 > 240 -> no room for one more attempt.
        almost_at_deadline = T0 + timedelta(seconds=189)
        assert deadline.can_start_attempt(now=almost_at_deadline) is False

    def test_can_start_attempt_is_true_exactly_at_the_bound(self) -> None:
        deadline = ProcessingDeadline(
            params=_APPROVED_DEFAULTS,
            max_poll_interval_seconds=300.0,
            message_received_at=T0,
        )
        # deadline_at = T0 + 240s; now + 52 == 240 exactly -> boundary is inclusive.
        exactly_at_bound = T0 + timedelta(seconds=188)
        assert deadline.can_start_attempt(now=exactly_at_bound) is True

    def test_can_afford_backoff_requires_backoff_plus_another_full_attempt(
        self,
    ) -> None:
        deadline = ProcessingDeadline(
            params=_APPROVED_DEFAULTS,
            max_poll_interval_seconds=300.0,
            message_received_at=T0,
        )
        # deadline_at = T0 + 240s. A 190s backoff + 52s attempt = 242 > 240.
        assert deadline.can_afford_backoff(now=T0, backoff_seconds=190.0) is False
        # A 187s backoff + 52s attempt = 239 <= 240.
        assert deadline.can_afford_backoff(now=T0, backoff_seconds=187.0) is True

    def test_zero_safety_margin_leaves_the_full_poll_interval_available(self) -> None:
        params = RetryTimingParameters(
            max_attempts=3,
            base_seconds=1.0,
            max_backoff_seconds=30.0,
            jitter_max_seconds=0.0,
            safety_margin_seconds=0.0,
            db_connect_timeout_seconds=5.0,
            db_pool_timeout_seconds=5.0,
            db_statement_timeout_seconds=5.0,
            processing_overhead_seconds=2.0,
            max_db_round_trips_per_attempt=8,
        )
        deadline = ProcessingDeadline(
            params=params, max_poll_interval_seconds=300.0, message_received_at=T0
        )
        assert deadline.deadline_at == T0 + timedelta(seconds=300.0)
