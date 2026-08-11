"""Network-free unit tests for ``atlas.consumer.__main__._run_poll_loop``
(correction pass, Slice 13C2B).

Covers the process-lifetime Kafka poll-recovery loop directly: its
termination rules, and the once-per-outage warning mechanism that keeps a
prolonged broker outage from spamming a fresh warning on every polling
cycle. No real PostgreSQL or Kafka connection is made in this file.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

import atlas.consumer.__main__ as consumer_main
from atlas.consumer.errors import (
    ConsumerError,
    ConsumerShutdownRequestedError,
    TransientKafkaError,
)
from atlas.consumer.runner import ProcessOutcome


class _FakeRunner:
    """A ``ConsumerRunner`` stand-in whose ``run_once()`` replays a scripted
    sequence."""

    def __init__(self, outcomes: list[ProcessOutcome | Exception]) -> None:
        self._outcomes = list(outcomes)
        self.calls = 0

    def run_once(self) -> ProcessOutcome:
        self.calls += 1
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _shutdown_after(n: int) -> Callable[[], bool]:
    """``False`` for the first ``n`` calls, ``True`` thereafter."""
    state = {"calls": 0}

    def _shutdown() -> bool:
        should_stop = state["calls"] >= n
        state["calls"] += 1
        return should_stop

    return _shutdown


def _wait_stub(*, requests_shutdown: bool = False) -> Callable[[float], bool]:
    def _wait(_seconds: float) -> bool:
        return requests_shutdown

    return _wait


# --- basic termination rules ------------------------------------------------


def test_stops_immediately_when_shutdown_already_requested() -> None:
    runner = _FakeRunner([])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: True,
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 0
    assert runner.calls == 0


def test_no_message_loops_and_stops_cleanly_at_shutdown() -> None:
    runner = _FakeRunner([ProcessOutcome.NO_MESSAGE])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=_shutdown_after(1),
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 0
    assert runner.calls == 1


def test_shutdown_requested_error_stops_cleanly() -> None:
    runner = _FakeRunner([ConsumerShutdownRequestedError()])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: False,
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 0
    assert runner.calls == 1


def test_fatal_consumer_error_terminates_immediately() -> None:
    runner = _FakeRunner([ConsumerError("Fatal")])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: False,
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 1
    assert runner.calls == 1


def test_unexpected_error_terminates_immediately() -> None:
    runner = _FakeRunner([RuntimeError("boom")])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: False,
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 1
    assert runner.calls == 1


# --- process-lifetime transient poll recovery -------------------------------


def test_transient_poll_failure_backs_off_and_continues_polling() -> None:
    """A poll()-site TransientKafkaError never terminates the process by itself."""
    runner = _FakeRunner(
        [TransientKafkaError("PollTimeout"), ProcessOutcome.NO_MESSAGE]
    )
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=_shutdown_after(2),
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 0
    assert runner.calls == 2


# --- once-per-outage warning mechanism --------------------------------------


def test_one_warning_across_repeated_transient_poll_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Five consecutive transient poll failures in one outage log exactly one
    warning."""
    runner = _FakeRunner([TransientKafkaError("PollTimeout") for _ in range(5)])
    with caplog.at_level(logging.WARNING):
        exit_code = consumer_main._run_poll_loop(
            runner,  # type: ignore[arg-type]
            shutdown_requested=_shutdown_after(5),
            wait=_wait_stub(),
            kafka_retry_backoff_seconds=0.001,
        )
    assert exit_code == 0
    assert runner.calls == 5
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "Transient Kafka error" in warnings[0].message


def test_recovery_resets_the_outage_episode(caplog: pytest.LogCaptureFixture) -> None:
    """A successful no-message poll between two outages resets the tracker, so
    each outage gets its own warning."""
    runner = _FakeRunner(
        [
            TransientKafkaError("PollTimeout"),
            TransientKafkaError("PollTimeout"),
            ProcessOutcome.NO_MESSAGE,
            TransientKafkaError("PollTimeout"),
        ]
    )
    with caplog.at_level(logging.WARNING):
        exit_code = consumer_main._run_poll_loop(
            runner,  # type: ignore[arg-type]
            shutdown_requested=_shutdown_after(4),
            wait=_wait_stub(),
            kafka_retry_backoff_seconds=0.001,
        )
    assert exit_code == 0
    assert runner.calls == 4
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2  # one for the first outage, one for the second


def test_a_later_outage_after_a_processed_message_also_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Recovery via an actually-processed message (not just NO_MESSAGE) must
    also reset the tracker."""
    runner = _FakeRunner(
        [
            TransientKafkaError("PollTimeout"),
            ProcessOutcome.APPLIED,
            TransientKafkaError("PollTimeout"),
            TransientKafkaError("PollTimeout"),
        ]
    )
    with caplog.at_level(logging.WARNING):
        exit_code = consumer_main._run_poll_loop(
            runner,  # type: ignore[arg-type]
            shutdown_requested=_shutdown_after(4),
            wait=_wait_stub(),
            kafka_retry_backoff_seconds=0.001,
        )
    assert exit_code == 0
    assert runner.calls == 4
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2


def test_shutdown_during_poll_backoff_stops_cleanly() -> None:
    """``wait()`` reporting shutdown mid-backoff stops the loop immediately,
    without waiting for the outer ``shutdown_requested`` check."""
    runner = _FakeRunner([TransientKafkaError("PollTimeout")])
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: False,
        wait=_wait_stub(requests_shutdown=True),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 0
    assert runner.calls == 1


def test_fatal_polling_failure_still_terminates_immediately_mid_outage() -> None:
    """A fatal ``ConsumerError`` must terminate even while an outage warning
    episode is already active -- the warning suppression never suppresses
    the fatal-termination path itself."""
    runner = _FakeRunner(
        [TransientKafkaError("PollTimeout"), ConsumerError("PollReturnedBrokerError")]
    )
    exit_code = consumer_main._run_poll_loop(
        runner,  # type: ignore[arg-type]
        shutdown_requested=lambda: False,
        wait=_wait_stub(),
        kafka_retry_backoff_seconds=0.001,
    )
    assert exit_code == 1
    assert runner.calls == 2


def test_outage_warning_logs_remain_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _FakeRunner([TransientKafkaError("PollTimeout")])
    with caplog.at_level(logging.WARNING):
        consumer_main._run_poll_loop(
            runner,  # type: ignore[arg-type]
            shutdown_requested=_shutdown_after(1),
            wait=_wait_stub(),
            kafka_retry_backoff_seconds=0.001,
        )
    assert "TransientKafkaError" in caplog.text
    # Only the sanitized exception class name is logged, never str(exc).
    assert "PollTimeout" not in caplog.text


# --- _OutageWarningTracker itself -------------------------------------------


def test_outage_warning_tracker_warns_once_then_suppresses() -> None:
    tracker = consumer_main._OutageWarningTracker()
    assert tracker.should_warn() is True
    assert tracker.should_warn() is False
    assert tracker.should_warn() is False


def test_outage_warning_tracker_reset_allows_a_new_warning() -> None:
    tracker = consumer_main._OutageWarningTracker()
    assert tracker.should_warn() is True
    tracker.reset()
    assert tracker.should_warn() is True
