"""Kafka outbox relay executable: ``python -m atlas.outbox`` (Slice 13C1).

Kafka-only runtime composition. ``FakeEventProducer`` is never constructed
here -- it remains test-only, wired in directly by tests via dependency
injection into :class:`~atlas.outbox.relay.OutboxRelay`. There is no
settings-driven fake/real producer switch.

Startup order (fails closed, nonzero exit, on any step):

1. Load and validate settings.
2. Construct PostgreSQL session/engine dependencies.
3. Construct the Kafka producer.
4. Acquire the singleton PostgreSQL advisory lock.
5. Verify Kafka broker connectivity.
6. Verify the fixed topic exists with exactly one partition.
7. Enter the polling loop.

Runtime behavior:

- The relay is always constructed with ``batch_size=1``: one claimed row is
  published at a time, bounded by ``kafka_delivery_timeout_seconds``.
- SIGINT/SIGTERM stop new claims; the current in-flight record (if any)
  still finishes before shutdown.
- ``EMPTY`` / ``RECOVERABLE_FAILURE`` / ``OWNERSHIP_LOST`` outcomes back off
  for ``outbox_relay_poll_interval_seconds`` before the next claim attempt.
- A ``FATAL_FAILURE`` or ``UNEXPECTED_FAILURE`` outcome (the relay has
  already released the row's claim) or any unexpected PostgreSQL/session/
  advisory-lock error terminates the process nonzero. ``lock.held`` is never
  consulted to decide whether to keep running after such an error.
- Shutdown always attempts to close the producer and release the advisory
  lock, in that order, even if the producer close itself fails; a failure in
  either step makes the process exit nonzero.
- No HTTP server.

Logging discipline (Slice 15A1): every log call below goes through
``atlas.observability.logging.log_event``/``log_exception_boundary``,
which only ever accept a fixed :class:`~atlas.observability.events.Event`
name and the approved structured fields -- never a free-text message,
``str(exc)``, ``repr(exc)``, ``exc.args``, ``exc_info``, ``stack_info``, or
any value derived from settings (SQL, database URLs, Kafka broker
addresses, configuration values). Only ``exc.__class__.__name__`` may
represent an exception, via ``log_exception_boundary``.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from collections.abc import Callable
from types import FrameType

from atlas.config import get_settings
from atlas.config.settings import Settings
from atlas.observability.events import Event
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.outbox.errors import KafkaTopicVerificationError, OutboxError
from atlas.outbox.kafka_producer import KafkaEventProducer
from atlas.outbox.relay import OutboxRelay, RelayRunOutcome
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.outbox.topic_admin import (
    verify_broker_connectivity,
    verify_topic_partitioning,
)
from atlas.persistence.db import get_engine, get_session_factory
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository

logger = logging.getLogger(__name__)

_BACKOFF_OUTCOMES = frozenset(
    {
        RelayRunOutcome.EMPTY,
        RelayRunOutcome.RECOVERABLE_FAILURE,
        RelayRunOutcome.OWNERSHIP_LOST,
    }
)
_TERMINAL_OUTCOMES = frozenset(
    {
        RelayRunOutcome.FATAL_FAILURE,
        RelayRunOutcome.UNEXPECTED_FAILURE,
    }
)

SignalHandler = Callable[[int, FrameType | None], object] | int | None


def _run_poll_loop(
    relay: OutboxRelay,
    settings: Settings,
    shutdown_requested: Callable[[], bool],
) -> int:
    """Poll until shutdown or a terminal/unexpected error. Returns the exit code."""
    while not shutdown_requested():
        try:
            result = relay.run_once()
        except OutboxError as exc:
            log_exception_boundary(logger, Event.POLL_LOOP_TERMINAL_ERROR, exc)
            return 1
        except Exception as exc:
            log_exception_boundary(logger, Event.POLL_LOOP_TERMINAL_ERROR, exc)
            return 1

        if result.outcome in _TERMINAL_OUTCOMES:
            log_event(
                logger,
                Event.POLL_LOOP_TERMINAL_ERROR,
                level=logging.ERROR,
                outcome=result.outcome.value,
            )
            return 1
        if result.outcome in _BACKOFF_OUTCOMES:
            time.sleep(settings.outbox_relay_poll_interval_seconds)
    return 0


def _cleanup(
    *,
    producer: KafkaEventProducer,
    lock: PostgresOutboxRelayLock,
    close_timeout_seconds: float,
    previous_sigint: SignalHandler,
    previous_sigterm: SignalHandler,
) -> bool:
    """Best-effort, ordered shutdown. Returns ``True`` only if every step succeeded.

    Every step is always attempted, in order, regardless of whether an
    earlier step failed: producer close, then advisory-lock release, then
    restoring the previous signal handlers. A failure in producer close must
    never skip the lock release.
    """
    cleanup_ok = True

    try:
        producer.close(timeout_seconds=close_timeout_seconds)
    except Exception as exc:
        log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
        cleanup_ok = False

    try:
        lock.release()
    except Exception as exc:
        log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
        cleanup_ok = False

    signal.signal(signal.SIGINT, previous_sigint)
    signal.signal(signal.SIGTERM, previous_sigterm)
    return cleanup_ok


def main() -> int:
    """Run the Kafka outbox relay until interrupted or a terminal error occurs."""
    configure_logging(service_role="outbox-relay")
    settings = get_settings()
    engine = get_engine(settings.database_url)
    session_factory = get_session_factory(engine)

    try:
        producer = KafkaEventProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            delivery_timeout_seconds=settings.kafka_delivery_timeout_seconds,
            socket_timeout_seconds=settings.kafka_socket_timeout_seconds,
        )
    except OutboxError as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1

    lock = PostgresOutboxRelayLock(engine)
    try:
        lock.acquire()
    except Exception as exc:
        # Broadened beyond OutboxError: PostgresOutboxRelayLock.acquire()
        # itself only wraps its own advisory-lock SQL, not the initial
        # engine.connect() -- an unreachable/misconfigured PostgreSQL
        # surfaces as a raw SQLAlchemy/DBAPI exception here, not an
        # OutboxError. Never log str(exc)/repr(exc): that raw exception can
        # otherwise embed the configured host/port directly in its message.
        # This is a narrow startup-boundary catch, not a change to
        # PostgresOutboxRelayLock's own (unchanged) error contract.
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        try:
            producer.close(timeout_seconds=settings.kafka_delivery_timeout_seconds)
        except Exception as close_exc:
            # A close failure here must never mask the original
            # classification above -- it is logged separately, and this
            # path still returns 1 for the original failure either way.
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, close_exc)
        return 1

    shutdown_requested = False

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        nonlocal shutdown_requested
        log_event(logger, Event.SIGNAL_RECEIVED)
        shutdown_requested = True

    previous_sigint = signal.signal(signal.SIGINT, _handle_signal)
    previous_sigterm = signal.signal(signal.SIGTERM, _handle_signal)

    exit_code = 1
    try:
        try:
            verify_broker_connectivity(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                timeout_seconds=settings.kafka_topic_verify_timeout_seconds,
            )
            verify_topic_partitioning(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                timeout_seconds=settings.kafka_topic_verify_timeout_seconds,
            )
        except KafkaTopicVerificationError as exc:
            log_exception_boundary(logger, Event.STARTUP_VERIFICATION_FAILED, exc)
            return 1

        relay = OutboxRelay(
            session_factory=session_factory,
            repository=SqlAlchemyOutboxRepository(),
            producer=producer,
            lock=lock,
            batch_size=1,
            publish_lease_seconds=settings.outbox_publish_lease_seconds,
        )
        log_event(logger, Event.PROCESS_STARTED)
        exit_code = _run_poll_loop(relay, settings, lambda: shutdown_requested)
    finally:
        cleanup_ok = _cleanup(
            producer=producer,
            lock=lock,
            close_timeout_seconds=settings.kafka_delivery_timeout_seconds,
            previous_sigint=previous_sigint,
            previous_sigterm=previous_sigterm,
        )

    if not cleanup_ok:
        exit_code = 1
    log_event(logger, Event.PROCESS_STOPPED, outcome=str(exit_code))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
