"""Business Kafka consumer executable: ``python -m atlas.consumer`` (Slice 13C2A).

No HTTP surface, matching ``python -m atlas.outbox``'s precedent: the
process's own exit code is the health signal for its process supervisor,
not a readiness/liveness endpoint. A future slice may add one, but it must
not claim that a healthy process means every partition or business handler
is healthy -- see Slice 13C2B.

Startup order (fails closed, nonzero exit, on any step):

1. Load and validate settings.
2. Construct PostgreSQL session/engine dependencies.
3. Construct the Kafka consumer (subscribes to the fixed reserved topic
   under the fixed allowlisted consumer group).
4. Install SIGINT/SIGTERM handlers.
5. Verify Kafka broker connectivity.
6. Verify the fixed topic exists with exactly one partition.
7. Enter the polling loop.

Every step above is wrapped so a failure yields a sanitized log line and a
nonzero exit rather than an uncaught traceback. Steps 1-2 (no consumer yet)
never attempt consumer cleanup; step 3 either succeeds or leaves no
consumer to clean up (its own subscribe-failure path already closes the
partially-constructed consumer -- see ``KafkaEventConsumer``); steps 4+
always attempt to close the already-constructed consumer before returning
on failure. Step 4 itself installs SIGINT then SIGTERM one at a time and is
not assumed atomic: if installing SIGTERM fails after SIGINT was already
replaced, the already-replaced SIGINT handler is restored before the
consumer is closed and the process exits -- see ``_install_signal_handlers``
and ``_restore_signal_handlers``.

Unlike ``OutboxRelay``, no PostgreSQL advisory lock is acquired here:
Kafka's own consumer-group protocol already guarantees at most one process
actively owns the topic's single partition at a time.

Runtime behavior:

- Each poll is bounded by ``consumer_poll_timeout_seconds``, so SIGINT/
  SIGTERM are observed between polls even when no records arrive.
- The Kafka offset for a record is committed synchronously, once, only
  after that record's PostgreSQL transaction (inbox record + business
  effect) has committed. A duplicate delivery is detected by the inbox and
  its effect is skipped, but the duplicate's offset is still committed.
- Permanent, record-specific poison (malformed envelope, invalid headers,
  or a lifecycle-order violation -- see ``atlas.consumer.errors.
  PoisonEventError``) is durably dead-lettered to PostgreSQL and its offset
  is still committed (Slice 13C2B) -- it never blocks the partition.
- A transient PostgreSQL failure (see ``atlas.consumer.db_classify``)
  receives bounded, process-local retry with exponential backoff, bounded
  by a runtime processing deadline safely under
  ``consumer_max_poll_interval_seconds``; exhaustion terminates the process
  nonzero with the offset uncommitted.
- Every other error (a fatal/unrecognized database error, an unexpected
  Kafka error, an internal inbox conflict, or any unexpected exception)
  terminates the process nonzero immediately, with no retry and no offset
  commit.

Bounded per-record retry versus process-lifetime poll recovery (Slice
13C2B correction pass) -- these are two deliberately different recovery
horizons, not one:

- Per-record processing retry (inside ``ConsumerRunner._apply_with_retry``/
  ``_dead_letter_and_commit``) and the offset-commit retry (``_commit_
  with_retry``) are bounded: a fixed number of attempts, exponential
  backoff, and a runtime processing deadline safely under
  ``consumer_max_poll_interval_seconds``. Exhaustion terminates the
  process.
- A ``TransientKafkaError`` raised by ``poll()`` itself -- i.e. *before* any
  record is in hand -- is different: it is process-lifetime broker-
  reconnect recovery, handled by ``_run_poll_loop`` below, and may continue
  indefinitely across separate polling cycles for as long as the process
  runs, until either the broker recovers or shutdown is requested. This is
  intentional -- a prolonged broker outage should not by itself terminate
  the consumer -- and is bounded only by wall-clock shutdown, never by an
  attempt count.
- No Kafka polling ever occurs while an in-hand record is backing off
  between attempts: ``poll()`` is called exactly once at the top of
  ``ConsumerRunner.run_once()``, before any retry loop begins.
- Fatal or unrecognized Kafka errors (anything ``KafkaEventConsumer``
  classifies as non-recoverable) still terminate the process immediately,
  with no retry at either horizon.
- Shutdown always attempts to close the consumer (triggering a clean
  consumer-group leave) and independently attempts to restore each
  previously-installed signal handler; a failure in any one of these steps
  never skips the others, and any such failure makes the process exit
  nonzero.

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
import threading
from collections.abc import Callable
from types import FrameType

from atlas.config import get_settings
from atlas.config.settings import Settings
from atlas.consumer.db import build_consumer_engine
from atlas.consumer.errors import (
    ConsumerError,
    ConsumerShutdownRequestedError,
    TransientKafkaError,
)
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.kafka_consumer import KafkaEventConsumer
from atlas.consumer.runner import ConsumerRunner
from atlas.consumer.timing import RetryTimingParameters
from atlas.consumer.wait import Waiter, build_shutdown_aware_waiter
from atlas.observability.events import Event
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.observability.metrics import (
    AtlasMetrics,
    default_metrics,
    start_metrics_http_server,
)
from atlas.observability.tracing import configure_tracing
from atlas.outbox.errors import KafkaTopicVerificationError
from atlas.outbox.topic_admin import (
    verify_broker_connectivity,
    verify_topic_partitioning,
)
from atlas.persistence.db import get_session_factory
from atlas.persistence.repositories.consumer_dead_letter import (
    SqlAlchemyDeadLetterRepository,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

logger = logging.getLogger(__name__)

SignalHandler = Callable[[int, FrameType | None], object] | int | None

#: Installed/restored in this fixed order everywhere below (SIGINT then
#: SIGTERM), matching ``atlas.outbox.__main__``'s equivalent precedent.
_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM)


def _timing_params_from_settings(settings: Settings) -> RetryTimingParameters:
    return RetryTimingParameters(
        max_attempts=settings.consumer_retry_max_attempts,
        base_seconds=settings.consumer_retry_base_seconds,
        max_backoff_seconds=settings.consumer_retry_max_backoff_seconds,
        jitter_max_seconds=settings.consumer_retry_jitter_max_seconds,
        safety_margin_seconds=settings.consumer_retry_safety_margin_seconds,
        db_connect_timeout_seconds=settings.consumer_db_connect_timeout_seconds,
        db_pool_timeout_seconds=settings.consumer_db_pool_timeout_seconds,
        db_statement_timeout_seconds=settings.consumer_db_statement_timeout_seconds,
        processing_overhead_seconds=settings.consumer_retry_processing_overhead_seconds,
        max_db_round_trips_per_attempt=settings.consumer_max_db_round_trips_per_attempt,
    )


class _OutageWarningTracker:
    """Thread-safe once-per-outage transient-poll warning (Slice 13C2B correction).

    A prolonged broker outage makes ``_run_poll_loop`` retry ``poll()``
    indefinitely (see its docstring's process-lifetime recovery horizon),
    potentially for a very long time. Logging a fresh warning on every one
    of those iterations would spam logs at roughly the polling interval for
    as long as the outage lasts. This tracks whether the *current* outage
    episode has already produced its one warning, and ``reset()`` (called
    after any successful poll/no-message result) clears that so a *later*,
    separate outage still produces its own new warning. Logs remain
    sanitized regardless: this only gates *whether* to log, never what.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._already_warned = False

    def should_warn(self) -> bool:
        """``True`` at most once per outage episode; call once per transient failure."""
        with self._lock:
            if self._already_warned:
                return False
            self._already_warned = True
            return True

    def reset(self) -> None:
        """Clear the outage episode after any successful poll/no-message result."""
        with self._lock:
            self._already_warned = False


def _run_poll_loop(
    runner: ConsumerRunner,
    *,
    shutdown_requested: Callable[[], bool],
    wait: Waiter,
    kafka_retry_backoff_seconds: float,
    metrics: AtlasMetrics | None = None,
) -> int:
    """Poll until shutdown or an error. Returns the exit code.

    ``ProcessOutcome.NO_MESSAGE`` loops immediately: ``poll()`` itself
    already blocked for up to ``consumer_poll_timeout_seconds``, so no
    extra sleep is needed to bound the loop's pace.

    A ``TransientKafkaError`` raised directly from ``poll()`` (before any
    record is in hand -- see ``KafkaEventConsumer.poll()``'s classification)
    is evidence-backed recoverable: rather than terminating the whole
    process, this backs off (shutdown-aware, never polling Kafka during the
    wait) and polls again -- this is process-lifetime broker-reconnect
    recovery and may continue until shutdown, a distinct and unbounded
    horizon from the bounded per-record retry inside ``runner.run_once()``
    (see module docstring). Only the first such failure in a given outage
    episode is logged (``_OutageWarningTracker``); every other error is
    fatal. A ``ConsumerShutdownRequestedError`` (shutdown observed
    mid-backoff inside ``runner.run_once()``) is a clean stop, not a
    failure.
    """
    active_metrics = metrics or default_metrics()
    outage_warning = _OutageWarningTracker()
    while not shutdown_requested():
        try:
            outcome = runner.run_once()
        except ConsumerShutdownRequestedError:
            log_event(logger, Event.POLL_LOOP_SHUTDOWN_DURING_BACKOFF)
            return 0
        except TransientKafkaError as exc:
            active_metrics.observe_consumer_message(outcome="poll_recoverable_error")
            if outage_warning.should_warn():
                log_exception_boundary(
                    logger,
                    Event.POLL_LOOP_RECOVERABLE_ERROR,
                    exc,
                    level=logging.WARNING,
                )
            if wait(kafka_retry_backoff_seconds):
                return 0
        except ConsumerError as exc:
            active_metrics.observe_consumer_message(outcome="terminal_error")
            log_exception_boundary(logger, Event.POLL_LOOP_TERMINAL_ERROR, exc)
            return 1
        except Exception as exc:
            active_metrics.observe_consumer_message(outcome="terminal_error")
            log_exception_boundary(logger, Event.POLL_LOOP_TERMINAL_ERROR, exc)
            return 1
        else:
            active_metrics.observe_consumer_message(outcome=outcome.value)
            outage_warning.reset()
    return 0


def _install_signal_handlers(
    handler: Callable[[int, FrameType | None], object],
) -> tuple[dict[int, SignalHandler], Exception | None]:
    """Install ``handler`` for every signal in ``_SIGNALS``, one at a time.

    Installation is not atomic at the OS level: each successfully installed
    signal's *previous* handler is recorded in ``installed`` as it succeeds,
    so if a later signal's installation fails, the caller can still see
    (and reverse) exactly which signals were already replaced. Returns
    ``(installed, None)`` on full success, or ``(installed_so_far, exc)`` on
    the first failure -- ``exc`` is never logged directly by this function;
    the caller is responsible for sanitized logging.
    """
    installed: dict[int, SignalHandler] = {}
    for signum in _SIGNALS:
        try:
            installed[signum] = signal.signal(signum, handler)
        except Exception as exc:
            return installed, exc
    return installed, None


def _restore_signal_handlers(previous_handlers: dict[int, SignalHandler]) -> bool:
    """Best-effort restore of every given previously-installed signal handler.

    Each signal is restored independently: a failure restoring one must
    never prevent attempting the other, and no restoration failure may ever
    escape as an uncaught exception. Returns ``True`` only if every
    restoration succeeded.
    """
    all_ok = True
    for signum, previous_handler in previous_handlers.items():
        try:
            signal.signal(signum, previous_handler)
        except Exception as exc:
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
            all_ok = False
    return all_ok


def _cleanup(
    *,
    consumer: KafkaEventConsumer,
    previous_handlers: dict[int, SignalHandler],
) -> bool:
    """Best-effort shutdown. Returns ``True`` only if every step succeeded.

    Consumer close and signal-handler restoration are both always attempted
    regardless of whether an earlier step failed.
    """
    cleanup_ok = True
    try:
        consumer.close()
    except Exception as exc:
        log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
        cleanup_ok = False

    if not _restore_signal_handlers(previous_handlers):
        cleanup_ok = False

    return cleanup_ok


def main() -> int:
    """Run the research-job projection consumer until interrupted or an error occurs."""
    configure_logging(service_role="consumer")
    metrics_server = None
    tracing_handle = None

    try:
        settings = get_settings()
        tracing_handle = configure_tracing(
            service_name="atlas-consumer",
            deployment_environment=settings.otel_deployment_environment,
            otlp_traces_endpoint=settings.otel_exporter_otlp_traces_endpoint,
        )
        metrics_server = start_metrics_http_server(port=settings.metrics_port)
        engine = build_consumer_engine(
            settings.database_url,
            connect_timeout_seconds=settings.consumer_db_connect_timeout_seconds,
            pool_timeout_seconds=settings.consumer_db_pool_timeout_seconds,
            statement_timeout_seconds=settings.consumer_db_statement_timeout_seconds,
        )
        session_factory = get_session_factory(engine)
    except Exception as exc:
        # No consumer exists yet -- nothing to clean up. A settings load
        # failure (e.g. a Pydantic validation error) can otherwise embed the
        # invalid environment-derived value in its own message, so only the
        # sanitized exception class name is ever logged here.
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        if metrics_server is not None:
            metrics_server.close()
        if tracing_handle is not None:
            tracing_handle.close()
        return 1

    try:
        consumer = KafkaEventConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
            session_timeout_seconds=settings.consumer_session_timeout_seconds,
            max_poll_interval_seconds=settings.consumer_max_poll_interval_seconds,
        )
    except Exception as exc:
        # Construction either fully failed (nothing to clean up -- see
        # ``KafkaEventConsumer``'s own subscribe-failure close path) or fully
        # succeeded; there is no partially-constructed consumer to close here.
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        metrics_server.close()
        tracing_handle.close()
        return 1

    # From this point on the consumer exists and must always be closed.
    shutdown_requested = False

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        nonlocal shutdown_requested
        log_event(logger, Event.SIGNAL_RECEIVED)
        shutdown_requested = True

    installed_handlers, install_error = _install_signal_handlers(_handle_signal)
    if install_error is not None:
        log_exception_boundary(logger, Event.STARTUP_FAILED, install_error)
        # Reverse whichever signals were already replaced before the
        # failure, then close the consumer -- both are independent
        # best-effort steps and neither may mask this classification.
        _restore_signal_handlers(installed_handlers)
        try:
            consumer.close()
        except Exception as close_exc:
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, close_exc)
        metrics_server.close()
        tracing_handle.close()
        return 1

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

        wait = build_shutdown_aware_waiter(lambda: shutdown_requested)
        runner = ConsumerRunner(
            consumer=consumer,
            session_factory=session_factory,
            inbox=SqlAlchemyInboxRepository(),
            projection=SqlAlchemyResearchJobProjectionRepository(),
            dead_letters=SqlAlchemyDeadLetterRepository(),
            consumer_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
            poll_timeout_seconds=settings.consumer_poll_timeout_seconds,
            max_poll_interval_seconds=settings.consumer_max_poll_interval_seconds,
            timing_params=_timing_params_from_settings(settings),
            wait=wait,
            metrics=default_metrics(),
        )
        log_event(logger, Event.PROCESS_STARTED)
        exit_code = _run_poll_loop(
            runner,
            shutdown_requested=lambda: shutdown_requested,
            wait=wait,
            kafka_retry_backoff_seconds=settings.consumer_poll_timeout_seconds,
            metrics=default_metrics(),
        )
    finally:
        cleanup_ok = _cleanup(
            consumer=consumer,
            previous_handlers=installed_handlers,
        )
        metrics_server.close()
        tracing_handle.close()

    if not cleanup_ok:
        exit_code = 1
    log_event(logger, Event.PROCESS_STOPPED, outcome=str(exit_code))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
