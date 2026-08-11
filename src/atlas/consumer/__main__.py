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
- Any error -- a decode/validation failure, an inconsistent lifecycle
  transition, a PostgreSQL error, or a Kafka error -- terminates the
  process nonzero. Slice 13C2A has no retry/backoff/DLQ policy; that is
  Slice 13C2B's scope.
- Shutdown always attempts to close the consumer (triggering a clean
  consumer-group leave) and independently attempts to restore each
  previously-installed signal handler; a failure in any one of these steps
  never skips the others, and any such failure makes the process exit
  nonzero.

Logging discipline: every log call below uses only a fixed, sanitized
message and -- where useful -- ``exc.__class__.__name__``. Never ``str(exc)``,
``repr(exc)``, ``exc.args``, ``exc_info``/``logger.exception()``, or any
value derived from settings (SQL, database URLs, Kafka broker addresses,
configuration values).
"""

from __future__ import annotations

import logging
import signal
import sys
from collections.abc import Callable
from types import FrameType

from atlas.config import get_settings
from atlas.consumer.errors import ConsumerError
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.kafka_consumer import KafkaEventConsumer
from atlas.consumer.runner import ConsumerRunner
from atlas.outbox.errors import KafkaTopicVerificationError
from atlas.outbox.topic_admin import (
    verify_broker_connectivity,
    verify_topic_partitioning,
)
from atlas.persistence.db import get_engine, get_session_factory
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

logger = logging.getLogger(__name__)

SignalHandler = Callable[[int, FrameType | None], object] | int | None

#: Installed/restored in this fixed order everywhere below (SIGINT then
#: SIGTERM), matching ``atlas.outbox.__main__``'s equivalent precedent.
_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM)


def _run_poll_loop(
    runner: ConsumerRunner,
    shutdown_requested: Callable[[], bool],
) -> int:
    """Poll until shutdown or an error. Returns the exit code.

    ``ProcessOutcome.NO_MESSAGE`` loops immediately: ``poll()`` itself
    already blocked for up to ``consumer_poll_timeout_seconds``, so no
    extra sleep is needed to bound the loop's pace.
    """
    while not shutdown_requested():
        try:
            runner.run_once()
        except ConsumerError as exc:
            logger.error(
                "Unexpected consumer error; terminating. error_class=%s",
                exc.__class__.__name__,
            )
            return 1
        except Exception as exc:
            logger.error(
                "Unexpected PostgreSQL/session/Kafka error; terminating. "
                "error_class=%s",
                exc.__class__.__name__,
            )
            return 1
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
            logger.error(
                "Failed to restore a shutdown signal handler. error_class=%s",
                exc.__class__.__name__,
            )
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
        logger.error(
            "Kafka consumer close failed during shutdown. error_class=%s",
            exc.__class__.__name__,
        )
        cleanup_ok = False

    if not _restore_signal_handlers(previous_handlers):
        cleanup_ok = False

    return cleanup_ok


def main() -> int:
    """Run the research-job projection consumer until interrupted or an error occurs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        settings = get_settings()
        engine = get_engine(settings.database_url)
        session_factory = get_session_factory(engine)
    except Exception as exc:
        # No consumer exists yet -- nothing to clean up. A settings load
        # failure (e.g. a Pydantic validation error) can otherwise embed the
        # invalid environment-derived value in its own message, so only the
        # sanitized exception class name is ever logged here.
        logger.error(
            "Failed to load settings or construct database dependencies; "
            "exiting. error_class=%s",
            exc.__class__.__name__,
        )
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
        logger.error(
            "Failed to construct the Kafka consumer; exiting. error_class=%s",
            exc.__class__.__name__,
        )
        return 1

    # From this point on the consumer exists and must always be closed.
    shutdown_requested = False

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        nonlocal shutdown_requested
        logger.info("Received signal %s; stopping after the current record.", signum)
        shutdown_requested = True

    installed_handlers, install_error = _install_signal_handlers(_handle_signal)
    if install_error is not None:
        logger.error(
            "Failed to install shutdown signal handlers; exiting. error_class=%s",
            install_error.__class__.__name__,
        )
        # Reverse whichever signals were already replaced before the
        # failure, then close the consumer -- both are independent
        # best-effort steps and neither may mask this classification.
        _restore_signal_handlers(installed_handlers)
        try:
            consumer.close()
        except Exception as close_exc:
            logger.error(
                "Kafka consumer close failed during shutdown. error_class=%s",
                close_exc.__class__.__name__,
            )
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
            logger.error(
                "Kafka startup verification failed; exiting. error_class=%s",
                exc.__class__.__name__,
            )
            return 1

        runner = ConsumerRunner(
            consumer=consumer,
            session_factory=session_factory,
            inbox=SqlAlchemyInboxRepository(),
            projection=SqlAlchemyResearchJobProjectionRepository(),
            consumer_id=RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
            poll_timeout_seconds=settings.consumer_poll_timeout_seconds,
        )
        logger.info("Starting research-job projection consumer.")
        exit_code = _run_poll_loop(runner, lambda: shutdown_requested)
    finally:
        cleanup_ok = _cleanup(
            consumer=consumer,
            previous_handlers=installed_handlers,
        )

    if not cleanup_ok:
        exit_code = 1
    logger.info("Research-job projection consumer stopped (exit_code=%s)", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
