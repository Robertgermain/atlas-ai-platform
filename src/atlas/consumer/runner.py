"""Poll-decode-apply-commit orchestration with bounded retry and DLQ (Slice 13C2B).

Scope boundary for what is and is not retried: only the PostgreSQL-side
"process one already-in-hand record" work (the normal apply loop, and --
on a permanent-poison classification -- the dead-letter upsert), plus the
final ``commit_message()`` call itself, is bounded and retried here.
``poll()`` is never called during a record's retry episode (the processing
deadline exists precisely to keep the whole episode well inside
``consumer_max_poll_interval_seconds`` without it). A ``commit_message()``
failure is retried using the exact same bounded attempts/backoff/deadline
machinery only when ``KafkaEventConsumer`` classifies it
``TransientKafkaError``; any other failure there terminates immediately
(see ``OffsetCommitFailedAfterDeadLetterError`` and the fatal
``ConsumerError`` "CommitFailed" classification), because redelivery after
restart safely reuses the durable inbox/dead-letter rows either way.

Every database retry decision is made by ``atlas.consumer.db_classify.
classify_database_error`` against the *raw* SQLAlchemy/psycopg exception,
and every Kafka retry decision is made by ``KafkaEventConsumer`` against the
raw ``confluent_kafka.KafkaError`` -- never by inspecting an exception's
string message or class name.

Retry-backoff waits use an injectable, shutdown-aware ``Waiter`` (see
``atlas.consumer.wait``) rather than a bare sleep: shutdown observed during
a wait raises ``ConsumerShutdownRequestedError`` so ``python -m
atlas.consumer`` can stop cleanly, with the offset uncommitted, well before
the whole retry episode would otherwise finish.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from confluent_kafka import Message
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.db_classify import DatabaseErrorClass, classify_database_error
from atlas.consumer.deserialize import decode_message
from atlas.consumer.errors import (
    TIER_A_ELIGIBLE_FAILURE_CODES,
    ConsumerError,
    ConsumerShutdownRequestedError,
    DeadLetterPersistenceExhaustedError,
    OffsetCommitFailedAfterDeadLetterError,
    PoisonEventError,
    ProcessingDeadlineExceededError,
    RetryExhaustedError,
    TransientKafkaError,
    failure_code_for,
)
from atlas.consumer.ports import (
    DeadLetterRepository,
    InboxOutcome,
    InboxRepository,
    ProjectionPort,
)
from atlas.consumer.retention import build_retention
from atlas.consumer.timing import (
    ProcessingDeadline,
    RetryTimingParameters,
    backoff_delay_seconds,
)
from atlas.consumer.wait import Waiter, build_shutdown_aware_waiter
from atlas.eventing.contracts import DomainEvent
from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.outbox.clock import Clock, utc_now
from atlas.outbox.kafka_errors import KafkaErrorClass, classify_kafka_error
from atlas.persistence.db import session_scope


class ProcessOutcome(StrEnum):
    """Result of one ``ConsumerRunner.run_once()`` call."""

    NO_MESSAGE = "no_message"
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    DEAD_LETTERED = "dead_lettered"


class _ConsumerLike(Protocol):
    """The ``KafkaEventConsumer`` surface this orchestrator depends on.

    Typed against ``confluent_kafka.Message`` (not a duck-typed
    stand-in) so ``KafkaEventConsumer`` satisfies this structurally without
    a cast. Unit tests inject a fake and accept a ``# type: ignore[arg-type]``
    at the injection site instead of ever constructing a real ``Message``.
    """

    def poll(self, timeout_seconds: float) -> Message | None: ...
    def commit_message(self, message: Message) -> None: ...


def _require_int(value: int | None, *, context: str) -> int:
    """Narrow an optional Kafka position field, failing closed if absent.

    ``confluent_kafka``'s stub types ``Message.partition()``/``.offset()``
    as ``Optional[int]`` to cover error-carrying messages; this runner
    already rejects those via ``message.error() is not None`` before this
    is called, so ``None`` here would indicate an unexpected message shape
    rather than an ordinary condition.
    """
    if value is None:
        raise ConsumerError(context)
    return value


class ConsumerRunner:
    """Drives exactly one poll-decode-apply-commit cycle per ``run_once()`` call.

    Offset acknowledgment happens strictly after either (a) the PostgreSQL
    transaction (inbox record + business effect) commits, or (b) a
    permanent-poison classification's dead-letter row durably commits.
    Transient-infrastructure exhaustion and any fatal classification both
    propagate out of ``run_once()`` with the Kafka offset never committed.
    """

    def __init__(
        self,
        *,
        consumer: _ConsumerLike,
        session_factory: sessionmaker[Session],
        inbox: InboxRepository,
        projection: ProjectionPort,
        dead_letters: DeadLetterRepository,
        consumer_id: str,
        poll_timeout_seconds: float,
        max_poll_interval_seconds: float = 300.0,
        timing_params: RetryTimingParameters | None = None,
        clock: Clock = utc_now,
        wait: Waiter | None = None,
        metrics: AtlasMetrics | None = None,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._inbox = inbox
        self._projection = projection
        self._dead_letters = dead_letters
        self._consumer_id = consumer_id
        self._poll_timeout_seconds = poll_timeout_seconds
        self._max_poll_interval_seconds = max_poll_interval_seconds
        self._metrics = metrics or default_metrics()
        self._timing_params = timing_params or RetryTimingParameters(
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
        self._clock = clock
        self._wait = wait or build_shutdown_aware_waiter(lambda: False)

    def run_once(self) -> ProcessOutcome:
        message = self._consumer.poll(self._poll_timeout_seconds)
        if message is None:
            return ProcessOutcome.NO_MESSAGE
        error = message.error()
        if error is not None:
            # Defense in depth: production ``KafkaEventConsumer.poll()``
            # already classifies and raises before ever returning an
            # error-carrying ``Message`` (see its docstring), so this only
            # fires for a test double or an unexpected future adapter that
            # does not uphold that contract. Classified the same way, never
            # by message/class-name text.
            if classify_kafka_error(error) is KafkaErrorClass.RECOVERABLE:
                raise TransientKafkaError("PollReturnedBrokerError")
            raise ConsumerError("PollReturnedBrokerError")

        at = self._clock()
        partition = _require_int(message.partition(), context="MissingPartition")
        offset = _require_int(message.offset(), context="MissingOffset")
        deadline = ProcessingDeadline(
            params=self._timing_params,
            max_poll_interval_seconds=self._max_poll_interval_seconds,
            message_received_at=at,
        )

        try:
            event = decode_message(message)
        except PoisonEventError as exc:
            return self._dead_letter_and_commit(
                exc,
                message=message,
                event=None,
                partition=partition,
                offset=offset,
                at=at,
                processing_attempt_count=1,
                deadline=deadline,
            )

        return self._apply_with_retry(
            message=message,
            event=event,
            partition=partition,
            offset=offset,
            at=at,
            deadline=deadline,
        )

    def _apply_with_retry(
        self,
        *,
        message: Message,
        event: DomainEvent,
        partition: int,
        offset: int,
        at: datetime,
        deadline: ProcessingDeadline,
    ) -> ProcessOutcome:
        def _apply_effect(session: Session, decoded_event: DomainEvent) -> None:
            self._projection.apply(session, decoded_event, at=at)

        attempt_index = 0
        while True:
            attempt_index += 1
            if not deadline.can_start_attempt(now=self._clock()):
                raise ProcessingDeadlineExceededError()
            try:
                with session_scope(self._session_factory) as session:
                    outcome = self._inbox.record_and_apply(
                        session,
                        consumer_id=self._consumer_id,
                        event=event,
                        kafka_partition=partition,
                        kafka_offset=offset,
                        at=at,
                        apply_effect=_apply_effect,
                    )
            except PoisonEventError as exc:
                return self._dead_letter_and_commit(
                    exc,
                    message=message,
                    event=event,
                    partition=partition,
                    offset=offset,
                    at=at,
                    processing_attempt_count=attempt_index,
                    deadline=deadline,
                )
            except Exception as exc:
                if classify_database_error(exc) is not DatabaseErrorClass.TRANSIENT:
                    raise
                if attempt_index >= self._timing_params.max_attempts:
                    raise RetryExhaustedError(exc.__class__.__name__) from exc
                backoff = backoff_delay_seconds(
                    self._timing_params, attempt_index=attempt_index - 1
                )
                if not deadline.can_afford_backoff(
                    now=self._clock(), backoff_seconds=backoff
                ):
                    raise ProcessingDeadlineExceededError() from exc
                self._metrics.observe_consumer_retry_attempt(stage="apply")
                if self._wait(backoff):
                    raise ConsumerShutdownRequestedError() from exc
                continue

            self._commit_with_retry(message, deadline=deadline)
            return (
                ProcessOutcome.APPLIED
                if outcome is InboxOutcome.APPLIED
                else ProcessOutcome.DUPLICATE
            )

    def _commit_with_retry(
        self, message: Message, *, deadline: ProcessingDeadline
    ) -> None:
        """Commit the offset, retrying only an evidence-backed transient failure.

        Uses the same bounded attempts/backoff/deadline machinery as the
        apply loop. Any other failure (including exhaustion) propagates
        immediately and uncommitted -- never dead-lettered, since the
        business effect already durably committed.
        """
        attempt_index = 0
        while True:
            attempt_index += 1
            if not deadline.can_start_attempt(now=self._clock()):
                self._metrics.observe_consumer_offset_commit(
                    outcome="deadline_exceeded"
                )
                raise ProcessingDeadlineExceededError()
            try:
                self._consumer.commit_message(message)
                self._metrics.observe_consumer_offset_commit(outcome="success")
                return
            except TransientKafkaError:
                if attempt_index >= self._timing_params.max_attempts:
                    self._metrics.observe_consumer_offset_commit(outcome="failure")
                    raise
                backoff = backoff_delay_seconds(
                    self._timing_params, attempt_index=attempt_index - 1
                )
                if not deadline.can_afford_backoff(
                    now=self._clock(), backoff_seconds=backoff
                ):
                    self._metrics.observe_consumer_offset_commit(
                        outcome="deadline_exceeded"
                    )
                    raise ProcessingDeadlineExceededError() from None
                self._metrics.observe_consumer_retry_attempt(stage="commit")
                if self._wait(backoff):
                    self._metrics.observe_consumer_offset_commit(
                        outcome="shutdown_requested"
                    )
                    raise ConsumerShutdownRequestedError() from None
                continue

    def _dead_letter_and_commit(
        self,
        exc: PoisonEventError,
        *,
        message: Message,
        event: DomainEvent | None,
        partition: int,
        offset: int,
        at: datetime,
        processing_attempt_count: int,
        deadline: ProcessingDeadline,
    ) -> ProcessOutcome:
        failure_code = failure_code_for(exc)
        retention = build_retention(
            failure_code=failure_code,
            raw_value=message.value(),
            decoded_event=event
            if failure_code in TIER_A_ELIGIBLE_FAILURE_CODES
            else None,
        )

        attempt_index = 0
        while True:
            attempt_index += 1
            if not deadline.can_start_attempt(now=self._clock()):
                raise ProcessingDeadlineExceededError()
            try:
                with session_scope(self._session_factory) as session:
                    self._dead_letters.upsert(
                        session,
                        consumer_id=self._consumer_id,
                        kafka_partition=partition,
                        kafka_offset=offset,
                        failure_code=failure_code,
                        processing_attempt_count=processing_attempt_count,
                        at=at,
                        retention=retention,
                    )
                break
            except Exception as db_exc:
                if classify_database_error(db_exc) is not DatabaseErrorClass.TRANSIENT:
                    raise DeadLetterPersistenceExhaustedError(
                        db_exc.__class__.__name__
                    ) from db_exc
                if attempt_index >= self._timing_params.max_attempts:
                    raise DeadLetterPersistenceExhaustedError(
                        db_exc.__class__.__name__
                    ) from db_exc
                backoff = backoff_delay_seconds(
                    self._timing_params, attempt_index=attempt_index - 1
                )
                if not deadline.can_afford_backoff(
                    now=self._clock(), backoff_seconds=backoff
                ):
                    raise ProcessingDeadlineExceededError() from db_exc
                self._metrics.observe_consumer_retry_attempt(stage="dead_letter_upsert")
                if self._wait(backoff):
                    raise ConsumerShutdownRequestedError() from db_exc
                continue

        # Emitted only after the dead-letter upsert above has durably
        # committed (the ``while`` loop above only exits via ``break`` once
        # ``session_scope`` has committed, or by raising) -- a redelivered
        # poison record after restart reuses the same row via the upsert's
        # own uniqueness boundary rather than creating a second one, but is
        # still counted again here since it is still a genuine dead-letter
        # occurrence from this consumer's perspective (Slice 15A2).
        self._metrics.observe_consumer_dead_letter(failure_code=failure_code)

        try:
            self._commit_with_retry(message, deadline=deadline)
        except ConsumerShutdownRequestedError:
            raise
        except ProcessingDeadlineExceededError:
            raise
        except Exception as commit_exc:
            raise OffsetCommitFailedAfterDeadLetterError(
                commit_exc.__class__.__name__
            ) from commit_exc
        return ProcessOutcome.DEAD_LETTERED


#: Type alias documenting the shutdown predicate ``python -m atlas.consumer`` polls.
ShutdownRequested = Callable[[], bool]
