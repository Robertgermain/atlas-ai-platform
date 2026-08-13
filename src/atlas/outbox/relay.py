"""Outbox relay orchestration behind a typed ``EventProducer`` port.

Delivery guarantee is at-least-once. A crash after producer acknowledgment but
before ``mark_published`` re-publishes the same ``event_id`` after lease expiry.
Consumer inbox deduplication remains mandatory in Slice 13C2.

Publication order is strict by ascending ``outbox_position``. Within a claimed
batch, a producer failure or lost mark ownership stops later rows: they are not
published and owned claims are released so a later run can resume in order.

``publish_attempts`` counts claim attempts (incremented when a row is claimed
for publication), not successful producer I/O calls.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from uuid import UUID

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from sqlalchemy.orm import Session, sessionmaker

from atlas.observability.events import Event
from atlas.observability.logging import log_event
from atlas.observability.tracing import current_traceparent, resolve_parent_or_link
from atlas.outbox.clock import Clock, utc_now
from atlas.outbox.errors import (
    EventPublishError,
    FatalEventPublishError,
    RelayNotOwnerError,
)
from atlas.outbox.ports import ClaimedOutboxRecord, EventProducer, OutboxRepository
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.persistence.db import session_scope

logger = logging.getLogger(__name__)
_tracer = trace.get_tracer(__name__)

DEFAULT_OUTBOX_BATCH_SIZE = 50

# Sanitized error classes persisted for non-producer stop reasons (never raw text).
_EARLIER_PUBLISH_FAILURE = "EarlierEventPublishFailure"
_EARLIER_OWNERSHIP_LOST = "EarlierEventOwnershipLost"


class RelayRunOutcome(StrEnum):
    """Distinguishes why a ``run_once()`` batch ended (Slice 13C1).

    ``FATAL_FAILURE`` and ``UNEXPECTED_FAILURE`` are the only outcomes that
    must cause the caller (the Kafka relay executable) to stop the poll loop
    and terminate nonzero; the row's claim has already been safely released
    before either is returned.
    """

    EMPTY = "empty"
    PUBLISHED = "published"
    RECOVERABLE_FAILURE = "recoverable_failure"
    FATAL_FAILURE = "fatal_failure"
    OWNERSHIP_LOST = "ownership_lost"
    # A producer raised something other than the typed EventPublishError /
    # FatalEventPublishError hierarchy -- e.g. a programming error. Never
    # treated as recoverable: Atlas has no basis to assume a retry is safe.
    UNEXPECTED_FAILURE = "unexpected_failure"


@dataclass(frozen=True, slots=True)
class RelayBatchResult:
    """Outcome of one ``run_once()`` call."""

    outcome: RelayRunOutcome
    published_count: int


class OutboxRelay:
    """Claim → publish outside TX → conditionally mark or release.

    Mark/release fencing always uses a fresh clock reading taken *after* the
    producer call so a slow publish cannot appear to still own an expired lease.
    """

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: OutboxRepository,
        producer: EventProducer,
        lock: PostgresOutboxRelayLock,
        batch_size: int = DEFAULT_OUTBOX_BATCH_SIZE,
        publish_lease_seconds: float = 30.0,
        clock: Clock | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if publish_lease_seconds <= 0:
            raise ValueError("publish_lease_seconds must be positive")
        self._session_factory = session_factory
        self._repository = repository
        self._producer = producer
        self._lock = lock
        self._batch_size = batch_size
        self._publish_lease_seconds = publish_lease_seconds
        self._clock: Clock = clock or utc_now

    def run_once(self) -> RelayBatchResult:
        """Process at most one claimable batch in ``outbox_position`` order.

        Returns a :class:`RelayBatchResult` describing how many rows were
        marked published and why the batch ended.
        """
        if not self._lock.held:
            raise RelayNotOwnerError(
                "Outbox relay requires the singleton advisory lock."
            )
        claim_at = self._clock()
        claim_token = secrets.token_hex(32)
        lease_expires_at = claim_at + timedelta(seconds=self._publish_lease_seconds)

        with session_scope(self._session_factory) as session:
            claimed = self._repository.claim_batch(
                session,
                claimant_token=claim_token,
                at=claim_at,
                lease_expires_at=lease_expires_at,
                batch_size=self._batch_size,
            )
        # Claim transaction is committed before any producer I/O.

        if not claimed:
            return RelayBatchResult(outcome=RelayRunOutcome.EMPTY, published_count=0)

        published = 0
        for index, record in enumerate(claimed):
            failure_outcome: RelayRunOutcome | None = None
            error_class: str | None = None
            # Slice 15A3: the row's own stored traceparent (captured
            # atomically at enqueue, in the same transaction as the
            # business insert -- see SqlAlchemyOutboxRepository.enqueue) is
            # always used as a direct parent here, never a Span Link --
            # unlike the worker's claim-vs-reclaim ambiguity, an outbox
            # row's lineage from its own enqueue transaction is never
            # ambiguous. A missing/malformed stored value simply yields no
            # parent (an ordinary new root span), never a failure.
            parent_context, _links = resolve_parent_or_link(
                record.traceparent, use_as_parent=True
            )
            with _tracer.start_as_current_span(
                "outbox.publish",
                context=parent_context,
                attributes={"atlas.outbox_event_id": str(record.event_id)},
            ) as publish_span:
                outgoing_traceparent = current_traceparent()
                try:
                    self._producer.publish(
                        record.event, traceparent=outgoing_traceparent
                    )
                except FatalEventPublishError as exc:
                    failure_outcome = RelayRunOutcome.FATAL_FAILURE
                    error_class = exc.__class__.__name__
                except EventPublishError as exc:
                    failure_outcome = RelayRunOutcome.RECOVERABLE_FAILURE
                    error_class = exc.__class__.__name__
                except Exception as exc:
                    # Not a typed publish error: a programming error or some
                    # other unexpected failure. Never assume this is safe to
                    # retry -- release what we can, using only the safe class
                    # name, then stop and let the caller terminate nonzero.
                    failure_outcome = RelayRunOutcome.UNEXPECTED_FAILURE
                    error_class = exc.__class__.__name__
                if failure_outcome is not None:
                    publish_span.set_status(Status(StatusCode.ERROR))
                    assert error_class is not None
                    publish_span.set_attribute("error.class", error_class)

            if failure_outcome is not None:
                assert error_class is not None
                finalize_at = self._clock()
                self._release_owned(
                    event_id=record.event_id,
                    claimant_token=claim_token,
                    at=finalize_at,
                    error_class=error_class,
                )
                self._release_remaining(
                    claimed[index + 1 :],
                    claimant_token=claim_token,
                    error_class=_EARLIER_PUBLISH_FAILURE,
                )
                return RelayBatchResult(
                    outcome=failure_outcome, published_count=published
                )

            finalize_at = self._clock()
            with session_scope(self._session_factory) as session:
                marked = self._repository.mark_published(
                    session,
                    event_id=record.event_id,
                    claimant_token=claim_token,
                    at=finalize_at,
                )
            if marked:
                published += 1
                continue

            # Ownership lost (lease expired / reclaimed). Do not overwrite the
            # new owner. Stop so later positions cannot leapfrog.
            log_event(
                logger,
                Event.OUTBOX_OWNERSHIP_LOST,
                level=logging.WARNING,
                outbox_event_id=str(record.event_id),
                outcome="mark_published",
            )
            self._release_remaining(
                claimed[index + 1 :],
                claimant_token=claim_token,
                error_class=_EARLIER_OWNERSHIP_LOST,
            )
            return RelayBatchResult(
                outcome=RelayRunOutcome.OWNERSHIP_LOST, published_count=published
            )
        return RelayBatchResult(
            outcome=RelayRunOutcome.PUBLISHED, published_count=published
        )

    def _release_owned(
        self,
        *,
        event_id: UUID,
        claimant_token: str,
        at: object,
        error_class: str,
    ) -> None:
        from datetime import datetime

        assert isinstance(at, datetime)
        with session_scope(self._session_factory) as session:
            released = self._repository.release_failed_claim(
                session,
                event_id=event_id,
                claimant_token=claimant_token,
                at=at,
                error_class=error_class,
            )
        if not released:
            log_event(
                logger,
                Event.OUTBOX_OWNERSHIP_LOST,
                level=logging.WARNING,
                outbox_event_id=str(event_id),
                outcome="release_failed_claim",
            )

    def _release_remaining(
        self,
        records: Sequence[ClaimedOutboxRecord],
        *,
        claimant_token: str,
        error_class: str,
    ) -> None:
        """Release later owned claims without publishing them."""
        for record in records:
            finalize_at = self._clock()
            self._release_owned(
                event_id=record.event_id,
                claimant_token=claimant_token,
                at=finalize_at,
                error_class=error_class,
            )
