"""Outbox relay orchestration with a typed producer port (no Kafka yet).

Delivery guarantee is at-least-once. A crash after producer acknowledgment but
before ``mark_published`` re-publishes the same ``event_id`` after lease expiry.
Consumer inbox deduplication remains mandatory in Slice 13C.

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
from datetime import timedelta
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from atlas.outbox.clock import Clock, utc_now
from atlas.outbox.errors import RelayNotOwnerError
from atlas.outbox.ports import ClaimedOutboxRecord, EventProducer, OutboxRepository
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.persistence.db import session_scope

logger = logging.getLogger(__name__)

DEFAULT_OUTBOX_BATCH_SIZE = 50

# Sanitized error classes persisted for non-producer stop reasons (never raw text).
_EARLIER_PUBLISH_FAILURE = "EarlierEventPublishFailure"
_EARLIER_OWNERSHIP_LOST = "EarlierEventOwnershipLost"


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

    def run_once(self) -> int:
        """Process at most one claimable batch in ``outbox_position`` order.

        Returns the number of rows successfully marked published in this run.
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

        published = 0
        for index, record in enumerate(claimed):
            try:
                self._producer.publish(record.event)
            except Exception as exc:
                finalize_at = self._clock()
                self._release_owned(
                    event_id=record.event_id,
                    claimant_token=claim_token,
                    at=finalize_at,
                    error_class=exc.__class__.__name__,
                )
                self._release_remaining(
                    claimed[index + 1 :],
                    claimant_token=claim_token,
                    error_class=_EARLIER_PUBLISH_FAILURE,
                )
                break

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
            logger.warning(
                "Outbox mark_published lost ownership for event %s; "
                "stopping batch to preserve outbox_position order.",
                record.event_id,
            )
            self._release_remaining(
                claimed[index + 1 :],
                claimant_token=claim_token,
                error_class=_EARLIER_OWNERSHIP_LOST,
            )
            break
        return published

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
            logger.warning(
                "Outbox release_failed_claim lost ownership for event %s.",
                event_id,
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
