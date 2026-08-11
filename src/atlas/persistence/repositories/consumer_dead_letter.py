"""PostgreSQL-backed dead-letter storage and operator replay fencing (Slice 13C2B)."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.errors import LifecycleOrderViolationError
from atlas.consumer.ports import InboxOutcome, InboxRepository, ProjectionPort
from atlas.consumer.replay_errors import (
    ReplayAlreadyClaimedError,
    ReplayConflictError,
    ReplayExpiredAttemptError,
    ReplayNotEligibleError,
    ReplayNotFoundError,
    ReplayOwnershipLostError,
)
from atlas.consumer.retention import DeadLetterRetention
from atlas.eventing.contracts import parse_domain_event
from atlas.eventing.errors import DomainEventError
from atlas.outbox.clock import Clock, utc_now
from atlas.persistence.db import session_scope
from atlas.persistence.models.consumer import (
    ConsumerDeadLetterModel,
    ConsumerDeadLetterReplayAttemptModel,
)
from atlas.persistence.repositories.consumer_inbox import ConsumerInboxConflictError

#: The only failure kinds ``_apply`` (TX2) treats as an ordinary, sanitized
#: replay failure (routed to TX3 / :meth:`DeadLetterReplayService._record_failure`):
#: a corrupt/invalid retained event (should not happen -- defense in depth),
#: a database failure, the inbox's own concurrent-write anomaly guard, or the
#: research-job lifecycle projection's ordering-invariant guard. Anything else
#: -- a programming defect in this service or its collaborators -- must
#: propagate uncaught rather than being silently recorded as a routine
#: ``FAILED`` outcome indistinguishable from an expected business rejection.
_EXPECTED_APPLY_FAILURES: tuple[type[Exception], ...] = (
    DomainEventError,
    DBAPIError,
    ConsumerInboxConflictError,
    LifecycleOrderViolationError,
)

_TERMINAL_ATTEMPT_STATUSES = frozenset(
    {"APPLIED", "DUPLICATE", "FAILED", "LOST_OWNERSHIP"}
)
_UNCLAIMED_REPLAYABLE_STATES = frozenset({"PENDING", "REPLAY_FAILED"})


class SqlAlchemyDeadLetterRepository:
    """Implements ``DeadLetterRepository`` (the ``ConsumerRunner``'s write path)."""

    def upsert(
        self,
        session: Session,
        *,
        consumer_id: str,
        kafka_partition: int,
        kafka_offset: int,
        failure_code: str,
        processing_attempt_count: int,
        at: datetime,
        retention: DeadLetterRetention,
    ) -> UUID:
        new_id = uuid4()
        stmt = (
            pg_insert(ConsumerDeadLetterModel)
            .values(
                id=new_id,
                consumer_id=consumer_id,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                event_id=retention.event_id,
                event_type=retention.event_type,
                event_version=retention.event_version,
                aggregate_type=retention.aggregate_type,
                aggregate_id=retention.aggregate_id,
                failure_code=failure_code,
                processing_attempt_count=processing_attempt_count,
                dead_letter_delivery_count=1,
                first_failed_at=at,
                last_failed_at=at,
                payload_sha256=retention.payload_sha256,
                payload_byte_length=retention.payload_byte_length,
                retained_canonical_value=retention.retained_canonical_value,
                retained_header_event_type=retention.retained_header_event_type,
                retained_header_event_version=retention.retained_header_event_version,
                retained_header_aggregate_type=retention.retained_header_aggregate_type,
                replay_eligible=retention.replay_eligible,
                replay_state="PENDING",
                replay_claim_token=None,
                replay_lease_expires_at=None,
                created_at=at,
                updated_at=at,
            )
            .on_conflict_do_update(
                index_elements=["consumer_id", "kafka_partition", "kafka_offset"],
                set_={
                    "dead_letter_delivery_count": (
                        ConsumerDeadLetterModel.dead_letter_delivery_count + 1
                    ),
                    "last_failed_at": at,
                    "updated_at": at,
                },
            )
            .returning(ConsumerDeadLetterModel.id)
        )
        result = session.execute(stmt)
        row_id = result.scalar_one()
        return UUID(str(row_id))


class ReplayOutcome(StrEnum):
    """Terminal (or short-circuited) result of one replay command invocation."""

    IN_PROGRESS = "in_progress"
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    FAILED = "failed"
    LOST_OWNERSHIP = "lost_ownership"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Returned by ``DeadLetterReplayService.replay`` to the operator CLI."""

    dead_letter_id: UUID
    attempt_id: UUID
    outcome: ReplayOutcome


@dataclass(frozen=True, slots=True)
class _Claim:
    dead_letter_id: UUID
    attempt_id: UUID
    ownership_token: str


class DeadLetterReplayService:
    """Orchestrates TX1 (claim) / TX2 (apply) / TX3 (record failure).

    Every substantive replay-attempt field is written exactly once, at
    claim time (TX1); only ``status``/``finished_at`` are ever updated
    afterward (TX2 or TX3), keeping the audit trail append-only in spirit.
    A reclaim of an expired ``REPLAYING`` lease always creates a *new*
    attempt row and durably marks the previous token's ``IN_PROGRESS`` row
    ``LOST_OWNERSHIP`` in the same transaction -- see module docstring in
    the implementation proposal for the invariant this preserves: an
    attempt row's status is ``IN_PROGRESS`` if and only if its own
    ``ownership_token`` equals the dead-letter row's current live token.
    """

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        inbox: InboxRepository,
        projection: ProjectionPort,
        lease_seconds: float,
        clock: Clock = utc_now,
    ) -> None:
        self._session_factory = session_factory
        self._inbox = inbox
        self._projection = projection
        self._lease_seconds = lease_seconds
        self._clock = clock

    def replay(
        self,
        *,
        dead_letter_id: UUID,
        idempotency_key: str,
        actor_id: str,
        operator_reason: str,
        request_fingerprint: str,
    ) -> ReplayResult:
        """Run the full claim -> apply (-> record-failure) sequence."""
        claim_outcome = self._claim(
            dead_letter_id=dead_letter_id,
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            operator_reason=operator_reason,
            request_fingerprint=request_fingerprint,
        )
        if isinstance(claim_outcome, ReplayResult):
            return claim_outcome
        return self._apply(claim_outcome)

    # --- TX1 -------------------------------------------------------------

    def _claim(
        self,
        *,
        dead_letter_id: UUID,
        idempotency_key: str,
        actor_id: str,
        operator_reason: str,
        request_fingerprint: str,
    ) -> _Claim | ReplayResult:
        result: dict[str, Any] = {"dead_letter_id": dead_letter_id}

        # Every branch below only ever *sets keys on* ``result`` and returns
        # (never raises) so the ``with`` block always exits normally and
        # ``session.commit()`` always runs -- including for the
        # "expired_same_key" branch, which durably marks the stale attempt
        # ``LOST_OWNERSHIP`` even though the overall claim is then rejected.
        # ``_finish_claim`` is called strictly after the block below returns,
        # once any such state change has already committed, so raising the
        # corresponding typed error there can never cause ``session_scope``
        # to roll back an intentional write.
        with session_scope(self._session_factory) as session:
            row = session.get(
                ConsumerDeadLetterModel, dead_letter_id, with_for_update=True
            )
            # Read the clock only after the row lock is held: acquiring it
            # can block for an unbounded time against another transaction
            # (e.g. a concurrent TX1/TX2/TX3 holding the same row), and a
            # clock reading taken beforehand could stale-pass a lease that
            # has since expired -- or stale-fail one that has not -- while
            # this call was waiting. Every TX1 decision below (expiry,
            # eligibility, the previous attempt's ``LOST_OWNERSHIP``
            # ``finished_at``, the new attempt's ``created_at``, the new
            # lease's expiration, and the row's ``updated_at``) uses this
            # single fresh reading. Starting the new lease from a timestamp
            # taken after the row lock is acquired avoids consuming any of
            # the lease's duration while waiting for that lock -- it does
            # not guarantee the lease cannot expire before commit: an
            # exceptionally delayed transaction could still consume lease
            # time between this reading and commit. TX2/TX3 do not rely on
            # this reading alone -- each independently re-reads the clock
            # after its own row lock and fails closed if the lease has by
            # then expired.
            now = self._clock()
            if row is None:
                result["kind"] = "not_found"
            else:
                existing_attempt = session.execute(
                    select(ConsumerDeadLetterReplayAttemptModel).where(
                        ConsumerDeadLetterReplayAttemptModel.dead_letter_id
                        == dead_letter_id,
                        ConsumerDeadLetterReplayAttemptModel.idempotency_key
                        == idempotency_key,
                    )
                ).scalar_one_or_none()

                lease_expired = (
                    row.replay_state == "REPLAYING"
                    and row.replay_claim_token is not None
                    and row.replay_lease_expires_at is not None
                    and row.replay_lease_expires_at < now
                )
                eligible = row.replay_eligible and (
                    (row.replay_state in _UNCLAIMED_REPLAYABLE_STATES) or lease_expired
                )

                if existing_attempt is not None:
                    if existing_attempt.request_fingerprint != request_fingerprint:
                        result["kind"] = "conflict"
                    elif existing_attempt.status != "IN_PROGRESS":
                        result["kind"] = "terminal"
                        result["attempt_id"] = existing_attempt.id
                        result["status"] = existing_attempt.status
                    elif not eligible:
                        # A live claim under this same key -> report it
                        # unchanged.
                        result["kind"] = "in_progress"
                        result["attempt_id"] = existing_attempt.id
                    else:
                        # Eligible while this key's own attempt is still
                        # IN_PROGRESS means its lease expired with nobody
                        # else having reclaimed it yet. Chosen behavior: do
                        # not resume in place -- mark it terminal and
                        # require a fresh idempotency key (see module
                        # docstring / implementation proposal for the
                        # rationale).
                        existing_attempt.status = "LOST_OWNERSHIP"
                        existing_attempt.finished_at = now
                        result["kind"] = "expired_same_key"
                elif not eligible:
                    result["kind"] = (
                        "already_claimed"
                        if row.replay_state == "REPLAYING"
                        else "not_eligible"
                    )
                else:
                    previous_token = row.replay_claim_token
                    if previous_token is not None:
                        session.execute(
                            update(ConsumerDeadLetterReplayAttemptModel)
                            .where(
                                ConsumerDeadLetterReplayAttemptModel.dead_letter_id
                                == dead_letter_id,
                                ConsumerDeadLetterReplayAttemptModel.ownership_token
                                == previous_token,
                                ConsumerDeadLetterReplayAttemptModel.status
                                == "IN_PROGRESS",
                            )
                            .values(status="LOST_OWNERSHIP", finished_at=now)
                        )

                    new_token = secrets.token_hex(32)
                    attempt_id = uuid4()
                    session.add(
                        ConsumerDeadLetterReplayAttemptModel(
                            id=attempt_id,
                            dead_letter_id=dead_letter_id,
                            idempotency_key=idempotency_key,
                            actor_id=actor_id,
                            operator_reason=operator_reason,
                            request_fingerprint=request_fingerprint,
                            ownership_token=new_token,
                            status="IN_PROGRESS",
                            created_at=now,
                            finished_at=None,
                        )
                    )
                    row.replay_claim_token = new_token
                    row.replay_lease_expires_at = now + timedelta(
                        seconds=self._lease_seconds
                    )
                    row.replay_state = "REPLAYING"
                    row.updated_at = now
                    session.flush()
                    result["kind"] = "claimed"
                    result["attempt_id"] = attempt_id
                    result["token"] = new_token

        return self._finish_claim(result)

    @staticmethod
    def _finish_claim(result: dict[str, Any]) -> _Claim | ReplayResult:
        """Translate the recorded TX1 outcome into a typed result, post-commit.

        Called from ``_claim`` strictly *after* its ``with session_scope``
        block has already exited (and therefore already committed) -- never
        from inside that block, which would cause ``session_scope`` to roll
        back an intentional state change -- e.g. the ``expired_same_key`` ->
        ``LOST_OWNERSHIP`` write -- if this raised there instead.
        """
        kind = result["kind"]
        if kind == "not_found":
            raise ReplayNotFoundError()
        if kind == "conflict":
            raise ReplayConflictError()
        if kind == "not_eligible":
            raise ReplayNotEligibleError()
        if kind == "already_claimed":
            raise ReplayAlreadyClaimedError()
        if kind == "expired_same_key":
            raise ReplayExpiredAttemptError()
        if kind == "terminal":
            return ReplayResult(
                dead_letter_id=result["dead_letter_id"],
                attempt_id=result["attempt_id"],
                outcome=ReplayOutcome(result["status"].lower()),
            )
        if kind == "in_progress":
            return ReplayResult(
                dead_letter_id=result["dead_letter_id"],
                attempt_id=result["attempt_id"],
                outcome=ReplayOutcome.IN_PROGRESS,
            )
        return _Claim(
            dead_letter_id=result["dead_letter_id"],
            attempt_id=result["attempt_id"],
            ownership_token=result["token"],
        )

    # --- TX2 / TX3 ---------------------------------------------------------

    def _apply(self, claim: _Claim) -> ReplayResult:
        """TX2: apply the replay's effect, or fall through to TX3 on expected failure.

        Only ``_EXPECTED_APPLY_FAILURES`` are routed to
        :meth:`_record_failure` (an ordinary, sanitized ``FAILED`` outcome).
        Any other exception -- a programming defect in this service or one
        of its collaborators -- propagates uncaught, leaving the replay
        attempt row ``IN_PROGRESS`` (this transaction rolls back) rather
        than misrecording it as a routine business failure.
        """
        outcome: dict[str, Any] = {}
        try:
            with session_scope(self._session_factory) as session:
                row = session.get(
                    ConsumerDeadLetterModel, claim.dead_letter_id, with_for_update=True
                )
                attempt = session.get(
                    ConsumerDeadLetterReplayAttemptModel,
                    claim.attempt_id,
                    with_for_update=True,
                )
                # Read the clock only after both row locks are held: lock
                # acquisition can block for an unbounded time (contending
                # with another transaction holding the same row), and a
                # clock reading taken beforehand could stale-pass a lease
                # that has since expired while this call was waiting.
                now = self._clock()
                owned = (
                    row is not None
                    and row.replay_state == "REPLAYING"
                    and row.replay_claim_token == claim.ownership_token
                    and row.replay_lease_expires_at is not None
                    and row.replay_lease_expires_at >= now
                    and attempt is not None
                    and attempt.status == "IN_PROGRESS"
                )
                if not owned:
                    # An expired (or otherwise no-longer-owned) claim must
                    # never mark FAILED, clear the dead-letter row's claim/
                    # lease, or apply any projection/inbox effect -- only
                    # this attempt's own row is affected, and only if it is
                    # still IN_PROGRESS (a concurrent reclaim in TX1 may
                    # already have flipped it to LOST_OWNERSHIP itself).
                    if attempt is not None and attempt.status == "IN_PROGRESS":
                        attempt.status = "LOST_OWNERSHIP"
                        attempt.finished_at = now
                    outcome["kind"] = "ownership_lost"
                else:
                    assert row is not None
                    assert attempt is not None
                    assert row.retained_canonical_value is not None
                    event = parse_domain_event(json.loads(row.retained_canonical_value))
                    inbox_outcome = self._inbox.record_and_apply(
                        session,
                        consumer_id=row.consumer_id,
                        event=event,
                        kafka_partition=row.kafka_partition,
                        kafka_offset=row.kafka_offset,
                        at=now,
                        apply_effect=lambda s, e: self._projection.apply(s, e, at=now),
                    )
                    # Reuses the same already-locked ``attempt`` row fetched
                    # above (still valid, unexpired ownership was already
                    # confirmed for it) rather than re-fetching -- a second
                    # ``SELECT ... FOR UPDATE`` on the same row within one
                    # transaction is redundant, not a race concern, but
                    # avoiding it keeps this one lock acquisition, one clock
                    # reading, one ownership decision per attempt.
                    final_status = (
                        "APPLIED"
                        if inbox_outcome is InboxOutcome.APPLIED
                        else "DUPLICATE"
                    )
                    attempt.status = final_status
                    attempt.finished_at = now
                    row.replay_state = (
                        "REPLAYED_APPLIED"
                        if inbox_outcome is InboxOutcome.APPLIED
                        else "REPLAYED_DUPLICATE"
                    )
                    row.replay_claim_token = None
                    row.replay_lease_expires_at = None
                    row.updated_at = now
                    outcome["kind"] = "success"
                    outcome["status"] = final_status
        except _EXPECTED_APPLY_FAILURES:
            return self._record_failure(claim)

        if outcome["kind"] == "ownership_lost":
            raise ReplayOwnershipLostError()
        return ReplayResult(
            dead_letter_id=claim.dead_letter_id,
            attempt_id=claim.attempt_id,
            outcome=ReplayOutcome(outcome["status"].lower()),
        )

    def _record_failure(self, claim: _Claim) -> ReplayResult:
        """TX3: a fresh, independent transaction after TX2 rolled back.

        Applies the exact same unexpired-lease ownership requirement as TX2
        (:meth:`_apply`), using a clock reading taken only after both row
        locks are held. An expired owner (lease elapsed, even if no new
        claimant has reclaimed it yet) must never mark this attempt FAILED
        or clear the dead-letter row's claim/lease/state -- that would
        incorrectly resurrect a claim a future reclaimer is entitled to
        take over, and could race destructively with that reclaim's own
        TX1 write. It falls through to the same ``lost_ownership`` handling
        an actual token mismatch would produce.
        """
        outcome: dict[str, Any] = {}
        with session_scope(self._session_factory) as session:
            row = session.get(
                ConsumerDeadLetterModel, claim.dead_letter_id, with_for_update=True
            )
            attempt = session.get(
                ConsumerDeadLetterReplayAttemptModel,
                claim.attempt_id,
                with_for_update=True,
            )
            now = self._clock()
            still_owned = (
                row is not None
                and row.replay_state == "REPLAYING"
                and row.replay_claim_token == claim.ownership_token
                and row.replay_lease_expires_at is not None
                and row.replay_lease_expires_at >= now
                and attempt is not None
                and attempt.status == "IN_PROGRESS"
            )
            if still_owned:
                assert row is not None
                assert attempt is not None
                attempt.status = "FAILED"
                attempt.finished_at = now
                row.replay_state = "REPLAY_FAILED"
                row.replay_claim_token = None
                row.replay_lease_expires_at = None
                row.updated_at = now
                outcome["kind"] = "failed"
            else:
                if attempt is not None and attempt.status == "IN_PROGRESS":
                    attempt.status = "LOST_OWNERSHIP"
                    attempt.finished_at = now
                outcome["kind"] = "lost_ownership"

        if outcome["kind"] == "lost_ownership":
            raise ReplayOwnershipLostError()
        return ReplayResult(
            dead_letter_id=claim.dead_letter_id,
            attempt_id=claim.attempt_id,
            outcome=ReplayOutcome.FAILED,
        )
