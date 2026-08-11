"""Real-PostgreSQL tests for DLQ persistence and operator replay fencing (Slice 13C2B).

Covers ``SqlAlchemyDeadLetterRepository.upsert`` (insert + redelivery-count
conflict path) and every ``DeadLetterReplayService`` TX1/TX2/TX3 branch:
fresh claim, idempotent re-request, conflicting idempotency key, expired
same-key attempt, fresh-key reclaim of an expired lease, a live claim held
by someone else, ineligible (Tier-B) records, success/duplicate outcomes,
a stale owner prevented from applying, and TX3 failure recording.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.errors import LifecycleOrderViolationError
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.replay_errors import (
    ReplayAlreadyClaimedError,
    ReplayConflictError,
    ReplayExpiredAttemptError,
    ReplayNotEligibleError,
    ReplayNotFoundError,
    ReplayOwnershipLostError,
)
from atlas.consumer.retention import build_retention
from atlas.eventing import build_research_job_created
from atlas.persistence.db import session_scope
from atlas.persistence.models.consumer import (
    ConsumerDeadLetterModel,
    ConsumerDeadLetterReplayAttemptModel,
)
from atlas.persistence.repositories.consumer_dead_letter import (
    DeadLetterReplayService,
    ReplayOutcome,
    SqlAlchemyDeadLetterRepository,
    _Claim,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1


def _fp(seed: str) -> str:
    """A deterministic, distinguishable, 64-char lowercase-hex fingerprint.

    Migration ``20260809_0013`` requires ``request_fingerprint`` to match
    ``^[0-9a-f]{64}$`` (see ``ck_consumer_dead_letter_replay_attempts_
    fingerprint_format``) -- these tests only need distinct values per
    call site, not any specific hash semantics.
    """
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _unique_offset() -> int:
    """A large, effectively-unique BigInteger offset so tests never collide."""
    return uuid4().int & 0x7FFFFFFFFFFF


def _seed_replayable_dead_letter(
    session_factory: sessionmaker[Session],
    *,
    at: datetime = T0,
) -> tuple[str, int]:
    """Insert one Tier-A (``replay_eligible=true``) dead-letter row.

    Returns ``(job_id, kafka_offset)`` identifying the seeded row (partition
    is always 0).
    """
    research_job_id = f"replay-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=at, event_id=uuid4()
    )
    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=b'{"irrelevant": true}',
        decoded_event=event,
    )
    offset = _unique_offset()
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="lifecycle_order_violation",
            processing_attempt_count=1,
            at=at,
            retention=retention,
        )
    return research_job_id, offset


def _dead_letter_id(
    session_factory: sessionmaker[Session], *, kafka_offset: int
) -> UUID:
    with session_scope(session_factory) as session:
        row = session.execute(
            select(ConsumerDeadLetterModel).where(
                ConsumerDeadLetterModel.consumer_id == _CONSUMER_ID,
                ConsumerDeadLetterModel.kafka_partition == 0,
                ConsumerDeadLetterModel.kafka_offset == kafka_offset,
            )
        ).scalar_one()
        return row.id


def _service(
    session_factory: sessionmaker[Session],
    *,
    clock: object = lambda: T0,
    lease_seconds: float = 90.0,
) -> DeadLetterReplayService:
    return DeadLetterReplayService(
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=SqlAlchemyResearchJobProjectionRepository(),
        lease_seconds=lease_seconds,
        clock=clock,  # type: ignore[arg-type]
    )


# --- DLQ upsert: insert + redelivery counters ------------------------------


def test_upsert_inserts_a_new_row_with_delivery_count_one(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    with session_scope(session_factory) as session:
        row = session.execute(
            select(ConsumerDeadLetterModel).where(
                ConsumerDeadLetterModel.consumer_id == _CONSUMER_ID,
                ConsumerDeadLetterModel.kafka_partition == 0,
                ConsumerDeadLetterModel.kafka_offset == offset,
            )
        ).scalar_one()
        assert row.dead_letter_delivery_count == 1
        assert row.processing_attempt_count == 1
        assert row.replay_state == "PENDING"
        assert row.replay_eligible is True


def test_upsert_conflict_increments_only_delivery_count(
    session_factory: sessionmaker[Session],
) -> None:
    research_job_id = f"replay-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=b"{}",
        decoded_event=event,
    )
    offset = _unique_offset()
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="lifecycle_order_violation",
            processing_attempt_count=1,
            at=T0,
            retention=retention,
        )
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="lifecycle_order_violation",
            processing_attempt_count=1,
            at=T0 + timedelta(seconds=5),
            retention=retention,
        )
    with session_scope(session_factory) as session:
        row = session.execute(
            select(ConsumerDeadLetterModel).where(
                ConsumerDeadLetterModel.consumer_id == _CONSUMER_ID,
                ConsumerDeadLetterModel.kafka_partition == 0,
                ConsumerDeadLetterModel.kafka_offset == offset,
            )
        ).scalar_one()
        assert row.dead_letter_delivery_count == 2
        assert row.processing_attempt_count == 1  # unchanged by redelivery
        assert row.last_failed_at == T0 + timedelta(seconds=5)


# --- TX1: claim/idempotency -------------------------------------------------


def test_fresh_claim_marks_the_row_replaying_with_a_new_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-1",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-1"),
    )
    assert result.outcome is ReplayOutcome.APPLIED
    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAYED_APPLIED"
        assert row.replay_claim_token is None
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, result.attempt_id)
        assert attempt is not None
        assert attempt.status == "APPLIED"


def test_not_found_dead_letter_id_raises(
    session_factory: sessionmaker[Session],
) -> None:
    service = _service(session_factory)
    with pytest.raises(ReplayNotFoundError):
        service.replay(
            dead_letter_id=uuid4(),
            idempotency_key="key-missing",
            actor_id="operator-1",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-1"),
        )


def test_tier_b_ineligible_dead_letter_cannot_be_replayed(
    session_factory: sessionmaker[Session],
) -> None:
    offset = _unique_offset()
    retention = build_retention(
        failure_code="invalid_json", raw_value=b"not json", decoded_event=None
    )
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="invalid_json",
            processing_attempt_count=1,
            at=T0,
            retention=retention,
        )
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    with pytest.raises(ReplayNotEligibleError):
        service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key="key-1",
            actor_id="operator-1",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-1"),
        )


def test_same_key_same_fingerprint_live_claim_returns_existing_in_progress(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)

    # Claim it (TX1 only) without applying, by directly using a service whose
    # clock is far enough in the future during _apply that ownership fails --
    # simpler: claim via a service, then attempt a second claim under the
    # same key/fingerprint while the DB row is still mid-claim. Since
    # ``replay()`` runs claim+apply synchronously, simulate "still live" by
    # claiming through the private _claim method directly.
    service = _service(session_factory)
    claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-live",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-live"),
    )
    assert isinstance(claim, _Claim)

    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-live",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-live"),
    )
    assert result.outcome is ReplayOutcome.IN_PROGRESS
    assert result.attempt_id == claim.attempt_id


def test_same_key_different_fingerprint_is_a_conflict(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-conflict",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-a"),
    )
    with pytest.raises(ReplayConflictError):
        service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key="key-conflict",
            actor_id="operator-1",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-b"),
        )


def test_same_key_same_fingerprint_after_full_success_is_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    """A repeated identical replay request after success is a safe no-op read."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    first = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-repeat",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-repeat"),
    )
    assert first.outcome is ReplayOutcome.APPLIED

    second = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-repeat",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-repeat"),
    )
    assert second.outcome is ReplayOutcome.APPLIED
    assert second.attempt_id == first.attempt_id


def test_already_claimed_by_a_live_lease_under_a_different_key_is_rejected(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-first",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-first"),
    )
    with pytest.raises(ReplayAlreadyClaimedError):
        service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key="key-second",
            actor_id="operator-2",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-second"),
        )


def test_same_key_expired_claim_marks_lost_ownership_and_requires_a_fresh_key(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-expiring",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-expiring"),
    )
    assert isinstance(claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)  # well past the 1s lease
    with pytest.raises(ReplayExpiredAttemptError):
        service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key="key-expiring",
            actor_id="operator-1",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-expiring"),
        )

    with session_scope(session_factory) as session:
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, claim.attempt_id)
        assert attempt is not None
        assert attempt.status == "LOST_OWNERSHIP"


def test_fresh_key_reclaims_an_expired_lease_and_marks_previous_attempt_lost(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    first_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-old",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-old"),
    )
    assert isinstance(first_claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)  # lease now expired
    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-new",
        actor_id="operator-2",
        operator_reason="reclaiming",
        request_fingerprint=_fp("fp-new"),
    )
    assert result.outcome is ReplayOutcome.APPLIED
    assert result.attempt_id != first_claim.attempt_id

    with session_scope(session_factory) as session:
        old_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, first_claim.attempt_id
        )
        assert old_attempt is not None
        assert old_attempt.status == "LOST_OWNERSHIP"
        new_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, result.attempt_id
        )
        assert new_attempt is not None
        assert new_attempt.status == "APPLIED"


def test_stale_owner_is_prevented_from_applying_after_being_reclaimed(
    session_factory: sessionmaker[Session],
) -> None:
    """A claim holder whose lease is stolen must never invoke the business effect."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    stale_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-stale",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-stale"),
    )
    assert isinstance(stale_claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)
    # A different operator reclaims with a fresh key -- this both expires
    # the stale owner's claim and successfully applies.
    reclaim_result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-reclaimer",
        actor_id="operator-2",
        operator_reason="reclaiming",
        request_fingerprint=_fp("fp-reclaimer"),
    )
    assert reclaim_result.outcome is ReplayOutcome.APPLIED

    # The stale owner's own (private) _apply must never invoke the business
    # effect now that its token is no longer the live one.
    with pytest.raises(ReplayOwnershipLostError):
        service._apply(stale_claim)  # noqa: SLF001

    from atlas.persistence.models.consumer import ResearchJobEventProjectionModel

    with session_scope(session_factory) as session:
        row = session.get(ResearchJobEventProjectionModel, _job_id)
        assert row is not None  # applied exactly once, by the reclaimer


# --- TX1 clock fencing: post-lock time drives every claim decision --------


def test_tx1_uses_fresh_post_lock_time_when_a_concurrent_holder_delays_it(
    session_factory: sessionmaker[Session],
) -> None:
    """``_claim`` must never decide reclaim eligibility -- or compute the new
    lease/timestamps -- from a clock reading taken before the row lock is
    granted.

    Regression scenario: reclaimer B's ``_claim()`` call begins while owner
    A's lease has *not yet* expired, but an unrelated transaction is
    concurrently holding the dead-letter row's lock, forcing B's own
    ``session.get(..., with_for_update=True)`` to block. By the time that
    lock is released and B's call finally proceeds, time has advanced well
    past the lease's expiration. Only a clock reading taken *after* the lock
    is acquired lets B correctly observe the now-expired lease and permit
    the reclaim; a pre-lock reading (the prior bug) would see the
    still-live lease captured at call time and incorrectly reject it.
    """
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=5.0)
    first_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-tx1-old",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-tx1-old"),
    )
    assert isinstance(first_claim, _Claim)

    holder_locked = Event()
    release_holder = Event()
    outcomes: dict[str, object] = {}

    def _lock_holder() -> None:
        session = session_factory()
        try:
            session.get(ConsumerDeadLetterModel, dead_letter_id, with_for_update=True)
            holder_locked.set()
            assert release_holder.wait(timeout=5.0)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _reclaimer() -> None:
        assert holder_locked.wait(timeout=5.0)
        outcomes["claim"] = service._claim(  # noqa: SLF001
            dead_letter_id=dead_letter_id,
            idempotency_key="key-tx1-new",
            actor_id="operator-2",
            operator_reason="reclaiming",
            request_fingerprint=_fp("fp-tx1-new"),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        holder_future = pool.submit(_lock_holder)
        reclaimer_future = pool.submit(_reclaimer)
        assert holder_locked.wait(timeout=5.0)
        # Give the reclaimer thread time to actually enter and block on the
        # row lock held by ``_lock_holder`` before advancing the clock and
        # releasing it -- otherwise the ordering this test depends on (the
        # reclaimer observing the *old* clock value at call time but the
        # *fresh* one only once the lock is granted) is not guaranteed.
        time.sleep(0.3)
        clock["now"] = T0 + timedelta(seconds=10)  # the 5s lease is now expired
        release_holder.set()
        holder_future.result(timeout=5.0)
        reclaimer_future.result(timeout=5.0)

    reclaim = outcomes["claim"]
    assert isinstance(reclaim, _Claim)
    fresh_time = T0 + timedelta(seconds=10)

    with session_scope(session_factory) as session:
        old_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, first_claim.attempt_id
        )
        assert old_attempt is not None
        assert old_attempt.status == "LOST_OWNERSHIP"
        assert old_attempt.finished_at == fresh_time  # post-lock time, not T0

        new_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, reclaim.attempt_id
        )
        assert new_attempt is not None
        assert new_attempt.status == "IN_PROGRESS"
        assert new_attempt.created_at == fresh_time

        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_claim_token == reclaim.ownership_token
        assert row.updated_at == fresh_time
        assert row.replay_lease_expires_at == fresh_time + timedelta(seconds=5.0)
        # The freshly created lease's expiration is computed from the
        # post-lock reading, not a stale pre-lock one, so it starts a full
        # lease duration ahead of the fresh time actually used for this
        # commit (an injected, non-advancing clock here -- this does not by
        # itself guarantee a real deployment's commit completes before the
        # lease would expire; see the docstring above).
        assert row.replay_lease_expires_at > fresh_time


# --- lease-fencing race tests (Blocker 3: post-lock clock, expired-owner
# --- finalization, reclaim-by-new-token, late old-owner mutation) ---------


def test_apply_treats_a_lock_wait_expired_lease_as_ownership_lost(
    session_factory: sessionmaker[Session],
) -> None:
    """A lease that expires *while ``_apply`` is running* (simulated here by
    advancing the clock between claim and apply, standing in for a lock-wait
    that outlasts the lease) must be treated exactly like an already-
    reclaimed lease: ``_apply`` reads the clock only after acquiring the row
    lock, so it self-detects the expiry rather than trusting a stale
    pre-lock reading."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-lockwait",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-lockwait"),
    )
    assert isinstance(claim, _Claim)

    # Nobody has reclaimed it -- the row is still REPLAYING under this same
    # token -- but the lease has now elapsed by the time _apply looks.
    clock["now"] = T0 + timedelta(seconds=10)
    with pytest.raises(ReplayOwnershipLostError):
        service._apply(claim)  # noqa: SLF001

    with session_scope(session_factory) as session:
        # Untouched: no projection/inbox effect, no claim/lease clearing --
        # the row is left exactly as a future reclaimer expects to find it.
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAYING"
        assert row.replay_claim_token == claim.ownership_token
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, claim.attempt_id)
        assert attempt is not None
        assert attempt.status == "LOST_OWNERSHIP"

    from atlas.persistence.models.consumer import ResearchJobEventProjectionModel

    with session_scope(session_factory) as session:
        row2 = session.get(ResearchJobEventProjectionModel, _job_id)
        assert row2 is None  # never applied


def test_record_failure_of_an_expired_unclaimed_lease_never_marks_failed(
    session_factory: sessionmaker[Session],
) -> None:
    """TX3 must apply the exact same unexpired-lease requirement as TX2: an
    expired-but-not-yet-reclaimed owner recording a failure must not mark
    FAILED or clear the row's claim/lease/state -- that would incorrectly
    resurrect a claim a future reclaimer is entitled to take over."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-tx3-expired",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-tx3-expired"),
    )
    assert isinstance(claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)  # lease elapsed, nobody reclaimed
    with pytest.raises(ReplayOwnershipLostError):
        service._record_failure(claim)  # noqa: SLF001

    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAYING"  # never REPLAY_FAILED
        assert row.replay_claim_token == claim.ownership_token  # never cleared
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, claim.attempt_id)
        assert attempt is not None
        assert attempt.status == "LOST_OWNERSHIP"  # never FAILED


def test_reclaim_by_a_new_token_clears_the_old_token_and_lease(
    session_factory: sessionmaker[Session],
) -> None:
    """A successful reclaim assigns a genuinely new ownership token, and the
    dead-letter row's live token afterward matches only the reclaimer's."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    first_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-token-old",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-token-old"),
    )
    assert isinstance(first_claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)
    second_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-token-new",
        actor_id="operator-2",
        operator_reason="reclaiming",
        request_fingerprint=_fp("fp-token-new"),
    )
    assert isinstance(second_claim, _Claim)
    assert second_claim.ownership_token != first_claim.ownership_token

    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_claim_token == second_claim.ownership_token
        old_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, first_claim.attempt_id
        )
        assert old_attempt is not None
        assert old_attempt.status == "LOST_OWNERSHIP"


def test_late_old_owner_record_failure_after_reclaim_does_not_clobber_new_owner(
    session_factory: sessionmaker[Session],
) -> None:
    """A late TX3 call from an owner already superseded by a token-holding
    reclaimer must reject as ownership-lost without touching the new
    owner's live claim -- a straightforward token mismatch, distinct from
    the expired-but-unclaimed case above."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    clock = {"now": T0}

    def _clock() -> datetime:
        return clock["now"]

    service = _service(session_factory, clock=_clock, lease_seconds=1.0)
    stale_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-late-old",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-late-old"),
    )
    assert isinstance(stale_claim, _Claim)

    clock["now"] = T0 + timedelta(seconds=10)
    new_claim = service._claim(  # noqa: SLF001
        dead_letter_id=dead_letter_id,
        idempotency_key="key-late-new",
        actor_id="operator-2",
        operator_reason="reclaiming",
        request_fingerprint=_fp("fp-late-new"),
    )
    assert isinstance(new_claim, _Claim)

    # The old, superseded owner's late TX3 call must be rejected...
    with pytest.raises(ReplayOwnershipLostError):
        service._record_failure(stale_claim)  # noqa: SLF001

    # ...and must never touch the new owner's still-live claim.
    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAYING"
        assert row.replay_claim_token == new_claim.ownership_token
        new_attempt = session.get(
            ConsumerDeadLetterReplayAttemptModel, new_claim.attempt_id
        )
        assert new_attempt is not None
        assert new_attempt.status == "IN_PROGRESS"


def test_replay_duplicate_when_the_inbox_already_recorded_the_event(
    session_factory: sessionmaker[Session],
) -> None:
    """If the original event was already independently applied, replay is a no-op."""
    research_job_id = f"replay-dup-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    from atlas.consumer.ports import InboxOutcome

    inbox = SqlAlchemyInboxRepository()
    projection = SqlAlchemyResearchJobProjectionRepository()
    with session_scope(session_factory) as session:
        outcome = inbox.record_and_apply(
            session,
            consumer_id=_CONSUMER_ID,
            event=event,
            kafka_partition=0,
            kafka_offset=_unique_offset(),
            at=T0,
            apply_effect=lambda s, e: projection.apply(s, e, at=T0),
        )
    assert outcome is InboxOutcome.APPLIED

    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=b"{}",
        decoded_event=event,
    )
    offset = _unique_offset()
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="lifecycle_order_violation",
            processing_attempt_count=1,
            at=T0,
            retention=retention,
        )
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-dup",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-dup"),
    )
    assert result.outcome is ReplayOutcome.DUPLICATE


def test_tx3_records_failure_when_apply_raises_under_valid_ownership(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A TX2 failure while ownership is still valid lands in TX3 as FAILED."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)

    class _FailingProjection:
        def apply(self, session: Session, event: object, *, at: datetime) -> None:
            del session, event, at
            raise LifecycleOrderViolationError()

    service = DeadLetterReplayService(
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=_FailingProjection(),
        lease_seconds=90.0,
        clock=lambda: T0,
    )
    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-tx3",
        actor_id="operator-1",
        operator_reason="investigating",
        request_fingerprint=_fp("fp-tx3"),
    )
    assert result.outcome is ReplayOutcome.FAILED

    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAY_FAILED"
        assert row.replay_claim_token is None
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, result.attempt_id)
        assert attempt is not None
        assert attempt.status == "FAILED"


def test_tx2_an_unexpected_defect_propagates_and_leaves_the_attempt_in_progress(
    session_factory: sessionmaker[Session],
) -> None:
    """A non-``_EXPECTED_APPLY_FAILURES`` exception is not silently recorded FAILED.

    Only ``DomainEventError``/``DBAPIError``/``ConsumerInboxConflictError``/
    ``LifecycleOrderViolationError`` are routed to TX3's ordinary ``FAILED``
    outcome (Blocker 6). Anything else -- e.g. a programming defect in the
    projection -- must propagate out of ``replay()`` uncaught, and TX2 must
    roll back rather than mark the attempt terminal.
    """
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)

    class _BuggyProjection:
        def apply(self, session: Session, event: object, *, at: datetime) -> None:
            del session, event, at
            raise TypeError("unexpected programming defect")

    service = DeadLetterReplayService(
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=_BuggyProjection(),
        lease_seconds=90.0,
        clock=lambda: T0,
    )
    with pytest.raises(TypeError):
        service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key="key-defect",
            actor_id="operator-1",
            operator_reason="investigating",
            request_fingerprint=_fp("fp-defect"),
        )

    with session_scope(session_factory) as session:
        row = session.get(ConsumerDeadLetterModel, dead_letter_id)
        assert row is not None
        assert row.replay_state == "REPLAYING"
        assert row.replay_claim_token is not None
        attempt = session.execute(
            select(ConsumerDeadLetterReplayAttemptModel).where(
                ConsumerDeadLetterReplayAttemptModel.dead_letter_id == dead_letter_id,
                ConsumerDeadLetterReplayAttemptModel.idempotency_key == "key-defect",
            )
        ).scalar_one()
        assert attempt.status == "IN_PROGRESS"
        assert attempt.finished_at is None


def test_replay_failed_row_can_be_reclaimed_with_a_fresh_key(
    session_factory: sessionmaker[Session],
) -> None:
    """After a TX3 FAILED outcome, the row remains eligible for a fresh attempt."""
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)

    class _FailOnceProjection:
        def __init__(self) -> None:
            self.calls = 0

        def apply(self, session: Session, event: object, *, at: datetime) -> None:
            self.calls += 1
            if self.calls == 1:
                raise LifecycleOrderViolationError()
            SqlAlchemyResearchJobProjectionRepository().apply(session, event, at=at)  # type: ignore[arg-type]

    projection = _FailOnceProjection()
    service = DeadLetterReplayService(
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=projection,
        lease_seconds=90.0,
        clock=lambda: T0,
    )
    first = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-retry-1",
        actor_id="operator-1",
        operator_reason="first attempt",
        request_fingerprint=_fp("fp-retry-1"),
    )
    assert first.outcome is ReplayOutcome.FAILED

    second = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-retry-2",
        actor_id="operator-1",
        operator_reason="second attempt",
        request_fingerprint=_fp("fp-retry-2"),
    )
    assert second.outcome is ReplayOutcome.APPLIED


def test_replay_attempt_actor_id_and_reason_are_durably_recorded(
    session_factory: sessionmaker[Session],
) -> None:
    _job_id, offset = _seed_replayable_dead_letter(session_factory)
    dead_letter_id = _dead_letter_id(session_factory, kafka_offset=offset)
    service = _service(session_factory)
    result = service.replay(
        dead_letter_id=dead_letter_id,
        idempotency_key="key-audit",
        actor_id="operator-audit",
        operator_reason="manual investigation of a lifecycle violation",
        request_fingerprint=secrets.token_hex(32),
    )
    with session_scope(session_factory) as session:
        attempt = session.get(ConsumerDeadLetterReplayAttemptModel, result.attempt_id)
        assert attempt is not None
        assert attempt.actor_id == "operator-audit"
        assert (
            attempt.operator_reason == "manual investigation of a lifecycle violation"
        )
