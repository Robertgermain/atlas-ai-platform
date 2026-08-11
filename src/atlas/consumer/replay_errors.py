"""Typed errors for the operator dead-letter replay CLI (Slice 13C2B)."""

from __future__ import annotations


class ReplayError(Exception):
    """Base class for replay-command failures."""


class ReplayNotFoundError(ReplayError):
    """Raised when the given dead-letter id does not exist."""


class ReplayNotEligibleError(ReplayError):
    """Raised when the dead-letter row is not replay-eligible or not claimable.

    Covers ``replay_eligible=false`` (Tier-B/untrusted record), and any
    already-terminal state (``REPLAYED_APPLIED`` / ``REPLAYED_DUPLICATE``).
    """


class ReplayAlreadyClaimedError(ReplayError):
    """Raised when the row is ``REPLAYING`` under a live (unexpired) lease.

    Distinct from ``ReplayNotEligibleError``: this is a live, currently
    in-progress replay by (presumably) another operator/process, not a
    structurally ineligible record.
    """


class ReplayConflictError(ReplayError):
    """Raised when an idempotency key is reused with a different request payload."""


class ReplayExpiredAttemptError(ReplayError):
    """Raised when the same idempotency key's prior attempt's lease expired.

    The prior attempt is durably marked ``LOST_OWNERSHIP`` as part of
    raising this. The operator must resubmit with a fresh idempotency key.
    """


class ReplayOwnershipLostError(ReplayError):
    """Raised when this replay's ownership token is no longer the live claim.

    Surfaces during TX2/TX3 when another reclaim has already superseded
    this attempt -- the business effect is never applied in this case.
    """
