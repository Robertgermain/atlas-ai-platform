"""Local operator dead-letter replay CLI: ``python -m atlas.consumer.replay``.

No HTTP surface. Actor identity is explicitly self-asserted under a
temporary trusted-shell boundary: whoever can execute this CLI locally is
trusted to state who they are via ``--actor-id``. Slice 13C2B has no
separate authentication/authorization layer for this command -- see
Milestone 16 (security, authentication and supply-chain CI) for that work.

This command never deletes or bypasses inbox rows: replaying an
already-applied ``event_id`` is a safe, inbox-deduplicated no-op
(``ReplayOutcome.DUPLICATE``). It never mutates projection/inbox state to
manufacture eligibility -- a lifecycle-order-violation dead letter may
require a separately authorized correction of the underlying projection
row before a replay of it can ever succeed; this command does not attempt
that correction itself.

Exit codes:

- ``0``: the record was applied, or replay was an idempotent no-op
  (duplicate, or a repeated call with the same idempotency key reporting an
  already-recorded ``applied``/``duplicate`` outcome).
- ``1``: rejected (not found, not eligible, already claimed, conflicting
  idempotency-key reuse, expired same-key attempt), failed, lost ownership
  to a concurrent reclaim, still in progress, or an unexpected/
  infrastructure error.

Logging discipline matches every other Atlas executable
(``atlas.observability.logging``): fixed structured events and
``exc.__class__.__name__`` only -- never ``str(exc)``, ``repr(exc)``,
``exc.args``, a database URL, or any payload-derived text.
"""

from __future__ import annotations

import argparse
import json
import logging
import secrets
import sys
from hashlib import sha256
from uuid import UUID

from atlas.config import get_settings
from atlas.consumer.db import build_consumer_engine
from atlas.consumer.replay_errors import ReplayError
from atlas.observability.events import Event
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.persistence.db import get_session_factory
from atlas.persistence.repositories.consumer_dead_letter import (
    DeadLetterReplayService,
    ReplayOutcome,
    ReplayResult,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

logger = logging.getLogger(__name__)

MAX_ACTOR_ID_LENGTH = 128
MAX_REASON_LENGTH = 512
MAX_IDEMPOTENCY_KEY_LENGTH = 256

_SUCCESS_OUTCOMES = frozenset({ReplayOutcome.APPLIED, ReplayOutcome.DUPLICATE})


def compute_request_fingerprint(
    *, dead_letter_id: str, actor_id: str, operator_reason: str
) -> str:
    """Bind an idempotency key to the rest of a replay request's fields.

    Reusing the same idempotency key with a different ``actor_id`` or
    ``operator_reason`` is rejected as ``ReplayConflictError`` rather than
    silently accepted, mirroring ``ReviewService``'s equivalent pattern.
    """
    payload = json.dumps(
        {
            "dead_letter_id": dead_letter_id,
            "actor_id": actor_id,
            "operator_reason": operator_reason,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m atlas.consumer.replay",
        description=(
            "Replay one dead-lettered Kafka record (local operator CLI, no "
            "HTTP surface)."
        ),
    )
    parser.add_argument("--dead-letter-id", required=True, help="Dead-letter row UUID.")
    parser.add_argument(
        "--actor-id", required=True, help="Self-asserted operator identity."
    )
    parser.add_argument(
        "--reason", required=True, help="Operator-supplied reason (audit trail)."
    )
    parser.add_argument(
        "--idempotency-key",
        required=False,
        default=None,
        help="Reuse to safely retry the exact same request. Random if omitted.",
    )
    return parser.parse_args(argv)


def _validate_bounded(value: str, *, name: str, max_length: int) -> str | None:
    cleaned = value.strip()
    if not cleaned or len(cleaned) > max_length:
        log_event(
            logger,
            Event.REPLAY_INPUT_REJECTED,
            level=logging.ERROR,
            outcome=name,
        )
        return None
    return cleaned


def main(argv: list[str] | None = None) -> int:
    """Run one replay command. Returns the process exit code."""
    configure_logging(service_role="consumer-replay")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    try:
        dead_letter_id = UUID(args.dead_letter_id)
    except ValueError:
        log_event(
            logger,
            Event.REPLAY_INPUT_REJECTED,
            level=logging.ERROR,
            outcome="--dead-letter-id",
        )
        return 1

    actor_id = _validate_bounded(
        args.actor_id, name="--actor-id", max_length=MAX_ACTOR_ID_LENGTH
    )
    operator_reason = _validate_bounded(
        args.reason, name="--reason", max_length=MAX_REASON_LENGTH
    )
    idempotency_key = _validate_bounded(
        args.idempotency_key or secrets.token_hex(16),
        name="--idempotency-key",
        max_length=MAX_IDEMPOTENCY_KEY_LENGTH,
    )
    if actor_id is None or operator_reason is None or idempotency_key is None:
        return 1

    try:
        settings = get_settings()
        engine = build_consumer_engine(
            settings.database_url,
            connect_timeout_seconds=settings.consumer_db_connect_timeout_seconds,
            pool_timeout_seconds=settings.consumer_db_pool_timeout_seconds,
            statement_timeout_seconds=settings.consumer_db_statement_timeout_seconds,
        )
        session_factory = get_session_factory(engine)
    except Exception as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1

    service = DeadLetterReplayService(
        session_factory=session_factory,
        inbox=SqlAlchemyInboxRepository(),
        projection=SqlAlchemyResearchJobProjectionRepository(),
        lease_seconds=settings.consumer_replay_lease_seconds,
    )
    fingerprint = compute_request_fingerprint(
        dead_letter_id=str(dead_letter_id),
        actor_id=actor_id,
        operator_reason=operator_reason,
    )

    result: ReplayResult
    try:
        result = service.replay(
            dead_letter_id=dead_letter_id,
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            operator_reason=operator_reason,
            request_fingerprint=fingerprint,
        )
    except ReplayError as exc:
        log_exception_boundary(
            logger, Event.REPLAY_ATTEMPT_FAILED, exc, outcome="rejected"
        )
        return 1
    except Exception as exc:
        log_exception_boundary(
            logger, Event.REPLAY_ATTEMPT_FAILED, exc, outcome="unexpected_error"
        )
        return 1

    log_event(logger, Event.REPLAY_FINISHED, outcome=result.outcome.value)
    return 0 if result.outcome in _SUCCESS_OUTCOMES else 1


if __name__ == "__main__":
    sys.exit(main())
