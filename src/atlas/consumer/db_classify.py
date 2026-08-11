"""Centralized SQLAlchemy/psycopg transient-vs-fatal error classification.

Mirrors ``atlas.outbox.kafka_errors``'s centralization pattern: a single
function decides transient (bounded retry) versus fatal (immediate
terminate), and this module never inspects an exception's string message.

Policy (narrow and fail-closed, in order):

1. Not a SQLAlchemy ``DBAPIError`` -> fatal. No signal to classify by (e.g.
   ``ConsumerInboxConflictError``, an unrecognized ``IntegrityError``, or a
   programming error).
2. ``exc.connection_invalidated`` -> transient. SQLAlchemy's own signal that
   the DBAPI connection was invalidated (dropped/reset) and the pool will
   recycle it -- a textbook transient condition.
3. The underlying driver exception exposes a psycopg3 ``sqlstate`` in SQLSTATE
   class ``08`` (connection exception) -> transient.
4. ``sqlstate`` is exactly ``40001`` (serialization_failure) or ``40P01``
   (deadlock_detected) -> transient. Both are documented PostgreSQL
   retry-safe conditions for a statement that never committed.
5. Any other ``sqlstate`` (including ``None``/absent) -> fatal. An unknown or
   absent SQLSTATE fails closed rather than being assumed safe to retry.

Only ``DBAPIError`` (a real database-driver failure) is ever classified
transient here -- ``IntegrityError`` (a constraint violation) is always
fatal regardless of any SQLSTATE it happens to carry, because a constraint
violation is not resolved by simply retrying the same statement.
"""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy.exc import DBAPIError, IntegrityError

#: SQLSTATE codes explicitly approved as transient despite not being in
#: connection-exception class 08. Each requires a real (or accurately
#: constructed) integration test proving the exact code is safe to retry --
#: never a guess. Do not widen this without such evidence.
_APPROVED_TRANSIENT_SQLSTATES: frozenset[str] = frozenset(
    {
        "40001",  # serialization_failure
        "40P01",  # deadlock_detected
    }
)

_CONNECTION_EXCEPTION_SQLSTATE_CLASS = "08"


class DatabaseErrorClass(StrEnum):
    """Classification outcome for a database-layer exception."""

    TRANSIENT = "transient"
    FATAL = "fatal"


def _sqlstate_of(exc: DBAPIError) -> str | None:
    """Extract psycopg3's SQLSTATE from the wrapped driver exception, if any."""
    orig = exc.orig
    sqlstate = getattr(orig, "sqlstate", None)
    return sqlstate if isinstance(sqlstate, str) else None


def classify_database_error(exc: BaseException) -> DatabaseErrorClass:
    """Classify a database-layer exception per the narrow, fail-closed policy above."""
    if isinstance(exc, IntegrityError):
        return DatabaseErrorClass.FATAL
    if not isinstance(exc, DBAPIError):
        return DatabaseErrorClass.FATAL
    if exc.connection_invalidated:
        return DatabaseErrorClass.TRANSIENT
    sqlstate = _sqlstate_of(exc)
    if sqlstate is None:
        return DatabaseErrorClass.FATAL
    if sqlstate[:2] == _CONNECTION_EXCEPTION_SQLSTATE_CLASS:
        return DatabaseErrorClass.TRANSIENT
    if sqlstate in _APPROVED_TRANSIENT_SQLSTATES:
        return DatabaseErrorClass.TRANSIENT
    return DatabaseErrorClass.FATAL
