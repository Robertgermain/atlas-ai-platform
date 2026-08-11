"""Real database round-trip instrumentation (test-only, Slice 13C2B).

Backs the timing-bound tests that verify processing paths stay within
``consumer_max_db_round_trips_per_attempt``. Never imported by production
application code.

Counts cursor executions (``before_cursor_execute``) and DBAPI-level commit/
rollback (``ConnectionEvents.commit`` / ``.rollback``) as the round trips
that ``consumer_max_db_round_trips_per_attempt * consumer_db_statement_
timeout_seconds`` conservatively bounds. Pool checkouts are tracked
separately -- connection acquisition (and any ``pool_pre_ping`` probe that
happens as part of it) is bounded independently by
``consumer_db_pool_timeout_seconds`` / ``consumer_db_connect_timeout_seconds``
in the timing formula, not multiplied by the per-attempt round-trip cap.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine


@dataclass
class RoundTripCounts:
    """Observed database round trips for one instrumented block."""

    cursor_executions: int = 0
    pool_checkouts: int = 0
    commits: int = 0
    rollbacks: int = 0
    statements: list[str] = field(default_factory=list)

    @property
    def statement_timeout_bound_total(self) -> int:
        """Round trips bounded by ``consumer_max_db_round_trips_per_attempt``.

        Cursor executions plus commit/rollback -- deliberately excludes
        ``pool_checkouts``, which the timing formula bounds separately.
        """
        return self.cursor_executions + self.commits + self.rollbacks


@contextmanager
def count_database_round_trips(engine: Engine) -> Iterator[RoundTripCounts]:
    """Count round trips observable at the SQLAlchemy Core level for ``engine``."""
    counts = RoundTripCounts()

    def _on_cursor_execute(
        conn: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        del conn, cursor, parameters, context, executemany
        counts.cursor_executions += 1
        counts.statements.append(statement.split()[0].upper() if statement else "")

    def _on_checkout(
        dbapi_conn: object, connection_record: object, connection_proxy: object
    ) -> None:
        del dbapi_conn, connection_record, connection_proxy
        counts.pool_checkouts += 1

    def _on_commit(conn: Connection) -> None:
        del conn
        counts.commits += 1

    def _on_rollback(conn: Connection) -> None:
        del conn
        counts.rollbacks += 1

    event.listen(engine, "before_cursor_execute", _on_cursor_execute)
    event.listen(engine.pool, "checkout", _on_checkout)
    event.listen(engine, "commit", _on_commit)
    event.listen(engine, "rollback", _on_rollback)
    try:
        yield counts
    finally:
        event.remove(engine, "before_cursor_execute", _on_cursor_execute)
        event.remove(engine.pool, "checkout", _on_checkout)
        event.remove(engine, "commit", _on_commit)
        event.remove(engine, "rollback", _on_rollback)
