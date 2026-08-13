"""Singleton PostgreSQL advisory-lock ownership for the outbox relay.

This first implementation intentionally supports one relay process. Multi-relay
sharding is deferred. The lock is held on a dedicated connection for the
relay's lifetime and is never acquired on a pooled session that returns to the
pool while still "owning" the lock.
"""

from __future__ import annotations

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection

from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.outbox.errors import RelayOwnershipError

# Stable dedicated key for Milestone 13 Slice 13B. Do not change once deployed
# without a coordinated dual-key migration; two code versions would otherwise
# appear to both hold "the" relay lock.
OUTBOX_RELAY_ADVISORY_LOCK_KEY = 738_192_013


class PostgresOutboxRelayLock:
    """Acquire/release the singleton outbox-relay advisory lock."""

    def __init__(self, engine: Engine, *, metrics: AtlasMetrics | None = None) -> None:
        self._engine = engine
        self._connection: Connection | None = None
        self._metrics = metrics or default_metrics()

    @property
    def held(self) -> bool:
        return self._connection is not None

    def acquire(self) -> None:
        """Acquire the lock or fail clearly without processing any rows."""
        if self._connection is not None:
            return
        # AUTOCOMMIT so the advisory lock is session-scoped, not TX-scoped.
        connection = self._engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        try:
            acquired = connection.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": OUTBOX_RELAY_ADVISORY_LOCK_KEY},
            ).scalar_one()
            if not acquired:
                connection.close()
                raise RelayOwnershipError(
                    "Outbox relay advisory lock is already held by another process."
                )
        except RelayOwnershipError:
            raise
        except Exception:
            connection.close()
            raise
        self._connection = connection
        self._metrics.set_outbox_relay_lock_held(held=True)

    def release(self) -> None:
        """Release the lock on clean shutdown. Connection close also releases it.

        The gauge update sits in its own outer ``finally`` so it always runs
        exactly once -- regardless of whether the unlock statement, the
        connection close, both, or neither raise. Nesting it inside the
        same ``finally`` as ``connection.close()`` (the prior shape) meant a
        ``close()`` failure raised out of that block before the gauge line
        was ever reached, silently leaving the gauge reporting "held" for a
        connection this process no longer owns.
        """
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            try:
                connection.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": OUTBOX_RELAY_ADVISORY_LOCK_KEY},
                )
            finally:
                connection.close()
        finally:
            self._metrics.set_outbox_relay_lock_held(held=False)

    def abandon_connection(self) -> None:
        """Discard the dedicated connection without an explicit unlock.

        Simulates process/connection death. Uses ``invalidate()`` so the
        pooled backend session is destroyed and PostgreSQL drops the
        session-level advisory lock. Outbox row leases are not cleared.
        """
        connection = self._connection
        self._connection = None
        if connection is not None:
            connection.invalidate()
        self._metrics.set_outbox_relay_lock_held(held=False)

    def __enter__(self) -> PostgresOutboxRelayLock:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()
