"""``atlas_outbox_relay_lock_held`` gauge behavior (Slice 15A2 correction).

Uses fake engine/connection doubles (no real PostgreSQL) so this stays a fast
unit test isolating only the gauge-setting behavior; real advisory-lock
acquisition/contention already has dedicated PostgreSQL integration coverage
in ``tests/integration/test_outbox_relay.py`` and
``tests/integration/test_outbox_relay_kafka.py``.
"""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.outbox.errors import RelayOwnershipError
from atlas.outbox.relay_lock import PostgresOutboxRelayLock


def _gauge_value(metrics: AtlasMetrics) -> float:
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name == "atlas_outbox_relay_lock_held":
                return sample.value
    raise AssertionError("atlas_outbox_relay_lock_held sample not found")


class _ScalarResult:
    def __init__(self, value: bool) -> None:
        self._value = value

    def scalar_one(self) -> bool:
        return self._value


class _FakeAcquireConnection:
    def __init__(self, *, acquired: bool) -> None:
        self._acquired = acquired
        self.closed = False

    def execute(self, *_args: object, **_kwargs: object) -> _ScalarResult:
        return _ScalarResult(self._acquired)

    def close(self) -> None:
        self.closed = True


class _FakeConnectResult:
    def __init__(self, *, acquired: bool) -> None:
        self._acquired = acquired

    def execution_options(self, **_kwargs: object) -> _FakeAcquireConnection:
        return _FakeAcquireConnection(acquired=self._acquired)


class _FakeEngine:
    def __init__(self, *, acquired: bool = True) -> None:
        self._acquired = acquired

    def connect(self) -> _FakeConnectResult:
        return _FakeConnectResult(acquired=self._acquired)


class _FakeHeldConnection:
    """Stands in for the already-acquired dedicated connection used by
    ``release()``/``abandon_connection()``. ``execute()`` (the unlock
    statement) and ``close()`` can each independently be made to raise, so
    tests can prove the gauge is still reset to "not held" no matter which
    one (or neither, or both) fails."""

    def __init__(
        self,
        *,
        execute_raises: Exception | None = None,
        close_raises: Exception | None = None,
    ) -> None:
        self.closed = False
        self.invalidated = False
        self._execute_raises = execute_raises
        self._close_raises = close_raises
        self.execute_calls = 0
        self.close_calls = 0

    def execute(self, *_args: object, **_kwargs: object) -> None:
        self.execute_calls += 1
        if self._execute_raises is not None:
            raise self._execute_raises
        return None

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self._close_raises is not None:
            raise self._close_raises

    def invalidate(self) -> None:
        self.invalidated = True


def _lock_with_held_connection(
    metrics: AtlasMetrics, connection: _FakeHeldConnection
) -> PostgresOutboxRelayLock:
    lock = object.__new__(PostgresOutboxRelayLock)
    lock._engine = None  # type: ignore[assignment]
    lock._metrics = metrics
    lock._connection = connection  # type: ignore[assignment]
    return lock


def test_acquire_sets_gauge_to_held() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    lock = PostgresOutboxRelayLock(_FakeEngine(acquired=True), metrics=metrics)  # type: ignore[arg-type]

    lock.acquire()

    assert lock.held is True
    assert _gauge_value(metrics) == 1.0


def test_release_sets_gauge_back_to_not_held() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    lock = object.__new__(PostgresOutboxRelayLock)
    lock._engine = None  # type: ignore[assignment]
    lock._metrics = metrics
    lock._connection = _FakeHeldConnection()  # type: ignore[assignment]

    lock.release()

    assert lock.held is False
    assert _gauge_value(metrics) == 0.0


def test_abandon_connection_sets_gauge_back_to_not_held() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    lock = object.__new__(PostgresOutboxRelayLock)
    lock._engine = None  # type: ignore[assignment]
    lock._metrics = metrics
    connection = _FakeHeldConnection()
    lock._connection = connection  # type: ignore[assignment]

    lock.abandon_connection()

    assert lock.held is False
    assert connection.invalidated is True
    assert _gauge_value(metrics) == 0.0


def test_failed_acquire_never_sets_gauge_held() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    lock = PostgresOutboxRelayLock(_FakeEngine(acquired=False), metrics=metrics)  # type: ignore[arg-type]

    with pytest.raises(RelayOwnershipError):
        lock.acquire()

    assert lock.held is False
    assert _gauge_value(metrics) == 0.0


def test_release_resets_gauge_even_when_unlock_statement_raises() -> None:
    """``connection.execute()`` (the unlock statement) failing must not
    prevent the gauge from being reset to "not held"."""
    metrics = AtlasMetrics(CollectorRegistry())
    connection = _FakeHeldConnection(execute_raises=RuntimeError("unlock-failed"))
    lock = _lock_with_held_connection(metrics, connection)

    with pytest.raises(RuntimeError, match="unlock-failed"):
        lock.release()

    assert connection.close_calls == 1
    assert lock.held is False
    assert _gauge_value(metrics) == 0.0


def test_release_resets_gauge_even_when_close_raises() -> None:
    """The blocker this test guards against: ``connection.close()`` raising
    must still leave the gauge reset to "not held" rather than stuck at
    "held" because the old code's gauge update lived in the same
    ``finally`` block as the failing ``close()`` call."""
    metrics = AtlasMetrics(CollectorRegistry())
    connection = _FakeHeldConnection(close_raises=RuntimeError("close-failed"))
    lock = _lock_with_held_connection(metrics, connection)

    with pytest.raises(RuntimeError, match="close-failed"):
        lock.release()

    assert connection.execute_calls == 1
    assert lock.held is False
    assert _gauge_value(metrics) == 0.0


def test_release_resets_gauge_even_when_both_unlock_and_close_raise() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    connection = _FakeHeldConnection(
        execute_raises=RuntimeError("unlock-failed"),
        close_raises=RuntimeError("close-failed"),
    )
    lock = _lock_with_held_connection(metrics, connection)

    # The close() failure (raised from the inner finally) propagates in
    # place of the execute() failure -- ordinary Python finally semantics,
    # unchanged by this fix -- but the gauge must still be reset either way.
    with pytest.raises(RuntimeError, match="close-failed"):
        lock.release()

    assert connection.execute_calls == 1
    assert connection.close_calls == 1
    assert lock.held is False
    assert _gauge_value(metrics) == 0.0


def test_repeated_release_is_a_safe_noop_and_gauge_stays_not_held() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    connection = _FakeHeldConnection()
    lock = _lock_with_held_connection(metrics, connection)

    lock.release()
    lock.release()  # must not raise or touch the already-released connection

    assert connection.execute_calls == 1
    assert connection.close_calls == 1
    assert lock.held is False
    assert _gauge_value(metrics) == 0.0
