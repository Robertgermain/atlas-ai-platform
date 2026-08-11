"""Network-free unit tests for ``atlas.consumer.db.build_consumer_engine``.

Verifies the exact bounds ``build_consumer_engine`` applies without ever
constructing a real database connection: ``create_engine`` itself is
patched to capture its call arguments, since SQLAlchemy does not open a
socket until a connection is actually checked out.
"""

from __future__ import annotations

from typing import Any

import pytest

from atlas.consumer import db as consumer_db
from atlas.consumer.db import build_consumer_engine

_FAKE_DATABASE_URL = (
    "postgresql+psycopg://unit-test-user:unit-test-secret@db-host:5432/atlas"
)


class _RecordingEngine:
    """A minimal stand-in so ``build_consumer_engine`` can return *something*."""


def _capture_create_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_create_engine(url: str, **kwargs: Any) -> _RecordingEngine:
        captured["url"] = url
        captured.update(kwargs)
        return _RecordingEngine()

    monkeypatch.setattr(consumer_db, "create_engine", fake_create_engine)
    return captured


def test_applies_the_pool_checkout_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=3.0,
        pool_timeout_seconds=7.5,
        statement_timeout_seconds=4.0,
    )
    assert captured["pool_timeout"] == 7.5


def test_applies_the_postgres_connect_timeout_ceiling_rounded_to_whole_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ceiling, not banker's/nearest rounding: 3.4s must become 4, never 3 --
    a shorter effective timeout than configured would be less strict than
    requested, and (more importantly, see the zero-boundary tests below)
    rounding down is exactly the direction that can reach 0."""
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=3.4,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    assert captured["connect_args"]["connect_timeout"] == 4


def test_applies_the_postgres_statement_timeout_in_milliseconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=2.5,
    )
    assert captured["connect_args"]["options"] == "-c statement_timeout=2500"


def test_a_whole_second_connect_timeout_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An already-whole-second value (e.g. the approved 5.0s default) must
    not be perturbed by the ceiling conversion."""
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    assert captured["connect_args"]["connect_timeout"] == 5
    assert captured["connect_args"]["options"] == "-c statement_timeout=5000"


def test_a_sub_second_connect_timeout_never_becomes_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured value below one second must still produce a nonzero
    effective libpq ``connect_timeout`` -- ``connect_timeout=0`` means
    "wait indefinitely" to libpq, the opposite of a bounded positive
    request. A naive ``round()`` would produce exactly 0 here."""
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=0.2,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    assert captured["connect_args"]["connect_timeout"] == 1
    assert captured["connect_args"]["connect_timeout"] != 0


def test_a_sub_millisecond_statement_timeout_never_becomes_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured value below one millisecond must still produce a
    nonzero effective PostgreSQL ``statement_timeout`` --
    ``statement_timeout=0`` means "no timeout" to PostgreSQL, the opposite
    of a bounded positive request. A naive ``round()`` would produce
    exactly 0 here."""
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=0.0001,
    )
    assert captured["connect_args"]["options"] == "-c statement_timeout=1"


def test_a_sub_second_statement_timeout_converts_up_to_whole_milliseconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """0.0025s must ceiling-round to 3ms, never 2ms (banker's/nearest
    rounding) and never 0ms."""
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=0.0025,
    )
    assert captured["connect_args"]["options"] == "-c statement_timeout=3"


def test_enables_pool_pre_ping(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _capture_create_engine(monkeypatch)
    build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    assert captured["pool_pre_ping"] is True


def test_never_caches_and_returns_a_fresh_engine_every_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engines_built = 0

    def fake_create_engine(url: str, **kwargs: object) -> _RecordingEngine:
        nonlocal engines_built
        del url, kwargs
        engines_built += 1
        return _RecordingEngine()

    monkeypatch.setattr(consumer_db, "create_engine", fake_create_engine)
    first = build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    second = build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=5.0,
        statement_timeout_seconds=5.0,
    )
    assert engines_built == 2
    assert first is not second


def test_never_logs_or_exposes_the_database_url_or_credentials(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The one deliberate exception: ``create_engine`` itself receives the
    real URL (it must, to connect) -- this only proves ``build_consumer_engine``
    does not additionally log it anywhere on the way there."""
    _capture_create_engine(monkeypatch)
    with caplog.at_level("DEBUG"):
        build_consumer_engine(
            _FAKE_DATABASE_URL,
            connect_timeout_seconds=5.0,
            pool_timeout_seconds=5.0,
            statement_timeout_seconds=5.0,
        )
    assert "unit-test-secret" not in caplog.text
    assert _FAKE_DATABASE_URL not in caplog.text


def test_real_engine_pool_reports_the_configured_checkout_timeout() -> None:
    """One assertion against a genuinely real (but never-connected)
    ``sqlalchemy.create_engine`` result, proving the patched-``create_engine``
    tests above are not hiding a wiring mistake specific to the fake."""
    engine = build_consumer_engine(
        _FAKE_DATABASE_URL,
        connect_timeout_seconds=5.0,
        pool_timeout_seconds=9.0,
        statement_timeout_seconds=5.0,
    )
    try:
        assert engine.pool._timeout == 9.0  # type: ignore[attr-defined]  # noqa: SLF001
        assert str(engine.url) != _FAKE_DATABASE_URL  # password masked
        assert "unit-test-secret" not in str(engine.url)
    finally:
        engine.dispose()
