"""Injectable clocks for deterministic outbox relay fencing tests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(UTC)


class ControllableClock:
    """Mutable UTC clock for lease-expiry and ordering tests without sleeps."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("ControllableClock start must be timezone-aware.")
        self._now = start.astimezone(UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now = self._now + delta

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("ControllableClock set() requires a timezone-aware value.")
        self._now = instant.astimezone(UTC)


Clock = Callable[[], datetime]
