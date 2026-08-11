"""Pure timeout-conversion arithmetic shared by ``Settings``' retry-timing
validator and the consumer/replay database-engine boundary (correction
pass, Slice 13C2B).

Lives in ``atlas.config`` -- not ``atlas.consumer`` -- specifically so
``atlas.config.settings.Settings`` can import it without an import cycle:
``atlas.consumer`` already imports ``atlas.config`` transitively, so the
reverse direction would cycle. Kept intentionally tiny and free of any
other Atlas import so both sides can depend on it safely.

A configured ``consumer_db_connect_timeout_seconds``/``consumer_db_
statement_timeout_seconds`` value is a float with only ``> 0`` enforced by
``Settings`` (e.g. ``0.01``). Converting that directly with Python's
``round()`` (banker's rounding) can produce a driver-level
``connect_timeout=0`` -- libpq treats ``0`` as "wait indefinitely", i.e. no
timeout at all -- or ``statement_timeout=0`` -- PostgreSQL treats ``0`` as
"no timeout" -- silently disabling the exact bound the setting exists to
enforce. Ceiling (never banker's-rounding, which rounds some values down)
with an explicit floor of 1 guarantees a nonzero effective bound: the
smallest representable whole second/millisecond that is greater than or
equal to the configured value. That effective bound is therefore never
shorter than what was configured -- but it is longer, not stricter,
whenever the configured value was fractional (e.g. ``0.2`` seconds becomes
an effective ``1`` second, a five-times-longer timeout). Using this same
effective (ceiling-rounded) value in the static timing-margin proof
(``atlas.consumer.timing`` and ``Settings._validate_consumer_retry_timing_
margin``), rather than the raw configured float, keeps that proof a
genuine upper bound on what the runtime engine actually enforces --
runtime can round a fractional configured value up to a slightly larger
effective bound, so the proof must use that same rounded-up value rather
than underestimating from the raw float.
"""

from __future__ import annotations

import math

_MILLISECONDS_PER_SECOND = 1000


def effective_connect_timeout_seconds(configured_seconds: float) -> int:
    """Whole seconds, ceiling-rounded, floored at 1.

    Never 0: libpq's ``connect_timeout=0`` means "no timeout", the opposite
    of what a positive configured value requests.
    """
    return max(1, math.ceil(configured_seconds))


def effective_statement_timeout_ms(configured_seconds: float) -> int:
    """Whole milliseconds, ceiling-rounded, floored at 1.

    Never 0: PostgreSQL's ``statement_timeout=0`` means "no timeout", the
    opposite of what a positive configured value requests.
    """
    return max(1, math.ceil(configured_seconds * _MILLISECONDS_PER_SECOND))


def effective_statement_timeout_seconds(configured_seconds: float) -> float:
    """``effective_statement_timeout_ms`` expressed back in seconds, for timing sums."""
    return effective_statement_timeout_ms(configured_seconds) / _MILLISECONDS_PER_SECOND
