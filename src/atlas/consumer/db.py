"""Dedicated, narrowly-scoped, non-cached database engine for the consumer
executable and dead-letter replay CLI (Slice 13C2B correction pass).

``atlas.persistence.db.get_engine`` is a process-wide ``lru_cache``d
singleton shared by the HTTP API and worker -- it applies no connect,
pool-checkout, or statement timeout at all, and changing it would silently
change every other engine consumer in the process. ``atlas.consumer.timing``'s
worst-case timing formula assumes ``consumer_db_connect_timeout_seconds``,
``consumer_db_pool_timeout_seconds``, and ``consumer_db_statement_timeout_seconds``
are actually enforced by the driver/pool -- until this module existed, those
settings were computed into the timing margin but never applied to any real
connection. This module is the single place that turns those three settings
into an actual bounded engine, used only by ``python -m atlas.consumer`` and
``python -m atlas.consumer.replay``.

Bounds applied:

- PostgreSQL connect timeout: the libpq/psycopg3 ``connect_timeout``
  connection parameter (whole seconds -- libpq itself only accepts whole
  seconds), converted via ``atlas.config.timeout_math.
  effective_connect_timeout_seconds`` (ceiling, floored at 1 -- never 0,
  which libpq treats as "no timeout").
- SQLAlchemy pool checkout timeout: ``pool_timeout`` (seconds; SQLAlchemy
  accepts any positive float directly -- no unit conversion, so no
  rounding-to-zero risk).
- PostgreSQL statement timeout: the server-side ``statement_timeout``
  run-time parameter (whole milliseconds), set via the connection's
  ``options`` startup parameter so it applies to every statement on every
  connection this engine ever hands out -- not just the first. Converted
  via ``atlas.config.timeout_math.effective_statement_timeout_ms``
  (ceiling, floored at 1 -- never 0, which PostgreSQL treats as "no
  timeout").

A naive ``round()`` (banker's rounding, and rounds-down for anything under
0.5) can turn a small positive configured value into ``0`` for either bound
above, silently disabling it entirely (the opposite of what a positive
timeout setting requests). The ceiling-with-floor-of-1 conversion in
``atlas.config.timeout_math`` guarantees a nonzero effective bound -- the
smallest representable whole second/millisecond that is greater than or
equal to the configured value -- for any positive configured value; note
this makes the effective bound *longer* than the configured value
whenever it was fractional, never stricter. ``atlas.consumer.timing``'s
worst-case timing formula uses that exact same (rounded-up) conversion so
the static timing-margin proof always sums the same effective values this
engine actually enforces, rather than underestimating from the raw
configured floats.

Never logs the database URL or any connection parameter value. Each call
returns a fresh engine (no caching): the consumer and replay executables
each construct exactly one, once, at process startup.
"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine

from atlas.config.timeout_math import (
    effective_connect_timeout_seconds,
    effective_statement_timeout_ms,
)


def build_consumer_engine(
    database_url: str,
    *,
    connect_timeout_seconds: float,
    pool_timeout_seconds: float,
    statement_timeout_seconds: float,
) -> Engine:
    """Build a fresh engine bounded by the three consumer/replay timeouts.

    Distinct from ``atlas.persistence.db.get_engine``: never cached, never
    shared with the HTTP API or worker, and applies bounds no other engine
    in the process applies.
    """
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_timeout=pool_timeout_seconds,
        connect_args={
            "connect_timeout": effective_connect_timeout_seconds(
                connect_timeout_seconds
            ),
            "options": (
                f"-c statement_timeout="
                f"{effective_statement_timeout_ms(statement_timeout_seconds)}"
            ),
        },
    )
