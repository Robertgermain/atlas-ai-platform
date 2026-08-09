"""PostgreSQL readiness checks."""

from __future__ import annotations

from sqlalchemy import Engine, text


def check_postgres_ready(engine: Engine) -> None:
    """Verify the database accepts connections. Raises on failure."""
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
