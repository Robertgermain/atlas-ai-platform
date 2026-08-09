"""PostgreSQL readiness integration checks."""

from __future__ import annotations

from sqlalchemy import Engine

from atlas.persistence.readiness import check_postgres_ready


def test_postgres_ready_against_live_database(engine: Engine) -> None:
    check_postgres_ready(engine)
