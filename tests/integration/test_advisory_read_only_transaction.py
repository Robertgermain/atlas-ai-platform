"""PostgreSQL READ ONLY DML rejection for advisory snapshot transactions."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text

from atlas.advisor.db import advisory_read_only_scope


def _sqlstate(exc: BaseException) -> str | None:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        state = getattr(current, "sqlstate", None)
        if isinstance(state, str) and state:
            return state
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException):
            current = orig
            continue
        nxt = current.__cause__ or current.__context__
        current = nxt if isinstance(nxt, BaseException) else None
    return None


def _assert_read_only_dml(
    session_factory: sessionmaker[Session], statement: str
) -> None:
    with pytest.raises(DBAPIError) as exc_info:
        with advisory_read_only_scope(session_factory) as session:
            session.execute(text(statement))
    assert _sqlstate(exc_info.value) == "25006"


def test_read_only_select_succeeds(session_factory: sessionmaker[Session]) -> None:
    with advisory_read_only_scope(session_factory) as session:
        count = session.execute(text("SELECT count(*) FROM research_jobs")).scalar_one()
    assert int(count) >= 0


def test_insert_in_read_only_transaction_is_sqlstate_25006(
    session_factory: sessionmaker[Session],
) -> None:
    _assert_read_only_dml(
        session_factory,
        "INSERT INTO research_jobs SELECT * FROM research_jobs WHERE false",
    )


def test_update_in_read_only_transaction_is_sqlstate_25006(
    session_factory: sessionmaker[Session],
) -> None:
    _assert_read_only_dml(
        session_factory,
        "UPDATE research_jobs SET id = id WHERE false",
    )


def test_delete_in_read_only_transaction_is_sqlstate_25006(
    session_factory: sessionmaker[Session],
) -> None:
    _assert_read_only_dml(
        session_factory,
        "DELETE FROM research_jobs WHERE false",
    )
