"""Network-free unit tests for ``atlas.consumer.db_classify.classify_database_error``.

See that module's docstring for the exact classification policy.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from atlas.consumer.db_classify import DatabaseErrorClass, classify_database_error
from atlas.consumer.fakes import build_dbapi_error


@pytest.mark.parametrize("sqlstate", ["08000", "08003", "08006", "08001", "08004"])
def test_connection_exception_class_08_is_transient(sqlstate: str) -> None:
    exc = build_dbapi_error(sqlstate=sqlstate)
    assert classify_database_error(exc) is DatabaseErrorClass.TRANSIENT


@pytest.mark.parametrize("sqlstate", ["40001", "40P01"])
def test_approved_transient_sqlstates_outside_class_08_are_transient(
    sqlstate: str,
) -> None:
    exc = build_dbapi_error(sqlstate=sqlstate)
    assert classify_database_error(exc) is DatabaseErrorClass.TRANSIENT


def test_connection_invalidated_flag_is_transient_regardless_of_sqlstate() -> None:
    exc = build_dbapi_error(sqlstate=None, connection_invalidated=True)
    assert classify_database_error(exc) is DatabaseErrorClass.TRANSIENT


@pytest.mark.parametrize("sqlstate", [None, "42601", "23505", "22001", "0A000"])
def test_unknown_or_unapproved_sqlstate_is_fatal(sqlstate: str | None) -> None:
    exc = build_dbapi_error(sqlstate=sqlstate)
    assert classify_database_error(exc) is DatabaseErrorClass.FATAL


def test_integrity_error_is_always_fatal_even_with_a_transient_looking_sqlstate() -> (
    None
):
    """A constraint violation is never resolved by retrying the same statement."""

    class _FakeOrig(Exception):
        sqlstate = "08006"

    exc = IntegrityError("INSERT ...", {}, _FakeOrig())
    assert classify_database_error(exc) is DatabaseErrorClass.FATAL


def test_non_dbapi_exception_is_fatal() -> None:
    assert classify_database_error(RuntimeError("boom")) is DatabaseErrorClass.FATAL
    assert classify_database_error(ValueError("boom")) is DatabaseErrorClass.FATAL


def test_no_sqlstate_and_no_connection_invalidated_is_fatal() -> None:
    exc = build_dbapi_error(sqlstate=None, connection_invalidated=False)
    assert classify_database_error(exc) is DatabaseErrorClass.FATAL
