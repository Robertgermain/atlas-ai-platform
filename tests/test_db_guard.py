"""Guard tests for destructive integration-test database helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from atlas.persistence.exceptions import UnsafeTestDatabaseError
from tests.integration.db_support import (
    assert_safe_test_database,
    reset_schema_to_empty,
)


def test_assert_safe_test_database_accepts_atlas_test() -> None:
    url = "postgresql+psycopg://atlas:atlas@127.0.0.1:5432/atlas_test"
    assert assert_safe_test_database(url) == url


def test_assert_safe_test_database_accepts_suffix_test() -> None:
    url = "postgresql+psycopg://atlas:atlas@127.0.0.1:5432/ci_test"
    assert assert_safe_test_database(url) == url


def test_reset_schema_rejects_non_test_database_without_sql() -> None:
    url = "postgresql+psycopg://atlas:atlas@127.0.0.1:5432/atlas"
    engine = create_engine(url)
    engine.connect = MagicMock(  # type: ignore[method-assign]
        side_effect=AssertionError("destructive SQL must not run"),
    )
    engine.begin = MagicMock(  # type: ignore[method-assign]
        side_effect=AssertionError("destructive SQL must not run"),
    )

    with pytest.raises(UnsafeTestDatabaseError, match="atlas"):
        reset_schema_to_empty(database_url=url, engine=engine)

    engine.connect.assert_not_called()
    engine.begin.assert_not_called()
