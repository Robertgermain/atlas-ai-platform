"""Shared fixtures for PostgreSQL integration tests."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from tests.integration.db_support import (
    assert_safe_test_database,
    reset_schema_to_empty,
    run_migrations,
    truncate_research_jobs,
)

DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://atlas:atlas@127.0.0.1:5433/atlas_test"


@pytest.fixture(scope="session")
def test_database_url() -> str:
    url = os.environ.get("ATLAS_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    return assert_safe_test_database(url)


@pytest.fixture(scope="session")
def engine(test_database_url: str) -> Iterator[Engine]:
    engine = create_engine(test_database_url, pool_pre_ping=True)
    try:
        reset_schema_to_empty(database_url=test_database_url, engine=engine)
        run_migrations(test_database_url)
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


@pytest.fixture(autouse=True)
def cleanup_research_jobs(test_database_url: str, engine: Engine) -> Iterator[None]:
    yield
    truncate_research_jobs(database_url=test_database_url, engine=engine)
