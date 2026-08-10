"""Shared fixtures for PostgreSQL integration tests."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from tests.integration.db_support import (
    assert_safe_test_database,
    initialize_langgraph_checkpoint_schema,
    reset_schema_to_empty,
    run_migrations,
    truncate_integration_tables,
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
        initialize_langgraph_checkpoint_schema(test_database_url)
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
def disable_create_job_rate_limit_for_postgres_integration() -> Iterator[None]:
    """Keep Postgres/API integration POSTs free of the fixed-window budget.

    CI sets ``ATLAS_COORDINATION_PROVIDER=redis``. Starlette TestClient uses one
    shared peer identity (``testclient``) across many files, so a live
    Redis-backed limiter would exhaust the 10/60s budget and flake unrelated
    workflow/API tests. Rate-limit correctness is covered by
    ``tests/integration/test_redis_rate_limit.py`` and the API unit 429
    contract tests with injected limiters.
    """
    from atlas.api.deps import provide_rate_limiter
    from atlas.coordination.noop import NoopRateLimiter
    from atlas.main import app

    previous = app.dependency_overrides.get(provide_rate_limiter)
    app.dependency_overrides[provide_rate_limiter] = lambda: NoopRateLimiter()
    try:
        yield
    finally:
        if previous is not None:
            app.dependency_overrides[provide_rate_limiter] = previous
        else:
            app.dependency_overrides.pop(provide_rate_limiter, None)


@pytest.fixture(autouse=True)
def cleanup_integration_tables(
    test_database_url: str,
    engine: Engine,
) -> Iterator[None]:
    yield
    truncate_integration_tables(database_url=test_database_url, engine=engine)
