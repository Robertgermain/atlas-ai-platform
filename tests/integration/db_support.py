"""Integration-test-only database guard and schema reset helpers.

These helpers must never be imported by production application code.
"""

from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, text
from sqlalchemy.engine.url import make_url

from atlas.persistence.exceptions import UnsafeTestDatabaseError
from atlas.workflow import create_checkpoint_runtime, initialize_checkpointer_schema

REPO_ROOT = Path(__file__).resolve().parents[2]


def assert_safe_test_database(database_url: str) -> str:
    """Refuse destructive operations unless the URL targets a dedicated test DB."""
    database_name = make_url(database_url).database
    if database_name != "atlas_test" and not (
        database_name is not None and database_name.endswith("_test")
    ):
        raise UnsafeTestDatabaseError(
            "Refusing destructive test database operation for "
            f"database name {database_name!r}; expected atlas_test or *_test."
        )
    return database_url


def reset_schema_to_empty(*, database_url: str, engine: Engine) -> None:
    """Drop and recreate public schema using AUTOCOMMIT on a guarded test DB."""
    assert_safe_test_database(database_url)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO CURRENT_USER"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO public"))


def run_migrations(database_url: str) -> None:
    """Upgrade the guarded test database to Alembic head."""
    assert_safe_test_database(database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = database_url
    try:
        config = Config(str(REPO_ROOT / "alembic.ini"))
        config.set_main_option("sqlalchemy.url", database_url)
        command.upgrade(config, "head")
    finally:
        if previous is None:
            os.environ.pop("ATLAS_DATABASE_URL", None)
        else:
            os.environ["ATLAS_DATABASE_URL"] = previous


def initialize_langgraph_checkpoint_schema(database_url: str) -> None:
    """Create LangGraph-owned checkpoint tables once (idempotent setup())."""
    assert_safe_test_database(database_url)
    runtime = create_checkpoint_runtime(database_url)
    try:
        initialize_checkpointer_schema(runtime)
    finally:
        runtime.close()


def truncate_integration_tables(*, database_url: str, engine: Engine) -> None:
    """Clear Atlas job/audit rows and LangGraph checkpoint data between tests."""
    assert_safe_test_database(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                TRUNCATE TABLE
                    model_invocation_attempts,
                    model_invocations,
                    workflow_node_executions,
                    workflow_executions,
                    research_jobs,
                    checkpoint_writes,
                    checkpoint_blobs,
                    checkpoints
                RESTART IDENTITY CASCADE
                """
            )
        )
