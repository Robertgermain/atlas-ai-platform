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


def truncate_research_jobs(*, database_url: str, engine: Engine) -> None:
    """Remove all research_jobs rows on a guarded test database."""
    assert_safe_test_database(database_url)
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE research_jobs"))
