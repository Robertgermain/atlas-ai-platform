"""Verify Alembic migrations for research_jobs."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text
from sqlalchemy.orm import sessionmaker

from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from tests.integration.db_support import (
    REPO_ROOT,
    assert_safe_test_database,
    initialize_langgraph_checkpoint_schema,
)


def _alembic_config(database_url: str) -> Config:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _reset_public_schema(engine: Engine) -> None:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO CURRENT_USER"))
        connection.execute(text("GRANT ALL ON SCHEMA public TO public"))


def test_empty_database_migrates_to_head(engine: Engine) -> None:
    inspector = inspect(engine)
    assert inspector.has_table("research_jobs")

    constraint_names = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("research_jobs")
    }
    assert "ck_research_jobs_status" in constraint_names
    assert "ck_research_jobs_status_fields" in constraint_names
    assert "ck_research_jobs_idempotency_pair" in constraint_names
    assert "ck_research_jobs_claim_lease_pair" in constraint_names

    unique_names = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("research_jobs")
    }
    assert "uq_research_jobs_idempotency_key" in unique_names

    columns = {column["name"] for column in inspector.get_columns("research_jobs")}
    assert "idempotency_key" in columns
    assert "request_fingerprint" in columns
    assert "lease_expires_at" in columns
    assert "claim_token" in columns

    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    assert version == "20260809_0004"
    assert inspector.has_table("workflow_executions")
    assert inspector.has_table("workflow_node_executions")


def test_legacy_row_survives_upgrade_from_0001(
    test_database_url: str,
    engine: Engine,
) -> None:
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260808_0001")

        created_at = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO research_jobs (
                        id, question, status, created_at, updated_at,
                        started_at, finished_at, result, failure_reason
                    ) VALUES (
                        :id, :question, :status, :created_at, :updated_at,
                        NULL, NULL, NULL, NULL
                    )
                    """
                ),
                {
                    "id": "legacy-job",
                    "question": "legacy question",
                    "status": "PENDING",
                    "created_at": created_at,
                    "updated_at": created_at,
                },
            )

        command.upgrade(config, "head")

        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                    SELECT id, question, status, idempotency_key, request_fingerprint,
                           lease_expires_at, claim_token
                    FROM research_jobs
                    WHERE id = :id
                    """
                    ),
                    {"id": "legacy-job"},
                )
                .mappings()
                .one()
            )
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()

        assert version == "20260809_0004"
        assert row["id"] == "legacy-job"
        assert row["question"] == "legacy question"
        assert row["status"] == "PENDING"
        assert row["idempotency_key"] is None
        assert row["request_fingerprint"] is None
        assert row["lease_expires_at"] is None
        assert row["claim_token"] is None

        factory = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        repo = SqlAlchemyResearchJobRepository()
        with session_scope(factory) as session:
            loaded = repo.get(session, "legacy-job")

        assert loaded is not None
        assert loaded.id == "legacy-job"
        assert loaded.question == "legacy question"
        assert loaded.status.value == "PENDING"
        assert loaded.created_at == created_at
        assert loaded.updated_at == created_at
        assert loaded.started_at is None
        assert loaded.finished_at is None
        assert loaded.result is None
        assert loaded.failure_reason is None
    finally:
        try:
            _reset_public_schema(engine)
            command.upgrade(config, "head")
            initialize_langgraph_checkpoint_schema(test_database_url)
        finally:
            if previous is None:
                os.environ.pop("ATLAS_DATABASE_URL", None)
            else:
                os.environ["ATLAS_DATABASE_URL"] = previous
