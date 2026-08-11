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
    assert version == "20260809_0011"
    assert inspector.has_table("workflow_executions")
    assert inspector.has_table("workflow_node_executions")
    assert inspector.has_table("model_invocations")
    assert inspector.has_table("model_invocation_attempts")
    assert inspector.has_table("tool_invocations")
    assert inspector.has_table("tool_invocation_attempts")
    assert inspector.has_table("sources")
    assert inspector.has_table("documents")
    assert inspector.has_table("evidence_items")
    assert inspector.has_table("evidence_job_links")
    assert inspector.has_table("report_artifacts")
    assert inspector.has_table("claims")
    assert inspector.has_table("citations")
    assert inspector.has_table("evidence_embeddings")
    assert inspector.has_table("evaluation_runs")
    assert inspector.has_table("evaluation_dimension_results")
    assert inspector.has_table("policy_decisions")
    assert inspector.has_table("job_recovery_attempts")
    assert inspector.has_table("human_review_decisions")
    assert inspector.has_table("outbox_events")

    outbox_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("outbox_events")
    }
    assert "ck_outbox_events_event_version" in outbox_constraints
    assert "ck_outbox_events_claim_pair" in outbox_constraints
    assert "ck_outbox_events_published_clears_claim" in outbox_constraints
    assert "ck_outbox_events_payload_size" in outbox_constraints
    outbox_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("outbox_events")
    }
    assert "uq_outbox_events_outbox_position" in outbox_uniques
    outbox_indexes = {index["name"] for index in inspector.get_indexes("outbox_events")}
    assert "ix_outbox_events_claimable_position" in outbox_indexes
    assert "ix_outbox_events_aggregate_history" in outbox_indexes
    assert "ix_outbox_events_occurred_at" in outbox_indexes

    job_columns = {column["name"] for column in inspector.get_columns("research_jobs")}
    assert "repair_count" in job_columns
    assert "job_retry_count" in job_columns
    assert "evaluation_attempt_count" in job_columns
    assert "next_attempt_at" in job_columns
    assert "continuation_mode" in job_columns
    assert "claimed_continuation_mode" in job_columns
    assert "active_workflow_execution_id" in job_columns

    evaluation_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("evaluation_runs")
    }
    assert "ck_evaluation_runs_status" in evaluation_constraints
    assert "ck_evaluation_runs_profile" in evaluation_constraints
    assert "ck_evaluation_runs_job_claim_fingerprint_len" in evaluation_constraints
    evaluation_columns = {
        column["name"] for column in inspector.get_columns("evaluation_runs")
    }
    assert "job_claim_fingerprint" in evaluation_columns
    evaluation_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("evaluation_runs")
    }
    assert "uq_evaluation_runs_execution_profile_attempt" in evaluation_uniques
    workflow_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("workflow_executions")
    }
    assert "uq_workflow_executions_id_research_job_id" in workflow_uniques
    evaluation_fks = {
        constraint["name"]
        for constraint in inspector.get_foreign_keys("evaluation_runs")
    }
    assert "fk_evaluation_runs_execution_job_pair" in evaluation_fks
    model_node_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("model_invocations")
    }
    assert "ck_model_invocations_node_name" in model_node_constraints

    document_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("documents")
    }
    assert "uq_documents_source_hash_parser" in document_uniques
    report_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("report_artifacts")
    }
    assert "uq_report_artifacts_workflow_execution_id" in report_uniques

    model_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("model_invocations")
    }
    assert "ck_model_invocations_status" in model_constraints
    assert "ck_model_invocations_status_fields" in model_constraints
    attempt_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("model_invocation_attempts")
    }
    assert "uq_model_invocation_attempts_number" in attempt_uniques
    tool_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("tool_invocations")
    }
    assert "ck_tool_invocations_status" in tool_constraints
    assert "ck_tool_invocations_origin_fields" in tool_constraints
    tool_attempt_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("tool_invocation_attempts")
    }
    assert "uq_tool_invocation_attempts_number" in tool_attempt_uniques


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

        assert version == "20260809_0011"
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


def test_upgrade_downgrade_0009_and_0008(
    test_database_url: str,
    engine: Engine,
) -> None:
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0008")
        inspector = inspect(engine)
        assert not inspector.has_table("evaluation_runs")
        assert not inspector.has_table("evaluation_dimension_results")

        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0008"

        command.upgrade(config, "20260809_0009")
        inspector = inspect(engine)
        assert inspector.has_table("evaluation_runs")
        assert inspector.has_table("evaluation_dimension_results")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0009"

        command.downgrade(config, "20260809_0008")
        inspector = inspect(engine)
        assert not inspector.has_table("evaluation_runs")
        assert not inspector.has_table("evaluation_dimension_results")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0008"

        command.upgrade(config, "20260809_0009")
        inspector = inspect(engine)
        assert inspector.has_table("evaluation_runs")
        assert inspector.has_table("evaluation_dimension_results")
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


def test_upgrade_downgrade_0010_and_0009(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0010 ↔ 0009 round-trip required by Slice 12B."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0009")
        inspector = inspect(engine)
        assert inspector.has_table("evaluation_runs")
        assert not inspector.has_table("policy_decisions")

        command.upgrade(config, "20260809_0010")
        inspector = inspect(engine)
        assert inspector.has_table("policy_decisions")
        assert inspector.has_table("job_recovery_attempts")
        assert inspector.has_table("human_review_decisions")

        command.downgrade(config, "20260809_0009")
        inspector = inspect(engine)
        assert not inspector.has_table("policy_decisions")
        assert inspector.has_table("evaluation_runs")

        command.upgrade(config, "20260809_0010")
        inspector = inspect(engine)
        assert inspector.has_table("policy_decisions")
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


def test_upgrade_downgrade_0011_and_0010(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0011 ↔ 0010 round-trip required by Slice 13B."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0010")
        inspector = inspect(engine)
        assert inspector.has_table("policy_decisions")
        assert not inspector.has_table("outbox_events")

        command.upgrade(config, "20260809_0011")
        inspector = inspect(engine)
        assert inspector.has_table("outbox_events")

        command.downgrade(config, "20260809_0010")
        inspector = inspect(engine)
        assert not inspector.has_table("outbox_events")
        assert inspector.has_table("policy_decisions")

        command.upgrade(config, "20260809_0011")
        inspector = inspect(engine)
        assert inspector.has_table("outbox_events")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0011"
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


def test_migrations_0001_through_0011_unchanged_versus_main() -> None:
    """Slice 13C1 must not modify migrations 0001–0011 relative to origin/main.

    Migration ``20260809_0011`` (the outbox table added in Slice 13B) is now
    merged into ``origin/main`` through Pull Request #20, so the invariant
    covers all eleven prior migrations, not just 0001–0010.

    Compares against the remote-tracking ref ``origin/main`` (not a local
    ``main`` branch) so GitHub Actions PR checkouts succeed when only remotes
    exist. Fails with a controlled assertion if the base ref cannot be resolved.
    """
    from tests.migration_history import (
        assert_prior_migrations_unchanged_versus_origin_main,
    )

    assert_prior_migrations_unchanged_versus_origin_main(REPO_ROOT)
