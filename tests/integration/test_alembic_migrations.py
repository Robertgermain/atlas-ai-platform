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
    assert "ck_research_jobs_traceparent_format" in constraint_names
    assert "ck_research_jobs_initial_traceparent_consumed_pair" in constraint_names
    assert "ck_research_jobs_evaluation_profile_allowed" in constraint_names
    assert "ck_research_jobs_started_has_evaluation_profile" in constraint_names

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
    assert "traceparent" in columns
    assert "initial_traceparent_consumed_at" in columns
    assert "evaluation_profile" in columns

    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    assert version == "20260813_0015"
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
    assert "ck_outbox_events_traceparent_format" in outbox_constraints
    outbox_columns = {
        column["name"] for column in inspector.get_columns("outbox_events")
    }
    assert "traceparent" in outbox_columns
    outbox_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("outbox_events")
    }
    assert "uq_outbox_events_outbox_position" in outbox_uniques
    outbox_indexes = {index["name"] for index in inspector.get_indexes("outbox_events")}
    assert "ix_outbox_events_claimable_position" in outbox_indexes
    assert "ix_outbox_events_aggregate_history" in outbox_indexes
    assert "ix_outbox_events_occurred_at" in outbox_indexes

    assert inspector.has_table("consumer_inbox")
    consumer_inbox_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("consumer_inbox")
    }
    assert "ck_consumer_inbox_consumer_id" in consumer_inbox_constraints
    assert "ck_consumer_inbox_event_type" in consumer_inbox_constraints
    assert "ck_consumer_inbox_kafka_partition_nonneg" in consumer_inbox_constraints
    assert "ck_consumer_inbox_kafka_offset_nonneg" in consumer_inbox_constraints
    consumer_inbox_pk = inspector.get_pk_constraint("consumer_inbox")
    assert set(consumer_inbox_pk["constrained_columns"]) == {
        "consumer_id",
        "event_id",
    }

    assert inspector.has_table("research_job_event_projection")
    projection_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints(
            "research_job_event_projection"
        )
    }
    assert "ck_research_job_event_projection_event_type" in projection_constraints
    projection_pk = inspector.get_pk_constraint("research_job_event_projection")
    assert projection_pk["constrained_columns"] == ["research_job_id"]

    assert inspector.has_table("consumer_dead_letters")
    dead_letter_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("consumer_dead_letters")
    }
    assert "ck_consumer_dead_letters_failure_code" in dead_letter_constraints
    assert "ck_consumer_dead_letters_replay_state" in dead_letter_constraints
    dead_letter_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_dead_letters")
    }
    assert "uq_consumer_dead_letters_identity" in dead_letter_uniques

    assert inspector.has_table("consumer_dead_letter_replay_attempts")
    replay_attempt_constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints(
            "consumer_dead_letter_replay_attempts"
        )
    }
    assert (
        "ck_consumer_dead_letter_replay_attempts_status" in replay_attempt_constraints
    )
    replay_attempt_uniques = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints(
            "consumer_dead_letter_replay_attempts"
        )
    }
    assert "uq_consumer_dead_letter_replay_attempts_key" in replay_attempt_uniques
    replay_attempt_fks = {
        constraint["name"]
        for constraint in inspector.get_foreign_keys(
            "consumer_dead_letter_replay_attempts"
        )
    }
    assert (
        "fk_consumer_dead_letter_replay_attempts_dead_letter_id" in replay_attempt_fks
    )

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
                           lease_expires_at, claim_token, traceparent,
                           initial_traceparent_consumed_at, evaluation_profile
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

        assert version == "20260813_0015"
        assert row["id"] == "legacy-job"
        assert row["question"] == "legacy question"
        assert row["status"] == "PENDING"
        assert row["idempotency_key"] is None
        assert row["request_fingerprint"] is None
        assert row["lease_expires_at"] is None
        assert row["claim_token"] is None
        assert row["traceparent"] is None
        assert row["initial_traceparent_consumed_at"] is None
        assert row["evaluation_profile"] == "evaluation.candidate.v1"

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


def test_upgrade_downgrade_0012_and_0011(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0012 ↔ 0011 round-trip required by Slice 13C2A."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0011")
        inspector = inspect(engine)
        assert inspector.has_table("outbox_events")
        assert not inspector.has_table("consumer_inbox")
        assert not inspector.has_table("research_job_event_projection")

        command.upgrade(config, "20260809_0012")
        inspector = inspect(engine)
        assert inspector.has_table("consumer_inbox")
        assert inspector.has_table("research_job_event_projection")

        command.downgrade(config, "20260809_0011")
        inspector = inspect(engine)
        assert not inspector.has_table("consumer_inbox")
        assert not inspector.has_table("research_job_event_projection")
        assert inspector.has_table("outbox_events")

        command.upgrade(config, "20260809_0012")
        inspector = inspect(engine)
        assert inspector.has_table("consumer_inbox")
        assert inspector.has_table("research_job_event_projection")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0012"
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


def test_upgrade_downgrade_0013_and_0012(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0013 ↔ 0012 round-trip required by Slice 13C2B."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0012")
        inspector = inspect(engine)
        assert inspector.has_table("consumer_inbox")
        assert not inspector.has_table("consumer_dead_letters")
        assert not inspector.has_table("consumer_dead_letter_replay_attempts")

        command.upgrade(config, "20260809_0013")
        inspector = inspect(engine)
        assert inspector.has_table("consumer_dead_letters")
        assert inspector.has_table("consumer_dead_letter_replay_attempts")

        command.downgrade(config, "20260809_0012")
        inspector = inspect(engine)
        assert not inspector.has_table("consumer_dead_letters")
        assert not inspector.has_table("consumer_dead_letter_replay_attempts")
        assert inspector.has_table("consumer_inbox")

        command.upgrade(config, "20260809_0013")
        inspector = inspect(engine)
        assert inspector.has_table("consumer_dead_letters")
        assert inspector.has_table("consumer_dead_letter_replay_attempts")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260809_0013"
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


def test_upgrade_downgrade_0014_and_0013(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0014 ↔ 0013 round-trip required by Slice 15A3."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260809_0013")
        inspector = inspect(engine)
        research_job_columns = {
            column["name"] for column in inspector.get_columns("research_jobs")
        }
        outbox_columns = {
            column["name"] for column in inspector.get_columns("outbox_events")
        }
        assert "traceparent" not in research_job_columns
        assert "initial_traceparent_consumed_at" not in research_job_columns
        assert "traceparent" not in outbox_columns

        command.upgrade(config, "20260812_0014")
        inspector = inspect(engine)
        research_job_columns = {
            column["name"] for column in inspector.get_columns("research_jobs")
        }
        research_job_constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("research_jobs")
        }
        outbox_columns = {
            column["name"] for column in inspector.get_columns("outbox_events")
        }
        outbox_constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("outbox_events")
        }
        assert "traceparent" in research_job_columns
        assert "initial_traceparent_consumed_at" in research_job_columns
        assert "ck_research_jobs_traceparent_format" in research_job_constraints
        assert (
            "ck_research_jobs_initial_traceparent_consumed_pair"
            in research_job_constraints
        )
        assert "traceparent" in outbox_columns
        assert "ck_outbox_events_traceparent_format" in outbox_constraints

        command.downgrade(config, "20260809_0013")
        inspector = inspect(engine)
        research_job_columns = {
            column["name"] for column in inspector.get_columns("research_jobs")
        }
        outbox_columns = {
            column["name"] for column in inspector.get_columns("outbox_events")
        }
        assert "traceparent" not in research_job_columns
        assert "initial_traceparent_consumed_at" not in research_job_columns
        assert "traceparent" not in outbox_columns
        assert inspector.has_table("consumer_dead_letters")

        command.upgrade(config, "20260812_0014")
        inspector = inspect(engine)
        research_job_columns = {
            column["name"] for column in inspector.get_columns("research_jobs")
        }
        assert "traceparent" in research_job_columns
        assert "initial_traceparent_consumed_at" in research_job_columns
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert version == "20260812_0014"
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


def test_upgrade_downgrade_0015_and_0014(
    test_database_url: str,
    engine: Engine,
) -> None:
    """Explicit 0015 ↔ 0014 round-trip for durable evaluation-profile binding."""
    assert_safe_test_database(test_database_url)
    previous = os.environ.get("ATLAS_DATABASE_URL")
    os.environ["ATLAS_DATABASE_URL"] = test_database_url
    config = _alembic_config(test_database_url)

    try:
        _reset_public_schema(engine)
        command.upgrade(config, "20260812_0014")
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("research_jobs")}
        constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("research_jobs")
        }
        eval_constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("evaluation_runs")
        }
        assert "evaluation_profile" not in columns
        assert "ck_research_jobs_evaluation_profile_allowed" not in constraints
        assert "ck_evaluation_runs_profile" in eval_constraints

        created_at = datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO research_jobs (
                        id, question, status, created_at, updated_at,
                        started_at, finished_at, result, failure_reason,
                        idempotency_key, request_fingerprint
                    ) VALUES (
                        :id, :question, :status, :created_at, :updated_at,
                        NULL, NULL, NULL, NULL, :idempotency_key, :fingerprint
                    )
                    """
                ),
                {
                    "id": "pre-freeze-job",
                    "question": "legacy candidate job",
                    "status": "PENDING",
                    "created_at": created_at,
                    "updated_at": created_at,
                    "idempotency_key": "pre-freeze-key",
                    "fingerprint": "a" * 64,
                },
            )

        command.upgrade(config, "20260813_0015")
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("research_jobs")}
        constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("research_jobs")
        }
        assert "evaluation_profile" in columns
        assert "ck_research_jobs_evaluation_profile_allowed" in constraints
        assert "ck_research_jobs_started_has_evaluation_profile" in constraints
        with engine.connect() as connection:
            job_profile = connection.execute(
                text("SELECT evaluation_profile FROM research_jobs WHERE id = :id"),
                {"id": "pre-freeze-job"},
            ).scalar_one()
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert job_profile == "evaluation.candidate.v1"
        assert version == "20260813_0015"

        command.downgrade(config, "20260812_0014")
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("research_jobs")}
        assert "evaluation_profile" not in columns

        command.upgrade(config, "20260813_0015")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            job_profile = connection.execute(
                text("SELECT evaluation_profile FROM research_jobs WHERE id = :id"),
                {"id": "pre-freeze-job"},
            ).scalar_one()
        assert version == "20260813_0015"
        assert job_profile == "evaluation.candidate.v1"
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


def test_migrations_0001_through_0013_unchanged_versus_main() -> None:
    """Slice 15A3 must not modify migrations 0001–0013 relative to origin/main.

    Migration ``20260809_0013`` (consumer dead-letter/replay tables from Slice
    13C2B) is merged into ``origin/main`` through Pull Request #25 / Milestone
    14, so the invariant covers all thirteen prior migrations.

    Compares against the remote-tracking ref ``origin/main`` (not a local
    ``main`` branch) so GitHub Actions PR checkouts succeed when only remotes
    exist. Fails with a controlled assertion if the base ref cannot be resolved.
    """
    from tests.migration_history import (
        assert_prior_migrations_unchanged_versus_origin_main,
    )

    assert_prior_migrations_unchanged_versus_origin_main(REPO_ROOT)
