"""Verify Alembic can migrate an empty test database to head."""

from __future__ import annotations

from sqlalchemy import Engine, inspect, text


def test_empty_database_migrates_to_head(engine: Engine) -> None:
    inspector = inspect(engine)
    assert inspector.has_table("research_jobs")

    constraint_names = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("research_jobs")
    }
    assert "ck_research_jobs_status" in constraint_names
    assert "ck_research_jobs_status_fields" in constraint_names

    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    assert version == "20260808_0001"
