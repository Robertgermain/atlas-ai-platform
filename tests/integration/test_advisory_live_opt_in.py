"""Opt-in live advisory analysis. Never enabled in CI.

Requires ``ATLAS_ENABLE_LIVE_ADVISORY_TESTS=1``, live OpenAI credentials,
LangSmith, and the integration PostgreSQL fixture. Prompts, keys, and raw
provider exceptions are not printed.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import Engine, event, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.catalogs import (
    ADVISORY_NODE_NAME,
    FROZEN_LIVE_ADVISORY_MODEL,
    FROZEN_LIVE_ADVISORY_PROVIDER,
    FROZEN_LIVE_ADVISORY_TEMPERATURE,
)
from atlas.advisor.composition import (
    build_advisory_service,
    require_advisory_composition,
)
from atlas.advisor.contracts import AdvisoryAnalysis
from atlas.advisor.db import advisory_read_only_scope
from atlas.advisor.output_policy import validate_advisory_output
from atlas.advisor.snapshot import assemble_facts
from atlas.config.settings import Settings
from atlas.domain import ResearchJob
from atlas.evaluation.semantic_contracts import FROZEN_LIVE_SEMANTIC_TEMPERATURE
from atlas.models.composition import resolve_model_name
from atlas.observability.langsmith import (
    FLUSH_BOUND_SECONDS,
    configure_langsmith,
    reset_langsmith_for_tests,
)
from atlas.observability.langsmith.redaction import ALLOWED_METADATA_KEYS
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.advisory_snapshot import (
    SqlAlchemyAdvisorySnapshotReader,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

pytestmark = pytest.mark.skipif(
    os.environ.get("ATLAS_ENABLE_LIVE_ADVISORY_TESTS") != "1",
    reason="opt-in live advisory tests are disabled",
)

T0 = datetime(2026, 8, 14, 14, 0, 0, tzinfo=UTC)
_JOB_ID = "advisory-live-job-1"
_QUESTION = "What is Atlas?"
_OTHER_LIVE_FLAGS = (
    "ATLAS_ENABLE_LIVE_MODEL_TESTS",
    "ATLAS_ENABLE_LIVE_TOOL_TESTS",
    "ATLAS_ENABLE_LIVE_EMBEDDING_TESTS",
    "ATLAS_ENABLE_LIVE_LANGSMITH_TESTS",
    "ATLAS_ENABLE_LIVE_HELD_OUT_SEMANTIC_TESTS",
    "ATLAS_ENABLE_LIVE_SEMANTIC_GRADER_TESTS",
    "ATLAS_ENABLE_LIVE_EVALUATION_V1_WORKFLOW_TESTS",
)
_TABLES = (
    "research_jobs",
    "workflow_executions",
    "workflow_node_executions",
    "model_invocations",
    "tool_invocations",
    "evaluation_runs",
    "policy_decisions",
    "human_review_decisions",
    "outbox_events",
    "consumer_inbox",
    "research_job_event_projection",
    "consumer_dead_letters",
)
_CANARIES = ("sk-", "lsv2_", "http://", "https://", "curl ", "kubectl ", _QUESTION)
_RUN_SELECT = (
    "id",
    "name",
    "run_type",
    "trace_id",
    "parent_run_id",
    "inputs",
    "outputs",
    "extra",
)
_LIVE_QUERY_DEADLINE_SECONDS = 30.0
_LIVE_QUERY_INTERVAL_SECONDS = 1.0


def _counts(session: Session) -> dict[str, int]:
    return {
        table: int(session.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())
        for table in _TABLES
    }


def _job_hash(session: Session, job_id: str) -> str:
    return str(
        session.execute(
            text(
                "SELECT md5(CAST((id, status, question, result, failure_reason, "
                "repair_count, job_retry_count, evaluation_attempt_count, "
                "evaluation_profile, continuation_mode, updated_at) AS text)) "
                "FROM research_jobs WHERE id = :id"
            ),
            {"id": job_id},
        ).scalar_one()
    )


def _run_metadata(run: object) -> dict[str, object]:
    extra = getattr(run, "extra", None) or {}
    merged: dict[str, object] = {}
    if isinstance(extra, Mapping):
        nested = extra.get("metadata")
        if isinstance(nested, Mapping):
            merged.update(dict(nested))
    direct = getattr(run, "metadata", None)
    if isinstance(direct, Mapping):
        merged.update(dict(direct))
    return merged


def _walk_strings(value: object) -> Iterator[str]:
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, (bool, int, float)):
        yield str(value)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _walk_strings(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        for item in value:
            yield from _walk_strings(item)
        return


def _payload_contains(payload: object, needle: str) -> bool:
    lowered = needle.casefold()
    for item in _walk_strings(payload):
        if lowered in item.casefold():
            return True
    return False


def _ensure_live_project(client: Any, project_name: str) -> None:
    from langsmith.utils import LangSmithConflictError, LangSmithNotFoundError

    try:
        client.read_project(project_name=project_name)
    except LangSmithNotFoundError:
        try:
            client.create_project(project_name=project_name)
        except LangSmithConflictError:
            client.read_project(project_name=project_name)


def _wait_for_advisory_root(client: Any, *, project_name: str, analysis_id: str) -> Any:
    from langsmith.utils import LangSmithNotFoundError

    metadata_filter = (
        f'has(metadata, \'{{"atlas.advisory_analysis_id": "{analysis_id}"}}\')'
    )
    deadline = time.monotonic() + _LIVE_QUERY_DEADLINE_SECONDS
    while True:
        try:
            roots = list(
                client.list_runs(
                    project_name=project_name,
                    is_root=True,
                    filter=metadata_filter,
                    select=list(_RUN_SELECT),
                    limit=20,
                )
            )
        except LangSmithNotFoundError:
            roots = []
        matched = [
            run
            for run in roots
            if _run_metadata(run).get("atlas.advisory_analysis_id") == analysis_id
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            pytest.fail("expected exactly one live advisory root")
        if time.monotonic() >= deadline:
            pytest.fail("timed out waiting for the live advisory LangSmith root")
        time.sleep(_LIVE_QUERY_INTERVAL_SECONDS)


def test_live_advisory_analyzes_seeded_research_job(
    engine: Engine,
    session_factory: sessionmaker[Session],
) -> None:
    for flag in _OTHER_LIVE_FLAGS:
        assert os.environ.get(flag) != "1"
    assert os.environ.get("ATLAS_ENABLE_LIVE_ADVISORY_TESTS") == "1"

    settings = Settings()
    require_advisory_composition(settings)
    assert settings.advisory_mode == "live"
    assert settings.model_provider == FROZEN_LIVE_ADVISORY_PROVIDER
    assert resolve_model_name(settings) == FROZEN_LIVE_ADVISORY_MODEL
    assert FROZEN_LIVE_ADVISORY_TEMPERATURE == 0.0
    assert FROZEN_LIVE_SEMANTIC_TEMPERATURE == 0.0

    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create(_JOB_ID, _QUESTION, at=T0)
    with session_scope(session_factory) as session:
        repo.add(
            session,
            job,
            idempotency_key=f"key-{_JOB_ID}",
            request_fingerprint="c" * 64,
        )

    with session_scope(session_factory) as session:
        before_counts = _counts(session)
        before_hash = _job_hash(session, _JOB_ID)
        inspector = inspect(session.get_bind())
        assert "advisory_analyses" not in inspector.get_table_names()

    versions = Path("alembic/versions")
    for path in versions.glob("*.py"):
        text_body = path.read_text(encoding="utf-8")
        assert "advisory_analyses" not in text_body

    analysis_id = str(uuid4())
    os.environ["LANGCHAIN_PROJECT"] = settings.langsmith_project
    handle = configure_langsmith(settings)
    assert handle.enabled is True
    assert handle.client is not None
    _ensure_live_project(handle.client, settings.langsmith_project)
    assert FLUSH_BOUND_SECONDS == 5.0

    checked_out: set[int] = set()
    order: list[str] = []

    def _checkout(
        dbapi_conn: object,
        connection_rec: object,
        connection_proxy: object,
    ) -> None:
        del connection_rec, connection_proxy
        checked_out.add(id(dbapi_conn))
        order.append("checkout")

    def _checkin(dbapi_conn: object, connection_rec: object) -> None:
        del connection_rec
        checked_out.discard(id(dbapi_conn))
        order.append("checkin")

    event.listen(engine, "checkout", _checkout)
    event.listen(engine, "checkin", _checkin)
    try:
        service = build_advisory_service(settings, session_factory=session_factory)
        chat_model = getattr(service._analyst, "_chat_model", None)
        assert chat_model is not None
        assert float(chat_model.temperature) == 0.0
        original_analyze = service._analyst.analyze

        def _tracked_analyze(facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
            order.append("analyze")
            assert checked_out == set()
            return original_analyze(facts, analysis_id=analysis_id)

        service._analyst.analyze = _tracked_analyze  # type: ignore[method-assign]
        service._analysis_id_factory = lambda: analysis_id
        envelope = service.analyze_job(_JOB_ID)
    finally:
        event.remove(engine, "checkout", _checkout)
        event.remove(engine, "checkin", _checkin)
        handle.close()

    assert "checkout" in order
    assert order.index("checkin") < order.index("analyze")
    assert envelope.research_job_id == _JOB_ID
    assert envelope.analysis_id == analysis_id
    assert isinstance(envelope.analysis, AdvisoryAnalysis)
    assert envelope.analysis.schema_version == "advisory.analysis.v1"
    assert len(envelope.analysis.recommendations) >= 1
    kinds = {item.action_kind for item in envelope.analysis.recommendations}
    assert kinds <= {
        "investigate",
        "inspect_state",
        "review_runbook",
        "collect_more_telemetry",
    }

    reader = SqlAlchemyAdvisorySnapshotReader()
    with advisory_read_only_scope(session_factory) as session:
        facts = assemble_facts(reader.load(session, _JOB_ID))
    validate_advisory_output(facts, envelope.analysis)
    allowed_ids = {item.signal_id for item in facts.signals}
    cited: set[str] = set()
    for hypothesis in envelope.analysis.hypotheses:
        cited.update(hypothesis.signal_ids)
    for recommendation in envelope.analysis.recommendations:
        cited.update(recommendation.signal_ids)
    assert cited <= allowed_ids

    encoded = json.dumps(
        envelope.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    for canary in _CANARIES:
        assert canary.casefold() not in encoded.casefold()

    client = handle.client
    assert client is not None
    try:
        root = _wait_for_advisory_root(
            client,
            project_name=settings.langsmith_project,
            analysis_id=analysis_id,
        )
        assert root.name == "atlas.advisor"
        from langsmith.utils import LangSmithNotFoundError

        try:
            runs = list(
                client.list_runs(
                    project_name=settings.langsmith_project,
                    trace_id=root.trace_id,
                    select=list(_RUN_SELECT),
                    limit=20,
                )
            )
        except LangSmithNotFoundError:
            pytest.fail("live advisory trace was not readable")
        names = {run.name for run in runs}
        assert "atlas.advisor" in names
        assert ADVISORY_NODE_NAME in names
        advise = next(run for run in runs if run.name == ADVISORY_NODE_NAME)
        assert str(advise.parent_run_id) == str(root.id)
        for run in runs:
            metadata = _run_metadata(run)
            assert metadata.get("atlas.advisory_analysis_id") == analysis_id
            atlas_keys = {
                key
                for key in metadata
                if key.startswith("atlas.") or key == "error.class"
            }
            assert atlas_keys <= set(ALLOWED_METADATA_KEYS)
            sdk_keys = set(metadata) - set(ALLOWED_METADATA_KEYS)
            assert all(key.startswith("ls_") for key in sdk_keys)
            for canary in _CANARIES:
                assert not _payload_contains(getattr(run, "inputs", None), canary)
                assert not _payload_contains(getattr(run, "outputs", None), canary)
                assert not _payload_contains(metadata, canary)
    finally:
        reset_langsmith_for_tests()

    with session_scope(session_factory) as session:
        after_counts = _counts(session)
        after_hash = _job_hash(session, _JOB_ID)
        assert after_counts == before_counts
        assert after_hash == before_hash
        assert after_counts["model_invocations"] == before_counts["model_invocations"]
