"""Opt-in live held-out semantic calibration (never enabled in CI).

Requires ``ATLAS_ENABLE_LIVE_HELD_OUT_SEMANTIC_TESTS=1``, a non-fake model
provider with credentials, ``ATLAS_LANGSMITH_API_KEY``, and the integration
PostgreSQL fixture. Human labels are never rewritten. Unique LangSmith
experiments are retained for manual inspection. Prompts, evidence, keys, and
raw provider exceptions are not printed.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from langsmith import evaluate
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.config.settings import Settings
from atlas.domain.research_job import ResearchJob
from atlas.evaluation.composition import require_semantic_grader_mode
from atlas.evaluation.llm_grader import dimension_from_semantic_output
from atlas.evaluation.semantic_contracts import SemanticGroundednessOutput
from atlas.models.composition import resolve_model_name
from atlas.models.contracts import ProviderId
from atlas.models.errors import ModelError
from atlas.models.langchain import build_chat_model
from atlas.models.service import ModelInvocationService
from atlas.observability.langsmith import (
    configure_langsmith,
    require_langsmith_for_live_ai,
    reset_langsmith_for_tests,
)
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from tests.evaluation.held_out_semantic_support import (
    DATASET_NAME,
    LIVE_FLAG,
    assemble_case,
    classify_model_error,
    ensure_held_out_dataset,
    load_held_out_dataset,
    metadata_label_compare,
    prediction_record,
    sanitized_error_class,
    summarize_predictions,
    target_from_recorded_predictions,
    upsert_held_out_examples,
)

pytestmark = pytest.mark.skipif(
    os.environ.get(LIVE_FLAG) != "1"
    or (os.environ.get("ATLAS_MODEL_PROVIDER") or "fake").strip() == "fake"
    or not (os.environ.get("ATLAS_LANGSMITH_API_KEY") or "").strip(),
    reason=(
        "Live held-out semantic calibration requires "
        f"{LIVE_FLAG}=1, a non-fake ATLAS_MODEL_PROVIDER, and "
        "ATLAS_LANGSMITH_API_KEY"
    ),
)


def _seed_job_and_execution(
    session_factory: sessionmaker[Session],
    *,
    job_id: str,
) -> str:
    repo = SqlAlchemyResearchJobRepository()
    workflow = SqlAlchemyWorkflowRepository()
    now = datetime.now(UTC)
    fingerprint = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    with session_scope(session_factory) as session:
        job = ResearchJob.create(
            id=job_id,
            question="held-out semantic calibration",
            at=now,
        )
        job.start(at=now)
        repo.add(
            session,
            job,
            idempotency_key=f"idem-{job_id}"[:128],
            request_fingerprint=fingerprint,
        )
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET claim_token = :token, lease_expires_at = :lease
                WHERE id = :id
                """
            ),
            {
                "token": "b" * 64,
                "lease": now + timedelta(seconds=90),
                "id": job_id,
            },
        )
        return workflow.create_execution(
            session,
            research_job_id=job_id,
            thread_id=job_id,
            at=now,
        )


def _sanitized_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "supported_precision": summary["supported"]["precision"],
        "supported_recall": summary["supported"]["recall"],
        "macro_f1": summary["macro_f1"],
        "report_f1": summary["report"]["f1"],
        "score_mae": summary["score_mae"],
        "availability": summary["availability"],
        "safety_boundary_failure": summary["safety_boundary_failure"],
        "automated_criteria_met": summary["automated_criteria_met"],
        "systematic_review_status": summary["systematic_review_status"],
        "promotion_criteria_met": summary["promotion_criteria_met"],
        "does_not_freeze_evaluation_v1": summary["does_not_freeze_evaluation_v1"],
        "disagreement_ids": [
            {"case_id": item["case_id"], "reason": item["reason"]}
            for item in summary["disagreements"]
        ],
    }


def test_live_held_out_semantic_calibration(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_SEMANTIC_GRADER_MODE", "live")
    dataset = load_held_out_dataset()
    frozen = [
        (
            case.id,
            [item.support for item in case.human.claims],
            case.human.report_passed,
        )
        for case in dataset.cases
    ]
    settings = Settings()
    require_semantic_grader_mode(settings)
    require_langsmith_for_live_ai(settings)
    handle = None
    try:
        handle = configure_langsmith(settings)
        if not handle.enabled or handle.client is None:
            pytest.fail("LangSmith client did not initialize")
        chat_model = build_chat_model(settings)
        records: dict[str, dict[str, Any]] = {}
        ordered: list[dict[str, Any]] = []
        for case in dataset.cases:
            if not case.claims:
                mapped = dimension_from_semantic_output(
                    SemanticGroundednessOutput(claims=[])
                )
                record = prediction_record(
                    case_id=case.id,
                    output_claims=[],
                    predicted_passed=mapped.passed,
                )
            else:
                job_id = f"hos-live-{case.id}-{uuid4().hex[:8]}"
                request = assemble_case(case, job_id=job_id)
                execution_id = _seed_job_and_execution(session_factory, job_id=job_id)
                service = ModelInvocationService(
                    session_factory=session_factory,
                    chat_model=chat_model,
                    provider=ProviderId(settings.model_provider),
                    model_name=resolve_model_name(settings),
                    call_timeout_seconds=settings.model_call_timeout_seconds,
                )
                try:
                    output, _meta = service.evaluate_semantic(
                        request,
                        workflow_execution_id=execution_id,
                    )
                    mapped = dimension_from_semantic_output(output)
                    record = prediction_record(
                        case_id=case.id,
                        output_claims=list(output.claims),
                        predicted_passed=mapped.passed,
                    )
                except ModelError as exc:
                    record = prediction_record(
                        case_id=case.id,
                        outcome=classify_model_error(exc),
                    )
                    _ = sanitized_error_class(exc)
            records[case.id] = record
            ordered.append(record)

        summary = summarize_predictions(dataset.cases, ordered)
        reloaded = load_held_out_dataset()
        assert [
            (
                case.id,
                [item.support for item in case.human.claims],
                case.human.report_passed,
            )
            for case in reloaded.cases
        ] == frozen
        if summary["safety_boundary_failure"]:
            pytest.fail("held-out prompt-injection boundary failed")
        client = handle.client
        ensure_held_out_dataset(client)
        upsert_held_out_examples(client, dataset)
        prefix = f"atlas.15c1.heldout.{uuid4().hex[:12]}"
        results = evaluate(
            target_from_recorded_predictions(records),
            data=DATASET_NAME,
            evaluators=[metadata_label_compare],
            client=client,
            experiment_prefix=prefix,
            max_concurrency=0,
            upload_results=True,
        )
        rows = list(results)
        assert len(rows) == len(dataset.cases)
        sanitized = _sanitized_summary(summary)
        assert sanitized["does_not_freeze_evaluation_v1"] is True
        assert sanitized["systematic_review_status"] == "pending"
        assert sanitized["promotion_criteria_met"] is not True
    finally:
        if handle is not None:
            handle.close()
        reset_langsmith_for_tests()
