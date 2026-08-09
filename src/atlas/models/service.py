"""Model invocation service with durable ledger and LangChain-backed capabilities."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from atlas.models.contracts import (
    DraftRequest,
    DraftResult,
    DraftStructuredOutput,
    FinishOutcome,
    ModelCallMeta,
    PlanRequest,
    PlanResult,
    PlanStructuredOutput,
    ProviderId,
    RetryClass,
)
from atlas.models.errors import (
    ModelAttemptOwnershipLostError,
    ModelError,
    ModelInvocationInProgressError,
    ModelUnknownError,
)
from atlas.models.langchain import draft_prompts, invoke_structured, plan_prompts
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.model_invocation import (
    ModelInvocationRecord,
    SqlAlchemyModelInvocationRepository,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker


def build_invocation_key(
    *,
    research_job_id: str,
    node_name: str,
    prompt_version: str,
    provider: str,
    model: str,
    input_fingerprint: str,
) -> str:
    payload = "|".join(
        [
            research_job_id,
            node_name,
            prompt_version,
            provider,
            model,
            input_fingerprint,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fingerprint_payload(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ModelInvocationService:
    """Ledger-backed planner/drafter facades over a LangChain chat model.

    Atlas prevents another provider call when a validated success is committed.
    Atlas cannot guarantee exactly-once billing if the process dies after the
    provider completes a request but before the ledger commit.

    Attempt finalization is fenced with conditional updates: a physical attempt
    may leave ``STARTED`` only while still ``STARTED``, and the logical
    invocation is updated only when that attempt remains the latest active
    attempt. Late results from superseded attempts raise
    ``ModelAttemptOwnershipLostError`` and do not overwrite newer ledger state.
    """

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        chat_model: BaseChatModel,
        provider: ProviderId,
        model_name: str,
        call_timeout_seconds: float,
        repository: SqlAlchemyModelInvocationRepository | None = None,
    ) -> None:
        if provider is ProviderId.FAKE:
            raise ValueError("ModelInvocationService requires a non-fake provider")
        self._session_factory = session_factory
        self._chat_model = chat_model
        self._provider = provider
        self._model_name = model_name
        self._call_timeout_seconds = call_timeout_seconds
        self._repository = repository or SqlAlchemyModelInvocationRepository()

    def plan(
        self,
        request: PlanRequest,
        *,
        workflow_execution_id: str,
    ) -> PlanResult:
        fingerprint = fingerprint_payload(
            {
                "question": request.question,
                "prompt_version": request.prompt_version,
            }
        )
        system, user = plan_prompts(request.question)
        validated, meta = self._execute(
            node_name="plan",
            research_job_id=request.job_id,
            workflow_execution_id=workflow_execution_id,
            prompt_version=request.prompt_version,
            input_fingerprint=fingerprint,
            schema=PlanStructuredOutput,
            system_prompt=system,
            user_prompt=user,
        )
        assert isinstance(validated, PlanStructuredOutput)
        return PlanResult(tasks=validated.tasks, meta=meta)

    def draft(
        self,
        request: DraftRequest,
        *,
        workflow_execution_id: str,
    ) -> DraftResult:
        fingerprint = fingerprint_payload(
            {
                "question": request.question,
                "plan": list(request.plan),
                "findings": list(request.findings),
                "evidence_item_ids": [
                    item.evidence_item_id for item in request.evidence
                ],
                "prompt_version": request.prompt_version,
            }
        )
        system, user = draft_prompts(
            question=request.question,
            plan=list(request.plan),
            findings=list(request.findings),
            evidence=[
                {
                    "evidence_item_id": item.evidence_item_id,
                    "source_display_uri": item.source_display_uri,
                    "trust_label": item.trust_label,
                    "text": item.text,
                }
                for item in request.evidence
            ],
        )
        validated, meta = self._execute(
            node_name="draft",
            research_job_id=request.job_id,
            workflow_execution_id=workflow_execution_id,
            prompt_version=request.prompt_version,
            input_fingerprint=fingerprint,
            schema=DraftStructuredOutput,
            system_prompt=system,
            user_prompt=user,
        )
        assert isinstance(validated, DraftStructuredOutput)
        return DraftResult(
            draft=validated.draft,
            claims=list(validated.claims),
            meta=meta,
        )

    def _execute[SchemaT: BaseModel](
        self,
        *,
        node_name: str,
        research_job_id: str,
        workflow_execution_id: str,
        prompt_version: str,
        input_fingerprint: str,
        schema: type[SchemaT],
        system_prompt: str,
        user_prompt: str,
    ) -> tuple[SchemaT, ModelCallMeta]:
        invocation_key = build_invocation_key(
            research_job_id=research_job_id,
            node_name=node_name,
            prompt_version=prompt_version,
            provider=self._provider.value,
            model=self._model_name,
            input_fingerprint=input_fingerprint,
        )
        now = datetime.now(UTC)
        # Attempt deadline matches the configured provider request timeout.
        # This is not a hard wall-clock around the entire structured-invoke
        # path (validation, ledger I/O, etc.).
        deadline = now + timedelta(seconds=self._call_timeout_seconds)

        with session_scope(self._session_factory) as session:
            existing = self._repository.get_by_key(
                session,
                invocation_key,
                for_update=True,
            )
            if existing is not None and existing.status == "SUCCEEDED":
                if existing.output_json is None:
                    raise ModelUnknownError()
                meta = _meta_from_record(
                    existing,
                    provider=self._provider,
                    model=self._model_name,
                    prompt_version=prompt_version,
                )
                return schema.model_validate(existing.output_json), meta

            if existing is not None and existing.status == "IN_PROGRESS":
                if not self._can_reclaim(session, existing=existing, now=now):
                    raise ModelInvocationInProgressError()
                latest = self._repository.latest_attempt(
                    session,
                    invocation_id=existing.id,
                )
                if latest is not None and latest.status == "STARTED":
                    if not self._repository.fail_attempt(
                        session,
                        attempt_id=latest.id,
                        error_class="ModelInvocationStaleError",
                        retry_class=RetryClass.TEMPORARY.value,
                        at=now,
                    ):
                        raise ModelInvocationInProgressError()
                    if not self._repository.mark_invocation_failed_for_attempt(
                        session,
                        invocation_id=existing.id,
                        attempt_id=latest.id,
                        error_class="ModelInvocationStaleError",
                        retry_class=RetryClass.TEMPORARY.value,
                        at=now,
                    ):
                        raise ModelInvocationInProgressError()

            invocation_id = existing.id if existing is not None else str(uuid4())
            attempt_id = str(uuid4())
            try:
                if existing is None:
                    self._repository.create_invocation(
                        session,
                        invocation_id=invocation_id,
                        invocation_key=invocation_key,
                        research_job_id=research_job_id,
                        workflow_execution_id=workflow_execution_id,
                        node_name=node_name,
                        provider=self._provider.value,
                        model=self._model_name,
                        prompt_version=prompt_version,
                        input_fingerprint=input_fingerprint,
                        at=now,
                    )
                else:
                    self._repository.reopen_invocation(
                        session,
                        invocation_id=invocation_id,
                        workflow_execution_id=workflow_execution_id,
                        at=now,
                    )
                self._repository.begin_attempt(
                    session,
                    attempt_id=attempt_id,
                    invocation_id=invocation_id,
                    deadline_at=deadline,
                    at=now,
                )
            except IntegrityError as exc:
                raise ModelInvocationInProgressError() from exc

        try:
            validated, meta = invoke_structured(
                chat_model=self._chat_model,
                provider=self._provider,
                model_name=self._model_name,
                prompt_version=prompt_version,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=schema,
            )
        except ModelError as exc:
            finished = datetime.now(UTC)
            with session_scope(self._session_factory) as session:
                if not self._repository.fail_attempt(
                    session,
                    attempt_id=attempt_id,
                    error_class=type(exc).__name__,
                    retry_class=exc.retry_class.value,
                    at=finished,
                ):
                    raise ModelAttemptOwnershipLostError() from exc
                if not self._repository.mark_invocation_failed_for_attempt(
                    session,
                    invocation_id=invocation_id,
                    attempt_id=attempt_id,
                    error_class=type(exc).__name__,
                    retry_class=exc.retry_class.value,
                    at=finished,
                ):
                    raise ModelAttemptOwnershipLostError() from exc
            raise

        finished = datetime.now(UTC)
        output_json = validated.model_dump(mode="json")
        with session_scope(self._session_factory) as session:
            if not self._repository.complete_attempt(
                session,
                attempt_id=attempt_id,
                provider_request_id=meta.provider_request_id,
                input_tokens=meta.input_tokens,
                output_tokens=meta.output_tokens,
                total_tokens=meta.total_tokens,
                latency_ms=meta.latency_ms,
                estimated_cost_usd=meta.estimated_cost_usd,
                pricing_version=meta.pricing_version,
                finish_outcome=meta.finish_outcome.value,
                at=finished,
            ):
                raise ModelAttemptOwnershipLostError()
            if not self._repository.complete_invocation_for_attempt(
                session,
                invocation_id=invocation_id,
                attempt_id=attempt_id,
                output_json=output_json,
                provider_request_id=meta.provider_request_id,
                input_tokens=meta.input_tokens,
                output_tokens=meta.output_tokens,
                total_tokens=meta.total_tokens,
                latency_ms=meta.latency_ms,
                estimated_cost_usd=meta.estimated_cost_usd,
                pricing_version=meta.pricing_version,
                finish_outcome=meta.finish_outcome.value,
                at=finished,
            ):
                raise ModelAttemptOwnershipLostError()
        return validated, meta

    def _can_reclaim(
        self,
        session: Session,
        *,
        existing: ModelInvocationRecord,
        now: datetime,
    ) -> bool:
        latest = self._repository.latest_attempt(session, invocation_id=existing.id)
        if latest is None:
            return True
        if latest.status == "STARTED" and latest.deadline_at > now:
            return False
        return not self._repository.job_has_valid_claim(
            session,
            research_job_id=existing.research_job_id,
            now=now,
        )


class LedgerBackedPlanner:
    """ResearchPlanner that records invocations against a workflow execution."""

    def __init__(
        self,
        service: ModelInvocationService,
        *,
        workflow_execution_id: str,
    ) -> None:
        self._service = service
        self._workflow_execution_id = workflow_execution_id

    def plan(self, request: PlanRequest) -> PlanResult:
        return self._service.plan(
            request,
            workflow_execution_id=self._workflow_execution_id,
        )


class LedgerBackedDrafter:
    """ResearchDrafter that records invocations against a workflow execution."""

    def __init__(
        self,
        service: ModelInvocationService,
        *,
        workflow_execution_id: str,
    ) -> None:
        self._service = service
        self._workflow_execution_id = workflow_execution_id

    def draft(self, request: DraftRequest) -> DraftResult:
        return self._service.draft(
            request,
            workflow_execution_id=self._workflow_execution_id,
        )


def _meta_from_record(
    row: ModelInvocationRecord,
    *,
    provider: ProviderId,
    model: str,
    prompt_version: str,
) -> ModelCallMeta:
    return ModelCallMeta(
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        provider_request_id=row.provider_request_id,
        input_tokens=row.input_tokens,
        output_tokens=row.output_tokens,
        total_tokens=row.total_tokens,
        latency_ms=int(row.latency_ms or 0),
        estimated_cost_usd=row.estimated_cost_usd,
        pricing_version=row.pricing_version,
        finish_outcome=FinishOutcome(
            row.finish_outcome or FinishOutcome.COMPLETED.value
        ),
        retry_class=RetryClass.NONE,
        status="succeeded",
    )
