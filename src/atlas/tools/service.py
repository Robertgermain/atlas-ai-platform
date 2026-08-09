"""Ledger-backed tool invocation service with fencing and retry."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from atlas.persistence.db import session_scope
from atlas.persistence.repositories.tool_invocation import (
    SqlAlchemyToolInvocationRepository,
    ToolInvocationRecord,
)
from atlas.tools.contracts import (
    TOOL_POLICY_VERSION,
    TOOL_VERSION,
    ToolCallContext,
    ToolId,
    ToolInvocationResult,
    ToolOrigin,
    ToolProviderId,
    ToolResultMeta,
    ToolRetryClass,
)
from atlas.tools.errors import (
    ToolAttemptOwnershipLostError,
    ToolAuthConfigError,
    ToolBudgetExhaustedError,
    ToolContentRejectedError,
    ToolError,
    ToolInvalidRequestError,
    ToolInvocationInProgressError,
    ToolPermissionDeniedError,
    ToolRateLimitedError,
    ToolSsrfBlockedError,
    ToolTemporaryError,
    ToolTimeoutError,
    ToolUnknownError,
)
from atlas.tools.ports import ResearchTool
from atlas.tools.registry import NodePermissionPolicy, ToolRegistry

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker


TRANSIENT_RETRY_CLASSES = frozenset(
    {
        ToolRetryClass.TIMEOUT,
        ToolRetryClass.RATE_LIMITED,
        ToolRetryClass.TEMPORARY,
    }
)


def fingerprint_payload(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_workflow_invocation_key(
    *,
    research_job_id: str,
    node_name: str,
    tool_id: str,
    tool_version: str,
    provider: str,
    input_fingerprint: str,
    tool_policy_version: str,
) -> str:
    payload = "|".join(
        [
            "WORKFLOW",
            research_job_id,
            node_name,
            tool_id,
            tool_version,
            provider,
            input_fingerprint,
            tool_policy_version,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_mcp_invocation_key(
    *,
    actor_id: str,
    tool_id: str,
    tool_version: str,
    provider: str,
    input_fingerprint: str,
    tool_policy_version: str,
) -> str:
    payload = "|".join(
        [
            "MCP",
            actor_id,
            tool_id,
            tool_version,
            provider,
            input_fingerprint,
            tool_policy_version,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ToolBudgets:
    """Orchestration budgets for research-node tool work."""

    def __init__(
        self,
        *,
        max_logical_calls: int,
        max_attempts_per_call: int,
        attempt_timeout_seconds: float,
        node_deadline_seconds: float,
        slack_seconds: float = 0.5,
    ) -> None:
        self.max_logical_calls = max_logical_calls
        self.max_attempts_per_call = max_attempts_per_call
        self.attempt_timeout_seconds = attempt_timeout_seconds
        self.node_deadline_seconds = node_deadline_seconds
        self.slack_seconds = slack_seconds
        self.logical_calls_used = 0
        self.node_started_at = time.monotonic()

    def remaining_seconds(self) -> float:
        elapsed = time.monotonic() - self.node_started_at
        return self.node_deadline_seconds - elapsed

    def assert_can_start_attempt(self) -> None:
        needed = self.attempt_timeout_seconds + self.slack_seconds
        if self.remaining_seconds() < needed:
            raise ToolBudgetExhaustedError("research node tool budget exhausted")

    def assert_can_start_logical_call(self) -> None:
        if self.logical_calls_used >= self.max_logical_calls:
            raise ToolBudgetExhaustedError("logical tool call budget exhausted")
        self.assert_can_start_attempt()

    def record_logical_call(self) -> None:
        self.logical_calls_used += 1


class ToolInvocationService:
    """Authorize, budget, ledger, retry, and invoke governed research tools."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        registry: ToolRegistry,
        policy: NodePermissionPolicy,
        provider_by_tool: dict[ToolId, ToolProviderId],
        budgets: ToolBudgets | None = None,
        repository: SqlAlchemyToolInvocationRepository | None = None,
        max_attempts_per_call: int = 2,
        attempt_timeout_seconds: float = 8.0,
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._policy = policy
        self._provider_by_tool = dict(provider_by_tool)
        self._budgets = budgets
        self._repository = repository or SqlAlchemyToolInvocationRepository()
        self._max_attempts_per_call = max_attempts_per_call
        self._attempt_timeout_seconds = attempt_timeout_seconds

    def invoke(
        self,
        *,
        tool_id: ToolId,
        raw_input: dict[str, object],
        context: ToolCallContext,
    ) -> ToolInvocationResult:
        self._policy.assert_allowed(
            origin=context.origin,
            tool_id=tool_id,
            node_name=context.node_name,
        )
        if self._budgets is not None:
            self._budgets.assert_can_start_logical_call()

        tool = self._registry.get(tool_id)
        provider = self._provider_by_tool[tool_id]
        input_fingerprint = fingerprint_payload(
            {"tool_id": tool_id.value, "input": raw_input}
        )
        invocation_key = self._build_key(
            context=context,
            tool_id=tool_id,
            provider=provider,
            input_fingerprint=input_fingerprint,
        )

        # Replay SUCCEEDED without counting a new logical call against budgets
        # after reclaim paths; first observation of success still records usage.
        replay = self._try_replay(invocation_key)
        if replay is not None:
            return replay

        if self._budgets is not None:
            self._budgets.record_logical_call()

        last_error: ToolError | None = None
        for attempt_index in range(self._max_attempts_per_call):
            if attempt_index > 0:
                if self._budgets is not None:
                    self._budgets.assert_can_start_attempt()
                if (
                    last_error is None
                    or last_error.retry_class not in TRANSIENT_RETRY_CLASSES
                ):
                    break

            try:
                return self._execute_once(
                    tool=tool,
                    tool_id=tool_id,
                    provider=provider,
                    raw_input=raw_input,
                    context=context,
                    invocation_key=invocation_key,
                    input_fingerprint=input_fingerprint,
                )
            except ToolError as exc:
                last_error = exc
                if (
                    attempt_index + 1 >= self._max_attempts_per_call
                    or exc.retry_class not in TRANSIENT_RETRY_CLASSES
                ):
                    raise
        assert last_error is not None
        raise last_error

    def _try_replay(self, invocation_key: str) -> ToolInvocationResult | None:
        with session_scope(self._session_factory) as session:
            existing = self._repository.get_by_key(
                session,
                invocation_key,
                for_update=True,
            )
            if existing is None or existing.status != "SUCCEEDED":
                return None
            if existing.output_summary_json is None:
                raise ToolUnknownError("succeeded invocation missing output")
            finding = str(existing.output_summary_json.get("finding_text") or "")
            meta = ToolResultMeta(
                tool_id=ToolId(existing.tool_id),
                provider=ToolProviderId(existing.provider),
                tool_version=existing.tool_version,
                tool_policy_version=existing.tool_policy_version,
                latency_ms=int(existing.latency_ms or 0),
                status="succeeded",
                retry_class=ToolRetryClass.NONE,
                content_digest=existing.content_digest,
                byte_length=existing.byte_length,
            )
            return ToolInvocationResult(
                output=dict(existing.output_summary_json.get("output") or {}),
                meta=meta,
                finding_text=finding,
                invocation_id=existing.id,
            )

    def _execute_once(
        self,
        *,
        tool: ResearchTool,
        tool_id: ToolId,
        provider: ToolProviderId,
        raw_input: dict[str, object],
        context: ToolCallContext,
        invocation_key: str,
        input_fingerprint: str,
    ) -> ToolInvocationResult:
        now = datetime.now(UTC)
        deadline = now + timedelta(seconds=self._attempt_timeout_seconds)
        with session_scope(self._session_factory) as session:
            existing = self._repository.get_by_key(
                session,
                invocation_key,
                for_update=True,
            )
            if existing is not None and existing.status == "SUCCEEDED":
                if existing.output_summary_json is None:
                    raise ToolUnknownError()
                finding = str(existing.output_summary_json.get("finding_text") or "")
                return ToolInvocationResult(
                    output=dict(existing.output_summary_json.get("output") or {}),
                    meta=ToolResultMeta(
                        tool_id=ToolId(existing.tool_id),
                        provider=ToolProviderId(existing.provider),
                        tool_version=existing.tool_version,
                        tool_policy_version=existing.tool_policy_version,
                        latency_ms=int(existing.latency_ms or 0),
                        status="succeeded",
                        retry_class=ToolRetryClass.NONE,
                        content_digest=existing.content_digest,
                        byte_length=existing.byte_length,
                    ),
                    finding_text=finding,
                    invocation_id=existing.id,
                )

            if existing is not None and existing.status == "IN_PROGRESS":
                if not self._can_reclaim(session, existing=existing, now=now):
                    raise ToolInvocationInProgressError()
                latest = self._repository.latest_attempt(
                    session,
                    invocation_id=existing.id,
                )
                if latest is not None and latest.status == "STARTED":
                    if not self._repository.fail_attempt(
                        session,
                        attempt_id=latest.id,
                        error_class="ToolInvocationStaleError",
                        retry_class=ToolRetryClass.TEMPORARY.value,
                        at=now,
                    ):
                        raise ToolInvocationInProgressError()
                    if not self._repository.mark_invocation_failed_for_attempt(
                        session,
                        invocation_id=existing.id,
                        attempt_id=latest.id,
                        error_class="ToolInvocationStaleError",
                        retry_class=ToolRetryClass.TEMPORARY.value,
                        at=now,
                    ):
                        raise ToolInvocationInProgressError()

            invocation_id = existing.id if existing is not None else str(uuid4())
            attempt_id = str(uuid4())
            try:
                if existing is None:
                    self._repository.create_invocation(
                        session,
                        invocation_id=invocation_id,
                        invocation_key=invocation_key,
                        origin=context.origin.value,
                        research_job_id=context.research_job_id,
                        workflow_execution_id=context.workflow_execution_id,
                        node_name=context.node_name,
                        workflow_node_attempt=context.workflow_node_attempt,
                        actor_id=context.actor_id,
                        tool_id=tool_id.value,
                        tool_version=context.tool_version,
                        provider=provider.value,
                        tool_policy_version=context.tool_policy_version,
                        input_fingerprint=input_fingerprint,
                        at=now,
                    )
                else:
                    self._repository.reopen_invocation(
                        session,
                        invocation_id=invocation_id,
                        workflow_execution_id=context.workflow_execution_id,
                        workflow_node_attempt=context.workflow_node_attempt,
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
                raise ToolInvocationInProgressError() from exc

        try:
            result = tool.invoke(raw_input, context=context)
        except ToolError as exc:
            finished = datetime.now(UTC)
            with session_scope(self._session_factory) as session:
                if not self._repository.fail_attempt(
                    session,
                    attempt_id=attempt_id,
                    error_class=type(exc).__name__,
                    retry_class=exc.retry_class.value,
                    at=finished,
                ):
                    raise ToolAttemptOwnershipLostError() from exc
                if not self._repository.mark_invocation_failed_for_attempt(
                    session,
                    invocation_id=invocation_id,
                    attempt_id=attempt_id,
                    error_class=type(exc).__name__,
                    retry_class=exc.retry_class.value,
                    at=finished,
                ):
                    raise ToolAttemptOwnershipLostError() from exc
            raise
        except Exception as exc:
            wrapped = ToolUnknownError("tool adapter failed")
            finished = datetime.now(UTC)
            with session_scope(self._session_factory) as session:
                if not self._repository.fail_attempt(
                    session,
                    attempt_id=attempt_id,
                    error_class=type(wrapped).__name__,
                    retry_class=wrapped.retry_class.value,
                    at=finished,
                ):
                    raise ToolAttemptOwnershipLostError() from exc
                if not self._repository.mark_invocation_failed_for_attempt(
                    session,
                    invocation_id=invocation_id,
                    attempt_id=attempt_id,
                    error_class=type(wrapped).__name__,
                    retry_class=wrapped.retry_class.value,
                    at=finished,
                ):
                    raise ToolAttemptOwnershipLostError() from exc
            raise wrapped from exc

        finished = datetime.now(UTC)
        summary = {
            "output": result.output,
            "finding_text": result.finding_text,
        }
        with session_scope(self._session_factory) as session:
            if not self._repository.complete_attempt(
                session,
                attempt_id=attempt_id,
                latency_ms=result.meta.latency_ms,
                at=finished,
            ):
                raise ToolAttemptOwnershipLostError()
            if not self._repository.complete_invocation_for_attempt(
                session,
                invocation_id=invocation_id,
                attempt_id=attempt_id,
                output_summary_json=summary,
                content_digest=result.meta.content_digest,
                byte_length=result.meta.byte_length,
                latency_ms=result.meta.latency_ms,
                at=finished,
            ):
                raise ToolAttemptOwnershipLostError()
        return result.model_copy(update={"invocation_id": invocation_id})

    def _build_key(
        self,
        *,
        context: ToolCallContext,
        tool_id: ToolId,
        provider: ToolProviderId,
        input_fingerprint: str,
    ) -> str:
        if context.origin is ToolOrigin.MCP:
            if not context.actor_id:
                raise ToolAuthConfigError("MCP calls require actor_id")
            return build_mcp_invocation_key(
                actor_id=context.actor_id,
                tool_id=tool_id.value,
                tool_version=context.tool_version,
                provider=provider.value,
                input_fingerprint=input_fingerprint,
                tool_policy_version=context.tool_policy_version,
            )
        if not context.research_job_id or not context.node_name:
            raise ToolInvalidRequestError("workflow calls require job and node")
        if not context.workflow_execution_id:
            raise ToolInvalidRequestError(
                "workflow calls require workflow_execution_id"
            )
        return build_workflow_invocation_key(
            research_job_id=context.research_job_id,
            node_name=context.node_name,
            tool_id=tool_id.value,
            tool_version=context.tool_version,
            provider=provider.value,
            input_fingerprint=input_fingerprint,
            tool_policy_version=context.tool_policy_version,
        )

    def _can_reclaim(
        self,
        session: Session,
        *,
        existing: ToolInvocationRecord,
        now: datetime,
    ) -> bool:
        latest = self._repository.latest_attempt(session, invocation_id=existing.id)
        if latest is None:
            return True
        if latest.status == "STARTED" and latest.deadline_at > now:
            return False
        if existing.origin == ToolOrigin.MCP.value:
            # MCP has no research-job claim; reclaim when attempt deadline expired.
            return latest.deadline_at <= now
        return not self._repository.job_has_valid_claim(
            session,
            research_job_id=existing.research_job_id or "",
            now=now,
        )


# Re-export error types used by adapters for mypy convenience in runners.
__all__ = [
    "ToolBudgets",
    "ToolInvocationService",
    "build_mcp_invocation_key",
    "build_workflow_invocation_key",
    "fingerprint_payload",
    "ToolAuthConfigError",
    "ToolBudgetExhaustedError",
    "ToolContentRejectedError",
    "ToolInvalidRequestError",
    "ToolPermissionDeniedError",
    "ToolRateLimitedError",
    "ToolSsrfBlockedError",
    "ToolTemporaryError",
    "ToolTimeoutError",
    "ToolUnknownError",
    "TOOL_POLICY_VERSION",
    "TOOL_VERSION",
]
