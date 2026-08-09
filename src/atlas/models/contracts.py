"""Typed contracts for Atlas model-backed planning and drafting."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator


class ProviderId(StrEnum):
    FAKE = "fake"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"


class RetryClass(StrEnum):
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    TEMPORARY = "temporary"
    AUTH_CONFIG = "auth_config"
    INVALID_REQUEST = "invalid_request"
    INVALID_STRUCTURED_OUTPUT = "invalid_structured_output"
    REFUSAL = "refusal"
    UNKNOWN = "unknown"
    NONE = "none"


class FinishOutcome(StrEnum):
    COMPLETED = "completed"
    REFUSED = "refused"
    FAILED = "failed"


class PlanStructuredOutput(BaseModel):
    """Provider-facing structured plan schema (exactly three tasks)."""

    tasks: Annotated[list[str], Field(min_length=3, max_length=3)]

    @field_validator("tasks")
    @classmethod
    def validate_tasks(cls, value: list[str]) -> list[str]:
        cleaned = [task.strip() for task in value]
        if len(cleaned) != 3 or any(not task for task in cleaned):
            raise ValueError("plan must contain exactly three non-empty tasks")
        return cleaned


class DraftStructuredOutput(BaseModel):
    """Provider-facing structured draft schema."""

    draft: str = Field(min_length=1)

    @field_validator("draft")
    @classmethod
    def validate_draft(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("draft must be non-empty")
        return cleaned


class PlanRequest(BaseModel):
    job_id: str
    question: str
    prompt_version: str


class DraftRequest(BaseModel):
    job_id: str
    question: str
    plan: Annotated[list[str], Field(min_length=3, max_length=3)]
    findings: list[str]
    prompt_version: str


class ModelCallMeta(BaseModel):
    provider: ProviderId
    model: str
    prompt_version: str
    provider_request_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    latency_ms: int
    estimated_cost_usd: float | None = None
    pricing_version: str | None = None
    finish_outcome: FinishOutcome
    retry_class: RetryClass = RetryClass.NONE
    status: Literal["succeeded", "failed"] = "succeeded"


class PlanResult(BaseModel):
    tasks: Annotated[list[str], Field(min_length=3, max_length=3)]
    meta: ModelCallMeta


class DraftResult(BaseModel):
    draft: str
    meta: ModelCallMeta
