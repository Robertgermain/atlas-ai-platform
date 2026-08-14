"""Bounded semantic-groundedness input/output contracts (Slice 15C1).

Live-provider input is claims plus their job-linked evidence excerpts only.
Bodies, questions, drafts, plans, findings, URLs, logs, and secrets are
never part of this contract.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from atlas.evaluation.aggregation import SEMANTIC_PASS_THRESHOLD
from atlas.evidence.bounds import (
    MAX_CLAIMS_PER_DRAFT,
    MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT,
    MAX_EVIDENCE_IDS_PER_CLAIM,
    MAX_EVIDENCE_ITEMS_TO_DRAFTER,
)

SEMANTIC_PROMPT_VERSION: Literal["semantic_groundedness.v1"] = (
    "semantic_groundedness.v1"
)
LIVE_SEMANTIC_GRADER_VERSION: Literal["semantic_groundedness.v1"] = (
    "semantic_groundedness.v1"
)
FAKE_SEMANTIC_GRADER_VERSION: Literal["fake.llm.v1"] = "fake.llm.v1"
SKIPPED_SEMANTIC_GRADER_VERSION: Literal["skipped"] = "skipped"

#: Frozen live semantic configuration for ``evaluation.v1``.
#: ``gpt-4o-mini`` is a provider alias: Atlas freezes provider/model/temperature
#: configuration, not immutable provider model weights.
FROZEN_LIVE_SEMANTIC_PROVIDER: Literal["openai"] = "openai"
FROZEN_LIVE_SEMANTIC_MODEL: Literal["gpt-4o-mini"] = "gpt-4o-mini"
FROZEN_LIVE_SEMANTIC_TEMPERATURE: float = 0.0

SemanticGraderVersion = Literal[
    "skipped",
    "fake.llm.v1",
    "semantic_groundedness.v1",
]
SemanticPromptVersion = Literal["skipped", "semantic_groundedness.v1"]

MAX_SEMANTIC_CLAIM_CODE_POINTS = 500
MAX_SEMANTIC_USER_PAYLOAD_BYTES = 16_384

UNTRUSTED_CLAIMS_BEGIN = "BEGIN_UNTRUSTED_CLAIMS"
UNTRUSTED_CLAIMS_END = "END_UNTRUSTED_CLAIMS"
UNTRUSTED_EVIDENCE_BEGIN = "BEGIN_UNTRUSTED_EVIDENCE"
UNTRUSTED_EVIDENCE_END = "END_UNTRUSTED_EVIDENCE"

SEMANTIC_FAILURE_UNSUPPORTED = "SEMANTIC_UNSUPPORTED"
SEMANTIC_FAILURE_UNCLEAR = "SEMANTIC_UNCLEAR"
SEMANTIC_FAILURE_WEAK = "SEMANTIC_GROUNDEDNESS_WEAK"

# Closed per-claim mapping. Inclusive lower / exclusive upper; no gaps.
# ``supported`` begins at the overall pass threshold so a fully-supported
# claim set cannot fail the mean.
SEMANTIC_UNCLEAR_INCLUSIVE_LOWER = 0.40
SemanticSupportLabel = Literal["supported", "unsupported", "unclear"]


def support_label_for_score(score: float) -> SemanticSupportLabel:
    """Derive the categorical label from Atlas's per-claim numeric score.

    - unsupported: ``0.00 <= score < 0.40``
    - unclear: ``0.40 <= score < 0.70``
    - supported: ``0.70 <= score <= 1.00``
    """
    if score < SEMANTIC_UNCLEAR_INCLUSIVE_LOWER:
        return "unsupported"
    if score < SEMANTIC_PASS_THRESHOLD:
        return "unclear"
    return "supported"


SEMANTIC_GRADER_OUTCOMES = frozenset(
    {
        "quality_pass",
        "quality_fail",
        "unavailable",
        "timeout",
        "rate_limited",
        "auth_config",
        "malformed",
        "refusal",
        "ownership_lost",
        "skipped",
        "config",
        "other",
    }
)


class SemanticClaimInput(BaseModel):
    """One sanitized claim sent to the semantic grader."""

    model_config = ConfigDict(extra="forbid")

    claim_ordinal: Annotated[int, Field(ge=1, le=MAX_CLAIMS_PER_DRAFT)]
    text: Annotated[str, Field(min_length=1, max_length=MAX_SEMANTIC_CLAIM_CODE_POINTS)]
    evidence_item_ids: Annotated[
        list[str],
        Field(min_length=1, max_length=MAX_EVIDENCE_IDS_PER_CLAIM),
    ]

    @field_validator("text")
    @classmethod
    def strip_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("claim text must be non-empty")
        return cleaned

    @field_validator("evidence_item_ids")
    @classmethod
    def validate_ids(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("evidence_item_ids must be non-empty strings")
        return cleaned


class SemanticExcerptInput(BaseModel):
    """One truncated, job-linked evidence excerpt sent to the grader."""

    model_config = ConfigDict(extra="forbid")

    evidence_item_id: Annotated[str, Field(min_length=1)]
    trust_label: Annotated[str, Field(min_length=1)]
    text: Annotated[
        str,
        Field(min_length=1, max_length=MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT),
    ]

    @field_validator("evidence_item_id", "trust_label")
    @classmethod
    def strip_required(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("field must be non-empty")
        return cleaned


class SemanticExcerptSource(BaseModel):
    """Approved evidence-view fields used to assemble excerpts.

    ``text`` is the stored display copy. Assembly truncates a copy and never
    mutates the stored value.
    """

    model_config = ConfigDict(extra="forbid")

    evidence_item_id: Annotated[str, Field(min_length=1)]
    trust_label: Annotated[str, Field(min_length=1)]
    text: str


class SemanticGradeRequest(BaseModel):
    """Bounded live-grader input: claims plus job-linked excerpts only."""

    model_config = ConfigDict(extra="forbid")

    job_id: Annotated[str, Field(min_length=1)]
    prompt_version: Literal["semantic_groundedness.v1"] = SEMANTIC_PROMPT_VERSION
    claims: Annotated[
        list[SemanticClaimInput],
        Field(max_length=MAX_CLAIMS_PER_DRAFT),
    ]
    excerpts: Annotated[
        list[SemanticExcerptInput],
        Field(max_length=MAX_EVIDENCE_ITEMS_TO_DRAFTER),
    ]

    @field_validator("job_id")
    @classmethod
    def strip_job_id(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("job_id must be non-empty")
        return cleaned


class SemanticClaimSupport(BaseModel):
    """Per-claim numeric score from the live grader.

    The model supplies only ``score``. Atlas derives the categorical
    support label from the closed score mapping. A provider ``support``
    field is forbidden extra data.
    """

    model_config = ConfigDict(extra="forbid")

    claim_ordinal: Annotated[int, Field(ge=1, le=MAX_CLAIMS_PER_DRAFT)]
    score: Annotated[float, Field(ge=0.0, le=1.0)]


class SemanticGroundednessOutput(BaseModel):
    """Strict structured LLM output. No free-text rationale.

    Atlas, not the model, owns the aggregate semantic score: the arithmetic
    mean of these per-claim scores. A model-supplied aggregate is forbidden.
    """

    model_config = ConfigDict(extra="forbid")

    claims: list[SemanticClaimSupport] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_ordinals(self) -> SemanticGroundednessOutput:
        ordinals = [item.claim_ordinal for item in self.claims]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("claim ordinals must be unique")
        return self
