"""Typed handoff contracts for Milestone 11 specialists."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from atlas.evidence.bounds import MAX_CLAIMS_PER_DRAFT
from atlas.evidence.contracts import ClaimStructured, EvidenceContextItem

MAX_RESEARCH_FINDINGS = 6
SPECIALIST_PLANNER = "planner"
SPECIALIST_RESEARCH = "research_retrieval"
SPECIALIST_SYNTHESIZER = "synthesizer"
SPECIALIST_CITATION_VERIFIER = "citation_verifier"


class PlannerInput(BaseModel):
    job_id: str
    question: str
    prompt_version: str


class PlannerOutput(BaseModel):
    tasks: Annotated[list[str], Field(min_length=3, max_length=3)]
    prompt_version: str
    specialist_id: Literal["planner"] = "planner"

    @field_validator("tasks")
    @classmethod
    def validate_tasks(cls, value: list[str]) -> list[str]:
        cleaned = [task.strip() for task in value]
        if len(cleaned) != 3 or any(not task for task in cleaned):
            raise ValueError("plan must contain exactly three non-empty tasks")
        return cleaned


class ResearchSpecialistInput(BaseModel):
    job_id: str
    question: str
    plan: Annotated[list[str], Field(min_length=3, max_length=3)]
    workflow_execution_id: str | None = None
    workflow_node_attempt: int | None = None
    retrieval_k: int = Field(default=5, ge=1, le=8)


class ResearchSpecialistOutput(BaseModel):
    findings: Annotated[list[str], Field(max_length=MAX_RESEARCH_FINDINGS)]
    evidence_item_ids: list[str]
    specialist_id: Literal["research_retrieval"] = "research_retrieval"

    @field_validator("findings")
    @classmethod
    def validate_findings(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("findings must be non-empty strings when present")
        if len(cleaned) > MAX_RESEARCH_FINDINGS:
            raise ValueError("findings exceed specialist bound")
        return cleaned


class SynthesizerInput(BaseModel):
    job_id: str
    question: str
    plan: Annotated[list[str], Field(min_length=3, max_length=3)]
    findings: Annotated[list[str], Field(max_length=MAX_RESEARCH_FINDINGS)]
    evidence_item_ids: list[str]
    prompt_version: str


class SynthesizerOutput(BaseModel):
    draft: str = Field(min_length=1)
    claims: Annotated[list[ClaimStructured], Field(max_length=MAX_CLAIMS_PER_DRAFT)]
    evidence_pack: list[EvidenceContextItem]
    specialist_id: Literal["synthesizer"] = "synthesizer"

    @field_validator("draft")
    @classmethod
    def validate_draft(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("draft must be non-empty")
        return cleaned


class CitationVerifierInput(BaseModel):
    research_job_id: str
    claims: list[ClaimStructured]


class CitationVerifierOutput(BaseModel):
    claims: list[ClaimStructured]
    specialist_id: Literal["citation_verifier"] = "citation_verifier"
