"""Bounded specialist agents for research planning through citation verification."""

from atlas.specialists.citation_verifier import DurableCitationVerifier
from atlas.specialists.contracts import (
    CitationVerifierInput,
    CitationVerifierOutput,
    PlannerInput,
    PlannerOutput,
    ResearchSpecialistInput,
    ResearchSpecialistOutput,
    SynthesizerInput,
    SynthesizerOutput,
)
from atlas.specialists.errors import (
    SpecialistCitationError,
    SpecialistConfigurationError,
    SpecialistError,
    SpecialistValidationError,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.research import (
    GovernedResearchRetrievalSpecialist,
    merge_evidence_ids_preserving_order,
)
from atlas.specialists.synthesizer import BoundedReportSynthesizer

__all__ = [
    "BoundedPlannerSpecialist",
    "BoundedReportSynthesizer",
    "CitationVerifierInput",
    "CitationVerifierOutput",
    "DurableCitationVerifier",
    "GovernedResearchRetrievalSpecialist",
    "PlannerInput",
    "PlannerOutput",
    "ResearchSpecialistInput",
    "ResearchSpecialistOutput",
    "SpecialistCitationError",
    "SpecialistConfigurationError",
    "SpecialistError",
    "SpecialistValidationError",
    "SynthesizerInput",
    "SynthesizerOutput",
    "merge_evidence_ids_preserving_order",
]
