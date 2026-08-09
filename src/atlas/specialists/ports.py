"""Specialist capability ports."""

from __future__ import annotations

from typing import Protocol

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


class PlannerSpecialist(Protocol):
    def run(self, request: PlannerInput) -> PlannerOutput: ...


class ResearchRetrievalSpecialist(Protocol):
    def run(self, request: ResearchSpecialistInput) -> ResearchSpecialistOutput: ...


class ReportSynthesizer(Protocol):
    def run(self, request: SynthesizerInput) -> SynthesizerOutput: ...


class CitationVerifier(Protocol):
    def run(self, request: CitationVerifierInput) -> CitationVerifierOutput: ...
