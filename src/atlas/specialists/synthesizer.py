"""Report synthesizer specialist."""

from __future__ import annotations

from atlas.evidence.contracts import EvidenceContextItem
from atlas.evidence.service import EvidenceIngestService
from atlas.models.contracts import DraftRequest
from atlas.models.ports import ResearchDrafter
from atlas.specialists.contracts import SynthesizerInput, SynthesizerOutput
from atlas.specialists.errors import SpecialistValidationError


class BoundedReportSynthesizer:
    """Builds a bounded evidence pack and validates claim→pack scope.

    Owns the synthesizer boundary: construct/consume the evidence pack, call the
    model drafter port, and reject any claim that cites an evidence ID outside
    the supplied pack. Never silently strips illegal IDs.
    """

    def __init__(
        self,
        *,
        drafter: ResearchDrafter,
        evidence_ingest: EvidenceIngestService | None = None,
    ) -> None:
        self._drafter = drafter
        self._evidence_ingest = evidence_ingest

    def run(self, request: SynthesizerInput) -> SynthesizerOutput:
        if len(request.plan) != 3:
            raise SpecialistValidationError("synthesizer plan is invalid")
        pack = self._build_pack(request.evidence_item_ids)
        pack_ids = {item.evidence_item_id for item in pack}
        result = self._drafter.draft(
            DraftRequest(
                job_id=request.job_id,
                question=request.question,
                plan=list(request.plan),
                findings=list(request.findings),
                prompt_version=request.prompt_version,
                evidence=pack,
            )
        )
        draft = result.draft.strip()
        if not draft:
            raise SpecialistValidationError("synthesizer draft is empty")
        claims = list(result.claims)
        if not pack:
            if claims:
                raise SpecialistValidationError(
                    "synthesizer claims require a non-empty evidence pack"
                )
            return SynthesizerOutput(draft=draft, claims=[], evidence_pack=[])
        for claim in claims:
            for evidence_item_id in claim.evidence_item_ids:
                if evidence_item_id not in pack_ids:
                    raise SpecialistValidationError(
                        "synthesizer claim cites evidence outside the pack"
                    )
        return SynthesizerOutput(
            draft=draft,
            claims=claims,
            evidence_pack=pack,
        )

    def _build_pack(self, evidence_item_ids: list[str]) -> list[EvidenceContextItem]:
        if self._evidence_ingest is None or not evidence_item_ids:
            return []
        return self._evidence_ingest.build_drafter_evidence_pack(evidence_item_ids)
