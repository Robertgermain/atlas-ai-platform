"""Deterministic citation verifier specialist (no LLM)."""

from __future__ import annotations

from atlas.evidence.errors import (
    CitationIntegrityError,
    ClaimEvidenceRequiredError,
    EvidenceNotFoundError,
)
from atlas.evidence.service import CitationValidator, EvidenceIngestService
from atlas.specialists.contracts import (
    CitationVerifierInput,
    CitationVerifierOutput,
)
from atlas.specialists.errors import SpecialistCitationError


class DurableCitationVerifier:
    """Fail-closed verifier against durable job-linked evidence provenance.

    Independently validates every claim citation as:

    claim evidence ID → evidence_job_links (this job) → evidence item →
    document → source

    Does not use an LLM and does not write model-ledger rows. Graph-state
    evidence ID lists are not authoritative for this check.
    """

    def __init__(
        self,
        *,
        citation_validator: CitationValidator,
        evidence_ingest: EvidenceIngestService,
    ) -> None:
        self._citation_validator = citation_validator
        self._evidence_ingest = evidence_ingest

    def run(self, request: CitationVerifierInput) -> CitationVerifierOutput:
        claims = list(request.claims)
        if not claims:
            return CitationVerifierOutput(claims=[])
        try:
            self._citation_validator.validate(
                research_job_id=request.research_job_id,
                claims=claims,
            )
            for claim in claims:
                for evidence_item_id in claim.evidence_item_ids:
                    view = self._evidence_ingest.get_item(evidence_item_id)
                    if not view.document_id or not view.source_id:
                        raise SpecialistCitationError(
                            "citation provenance is incomplete"
                        )
        except (
            CitationIntegrityError,
            ClaimEvidenceRequiredError,
            EvidenceNotFoundError,
            SpecialistCitationError,
        ) as exc:
            raise SpecialistCitationError("citation verification failed") from exc
        return CitationVerifierOutput(claims=claims)
