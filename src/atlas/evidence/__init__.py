"""Evidence, provenance, claims, and citation services for Milestone 10A."""

from atlas.evidence.contracts import (
    ClaimStructured,
    EvidenceContextItem,
    EvidenceItemView,
    IngestDocumentRequest,
    IngestDocumentResult,
    JobCitationsResponse,
    ReportArtifactView,
)
from atlas.evidence.errors import (
    CitationIntegrityError,
    ClaimEvidenceRequiredError,
    EvidenceError,
    EvidenceNotFoundError,
    EvidenceTooLargeError,
    EvidenceValidationError,
    ReportArtifactConflictError,
    UrlCanonicalizationError,
)
from atlas.evidence.service import (
    CitationValidator,
    EvidenceIngestService,
    ReportArtifactService,
)

__all__ = [
    "CitationIntegrityError",
    "CitationValidator",
    "ClaimEvidenceRequiredError",
    "ClaimStructured",
    "EvidenceContextItem",
    "EvidenceError",
    "EvidenceIngestService",
    "EvidenceItemView",
    "EvidenceNotFoundError",
    "EvidenceTooLargeError",
    "EvidenceValidationError",
    "IngestDocumentRequest",
    "IngestDocumentResult",
    "JobCitationsResponse",
    "ReportArtifactConflictError",
    "ReportArtifactService",
    "ReportArtifactView",
    "UrlCanonicalizationError",
]
