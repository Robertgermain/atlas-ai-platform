"""Typed errors for evidence ingestion, citation, and report persistence."""

from __future__ import annotations


class EvidenceError(Exception):
    """Base class for evidence-layer failures."""


class EvidenceValidationError(EvidenceError):
    """Raised when evidence input fails validation or size bounds."""


class EvidenceNotFoundError(EvidenceError):
    """Raised when an evidence item cannot be found."""

    def __init__(self, evidence_item_id: str) -> None:
        self.evidence_item_id = evidence_item_id
        super().__init__("Evidence item not found.")


class EvidenceTooLargeError(EvidenceError):
    """Raised when chunking or text size exceeds hard caps."""


class CitationIntegrityError(EvidenceError):
    """Raised when a claim cites evidence outside the job scope."""


class ClaimEvidenceRequiredError(EvidenceError):
    """Raised when a claim is missing required evidence citations."""


class ReportArtifactConflictError(EvidenceError):
    """Raised when an execution already has a conflicting final artifact."""


class UrlCanonicalizationError(EvidenceError):
    """Raised when a URL cannot be safely canonicalized for source identity."""
