"""Hash helpers with documented Milestone 10A semantics."""

from __future__ import annotations

import hashlib


def sha256_utf8_bytes(raw: bytes) -> str:
    """Return hex SHA-256 of exact bytes."""
    return hashlib.sha256(raw).hexdigest()


def sha256_utf8_text(text: str) -> str:
    """Return hex SHA-256 of ``text`` encoded as UTF-8."""
    return sha256_utf8_bytes(text.encode("utf-8"))


def document_content_sha256(raw_utf8: bytes) -> str:
    """SHA-256 of the exact accepted raw UTF-8 bytes before normalization."""
    return sha256_utf8_bytes(raw_utf8)


def evidence_item_content_sha256(normalized_text: str) -> str:
    """SHA-256 of the exact normalized evidence-item text encoded as UTF-8."""
    return sha256_utf8_text(normalized_text)


def report_artifact_content_sha256(rendered_report: str) -> str:
    """SHA-256 of the canonical final rendered report encoded as UTF-8."""
    return sha256_utf8_text(rendered_report)
