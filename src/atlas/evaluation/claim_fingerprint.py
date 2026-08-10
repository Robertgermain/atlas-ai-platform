"""Hash helpers for evaluation claim attribution (never store raw claim tokens)."""

from __future__ import annotations

import hashlib
import re

from atlas.evaluation.errors import EvaluationValidationError

_CLAIM_TOKEN_RE = re.compile(r"^[0-9a-f]{64}$")


def fingerprint_job_claim_token(claim_token: str) -> str:
    """Return SHA-256 hex of a research-job claim token.

    The raw token must never be persisted in evaluation rows, APIs, logs, or
    checkpoints. Callers pass the token only in-memory for possession proof.
    """
    cleaned = claim_token.strip()
    if not _CLAIM_TOKEN_RE.fullmatch(cleaned):
        raise EvaluationValidationError(
            "Job claim token is missing or invalid for evaluation."
        )
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()
