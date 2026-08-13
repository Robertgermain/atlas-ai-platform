"""Canonical fingerprinting for evaluation grading snapshots."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

from atlas.evaluation.contracts import EvaluationCandidateInput, ToolSummaryRow
from atlas.evaluation.semantic_contracts import (
    SKIPPED_SEMANTIC_GRADER_VERSION,
    SemanticClaimInput,
    SemanticExcerptInput,
    SemanticGraderVersion,
    SemanticPromptVersion,
)
from atlas.evaluation.semantic_input import (
    claim_text_hashes_in_ordinal_order,
    excerpt_fingerprint_rows,
)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _claims_canonical(candidate: EvaluationCandidateInput) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for claim in candidate.claims:
        rows.append(
            {
                "text_sha256": _hash_text(claim.text),
                "evidence_item_ids": sorted(claim.evidence_item_ids),
            }
        )
    rows.sort(key=lambda row: (row["text_sha256"], tuple(row["evidence_item_ids"])))
    return rows


def _tool_rows_canonical(tool_rows: list[ToolSummaryRow]) -> list[dict[str, Any]]:
    rows = [
        {
            "node_name": row.node_name,
            "origin": row.origin,
            "tool_id": row.tool_id,
            "status": row.status,
        }
        for row in tool_rows
    ]
    rows.sort(
        key=lambda row: (
            row["origin"],
            row["node_name"],
            row["tool_id"],
            row["status"],
        )
    )
    return rows


def fingerprint_grading_snapshot(
    candidate: EvaluationCandidateInput,
    *,
    linked_evidence_ids: set[str] | list[str],
    tool_rows: list[ToolSummaryRow],
    provenance_ok: bool,
    max_logical_calls: int,
    semantic_grader_version: SemanticGraderVersion = SKIPPED_SEMANTIC_GRADER_VERSION,
    semantic_prompt_version: SemanticPromptVersion = SKIPPED_SEMANTIC_GRADER_VERSION,
    semantic_claims: Sequence[SemanticClaimInput] | None = None,
    semantic_excerpts: Sequence[SemanticExcerptInput] | None = None,
) -> str:
    """Return a 64-hex SHA-256 of the complete durable grading snapshot.

    Includes every input that can change a grade: candidate plan/findings/
    draft/claims, linked evidence identifiers, execution-scoped tool summary
    rows, logical-call budget summary, provenance outcome, semantic grader
    identity, and (when not skipped) ordinal claim-text hashes plus hashes of
    the exact truncated excerpt bytes that would be sent.

    Skipped mode does not load or hash unused semantic excerpts. Grader
    version still distinguishes skipped from fake/live so a skipped run
    cannot replay as a graded one.

    Never includes evidence bodies, raw prompts, ownership tokens, or secrets.
    """
    logical_call_count = len(tool_rows)
    if semantic_grader_version == SKIPPED_SEMANTIC_GRADER_VERSION:
        claim_hashes: list[str] = []
        excerpt_rows: list[dict[str, str]] = []
        prompt_version: str = SKIPPED_SEMANTIC_GRADER_VERSION
    else:
        claim_hashes = claim_text_hashes_in_ordinal_order(list(semantic_claims or ()))
        excerpt_rows = excerpt_fingerprint_rows(list(semantic_excerpts or ()))
        prompt_version = semantic_prompt_version
    payload: dict[str, Any] = {
        "claims": _claims_canonical(candidate),
        "draft_sha256": _hash_text(candidate.draft),
        "evaluation_attempt": candidate.evaluation_attempt,
        "findings_sha256": [_hash_text(item) for item in candidate.findings],
        "job_id": candidate.job_id,
        "linked_evidence_ids": sorted(linked_evidence_ids),
        "plan": list(candidate.plan),
        "profile": candidate.evaluation_profile,
        "provenance_ok": bool(provenance_ok),
        "repair_count": candidate.repair_count,
        "semantic": {
            "claim_text_sha256": claim_hashes,
            "excerpts": excerpt_rows,
            "grader_version": semantic_grader_version,
            "prompt_version": prompt_version,
        },
        "tool_budget": {
            "logical_call_count": logical_call_count,
            "max_logical_calls": int(max_logical_calls),
        },
        "tool_rows": _tool_rows_canonical(tool_rows),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fingerprint_candidate(candidate: EvaluationCandidateInput) -> str:
    """Fingerprint using only candidate-embedded fields (unit-test helper).

    Production evaluation must call :func:`fingerprint_grading_snapshot` after
    loading durable linked evidence and execution-scoped tool rows.
    """
    return fingerprint_grading_snapshot(
        candidate,
        linked_evidence_ids=set(candidate.evidence_item_ids),
        tool_rows=list(candidate.tool_summary),
        provenance_ok=True,
        max_logical_calls=6,
    )
