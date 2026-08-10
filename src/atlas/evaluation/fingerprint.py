"""Canonical fingerprinting for evaluation grading snapshots."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from atlas.evaluation.contracts import EvaluationCandidateInput, ToolSummaryRow


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
) -> str:
    """Return a 64-hex SHA-256 of the complete durable grading snapshot.

    Includes every input that can change a grade: candidate plan/findings/
    draft/claims, linked evidence identifiers, execution-scoped tool summary
    rows, logical-call budget summary, and provenance outcome.

    Never includes evidence bodies, raw prompts, ownership tokens, or secrets.
    """
    logical_call_count = len(tool_rows)
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
