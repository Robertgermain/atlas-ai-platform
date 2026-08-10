"""Deterministic fingerprinting for policy decisions."""

from __future__ import annotations

import hashlib
import json


def fingerprint_policy_decision(
    *,
    research_job_id: str,
    workflow_execution_id: str | None,
    evaluation_run_id: str | None,
    decision: str,
    failure_category: str,
    reason_code: str,
    repair_count: int,
    job_retry_count: int,
    evaluation_attempt_count: int,
) -> str:
    """Compute a SHA-256 hex digest for a policy decision's canonical inputs.

    The fingerprint enables idempotent replay: identical decision inputs produce
    the same fingerprint; the unique constraint on (job, fingerprint) prevents
    duplicate rows while allowing re-select on conflict.
    """
    canonical = json.dumps(
        {
            "research_job_id": research_job_id,
            "workflow_execution_id": workflow_execution_id,
            "evaluation_run_id": evaluation_run_id,
            "decision": decision,
            "failure_category": failure_category,
            "reason_code": reason_code,
            "repair_count": repair_count,
            "job_retry_count": job_retry_count,
            "evaluation_attempt_count": evaluation_attempt_count,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
