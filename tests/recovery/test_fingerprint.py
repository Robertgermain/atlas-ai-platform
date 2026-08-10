"""Tests for policy decision fingerprint idempotency."""

from __future__ import annotations

from atlas.recovery.fingerprint import fingerprint_policy_decision


class TestFingerprintPolicyDecision:
    """Verify determinism and uniqueness of policy fingerprints."""

    def test_same_inputs_same_fingerprint(self) -> None:
        fp1 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="repair",
            failure_category="QUALITY_STRUCTURE",
            reason_code="STRUCTURE_REPAIR",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=1,
        )
        fp2 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="repair",
            failure_category="QUALITY_STRUCTURE",
            reason_code="STRUCTURE_REPAIR",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=1,
        )
        assert fp1 == fp2
        assert len(fp1) == 64

    def test_different_decision_different_fingerprint(self) -> None:
        fp1 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="repair",
            failure_category="QUALITY_STRUCTURE",
            reason_code="STRUCTURE_REPAIR",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=1,
        )
        fp2 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="terminal",
            failure_category="QUALITY_STRUCTURE",
            reason_code="STRUCTURE_REPAIR_EXHAUSTED",
            repair_count=1,
            job_retry_count=0,
            evaluation_attempt_count=2,
        )
        assert fp1 != fp2

    def test_different_job_different_fingerprint(self) -> None:
        fp1 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
        fp2 = fingerprint_policy_decision(
            research_job_id="job-2",
            workflow_execution_id="exec-1",
            evaluation_run_id=None,
            decision="retry",
            failure_category="TRANSIENT_TIMEOUT",
            reason_code="TRANSIENT_RETRY",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
        assert fp1 != fp2

    def test_null_execution_id_handled(self) -> None:
        fp = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id=None,
            evaluation_run_id=None,
            decision="terminal",
            failure_category="TERMINAL_UNKNOWN",
            reason_code="UNCLASSIFIED_TERMINAL",
            repair_count=0,
            job_retry_count=0,
            evaluation_attempt_count=0,
        )
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_replay_same_fingerprint_on_increment(self) -> None:
        """Same counters + same decision inputs = same fingerprint (replay)."""
        fp1 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="await_review",
            failure_category="NEEDS_HUMAN_REVIEW",
            reason_code="AWAIT_REVIEW_POLICY",
            repair_count=1,
            job_retry_count=0,
            evaluation_attempt_count=3,
        )
        fp2 = fingerprint_policy_decision(
            research_job_id="job-1",
            workflow_execution_id="exec-1",
            evaluation_run_id="run-1",
            decision="await_review",
            failure_category="NEEDS_HUMAN_REVIEW",
            reason_code="AWAIT_REVIEW_POLICY",
            repair_count=1,
            job_retry_count=0,
            evaluation_attempt_count=3,
        )
        assert fp1 == fp2
