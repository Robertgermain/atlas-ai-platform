"""Unit tests for recovery policy decisions and backoff formula."""

from __future__ import annotations

from typing import Literal

from atlas.evaluation.contracts import DimensionResult, EvaluationRunResult
from atlas.recovery.policy import (
    DEFAULT_RETRY_BASE_SECONDS,
    AttemptCounts,
    FailureCategory,
    categorize_exception,
    compute_retry_delay_seconds,
    decide_for_evaluation,
    decide_for_exception,
)

ZERO_COUNTS = AttemptCounts(
    repair_count=0, job_retry_count=0, evaluation_attempt_count=1
)


def _make_eval_result(
    *,
    passed: bool,
    disposition_hint: Literal[
        "complete", "terminal", "repair", "await_review", "retry"
    ] = "complete",
) -> EvaluationRunResult:
    return EvaluationRunResult(
        run_id="eval-1",
        research_job_id="job-1",
        workflow_execution_id="exec-1",
        evaluation_profile="evaluation.candidate.v1",
        evaluation_attempt=1,
        status="SUCCEEDED",
        input_fingerprint="a" * 64,
        passed=passed,
        aggregate_score=1.0 if passed else 0.3,
        disposition_hint=disposition_hint,
        dimensions=[],
        grader_versions={},
    )


def _dim(
    name: str,
    *,
    passed: bool,
    is_hard: bool = False,
    method: str = "deterministic",
) -> DimensionResult:
    return DimensionResult(
        name=name,  # type: ignore[arg-type]
        score=1.0 if passed else 0.0,
        passed=passed,
        method=method,  # type: ignore[arg-type]
        is_hard=is_hard,
        is_provisional=False,
    )


class TestBackoffFormula:
    def test_attempt_1_equals_base(self) -> None:
        delay = compute_retry_delay_seconds(attempt_number=1, jitter_seconds=0.0)
        assert delay == DEFAULT_RETRY_BASE_SECONDS

    def test_attempt_2_doubles(self) -> None:
        delay = compute_retry_delay_seconds(attempt_number=2, jitter_seconds=0.0)
        assert delay == DEFAULT_RETRY_BASE_SECONDS * 2

    def test_max_backoff_caps(self) -> None:
        delay = compute_retry_delay_seconds(
            attempt_number=20,
            max_backoff_seconds=30.0,
            jitter_seconds=0.0,
        )
        assert delay == 30.0

    def test_jitter_adds_bounded(self) -> None:
        delay = compute_retry_delay_seconds(
            attempt_number=1,
            jitter_seconds=0.5,
            jitter_max_seconds=1.0,
        )
        assert delay == DEFAULT_RETRY_BASE_SECONDS + 0.5


class TestDecideForEvaluation:
    def test_passed_returns_complete(self) -> None:
        result = _make_eval_result(passed=True)
        decision = decide_for_evaluation(
            result=result, dimensions=[], counts=ZERO_COUNTS
        )
        assert decision.action == "complete"

    def test_hard_failure_returns_terminal(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("citation_integrity", passed=False, is_hard=True)]
        decision = decide_for_evaluation(
            result=result, dimensions=dims, counts=ZERO_COUNTS
        )
        assert decision.action == "terminal"

    def test_soft_failure_returns_repair_when_cap_available(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("coverage", passed=False)]
        decision = decide_for_evaluation(
            result=result, dimensions=dims, counts=ZERO_COUNTS
        )
        assert decision.action == "repair"

    def test_soft_failure_returns_await_review_when_repair_exhausted(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("coverage", passed=False)]
        counts = AttemptCounts(
            repair_count=1, job_retry_count=0, evaluation_attempt_count=2
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "await_review"

    def test_eval_cap_returns_terminal(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("coverage", passed=False)]
        counts = AttemptCounts(
            repair_count=0, job_retry_count=0, evaluation_attempt_count=4
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "terminal"


class TestDecideForException:
    def test_transient_retry(self) -> None:
        decision = decide_for_exception(
            exc=TimeoutError("timed out"), counts=ZERO_COUNTS
        )
        assert decision.action == "retry"

    def test_transient_retry_cap_terminal(self) -> None:
        counts = AttemptCounts(
            repair_count=0, job_retry_count=2, evaluation_attempt_count=0
        )
        decision = decide_for_exception(exc=TimeoutError("timed out"), counts=counts)
        assert decision.action == "terminal"

    def test_permanent_terminal(self) -> None:
        decision = decide_for_exception(exc=ValueError("bad input"), counts=ZERO_COUNTS)
        assert decision.action == "terminal"

    def test_unknown_terminal(self) -> None:
        decision = decide_for_exception(exc=RuntimeError("unknown"), counts=ZERO_COUNTS)
        assert decision.action == "terminal"


class TestCategorizeException:
    def test_timeout(self) -> None:
        cat = categorize_exception(TimeoutError("x"))
        assert cat == FailureCategory.TRANSIENT_TIMEOUT

    def test_value_error(self) -> None:
        cat = categorize_exception(ValueError("x"))
        assert cat == FailureCategory.PERMANENT_VALIDATION

    def test_unknown(self) -> None:
        cat = categorize_exception(RuntimeError("x"))
        assert cat == FailureCategory.TERMINAL_UNKNOWN


class TestStructureRepairPolicy:
    """QUALITY_STRUCTURE: repair once, then terminal."""

    def test_structure_repair_first_time(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("report_structure", passed=False, is_hard=True)]
        counts = AttemptCounts(
            repair_count=0, job_retry_count=0, evaluation_attempt_count=1
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "repair"
        assert decision.reason_code == "STRUCTURE_REPAIR"

    def test_structure_terminal_after_repair(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("report_structure", passed=False, is_hard=True)]
        counts = AttemptCounts(
            repair_count=1, job_retry_count=0, evaluation_attempt_count=2
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "terminal"
        assert decision.reason_code == "STRUCTURE_REPAIR_EXHAUSTED"

    def test_citation_integrity_always_terminal(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("citation_integrity", passed=False, is_hard=True)]
        counts = AttemptCounts(
            repair_count=0, job_retry_count=0, evaluation_attempt_count=1
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "terminal"
        assert decision.reason_code == "HARD_QUALITY_FAIL"

    def test_tool_policy_always_terminal(self) -> None:
        result = _make_eval_result(passed=False, disposition_hint="terminal")
        dims = [_dim("tool_use", passed=False, is_hard=True)]
        counts = AttemptCounts(
            repair_count=0, job_retry_count=0, evaluation_attempt_count=1
        )
        decision = decide_for_evaluation(result=result, dimensions=dims, counts=counts)
        assert decision.action == "terminal"
        assert decision.reason_code == "HARD_QUALITY_FAIL"
