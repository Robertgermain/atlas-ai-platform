"""Evaluation grading fingerprint includes semantic identity (Slice 15C1)."""

from __future__ import annotations

from atlas.evaluation.contracts import EvaluationCandidateInput
from atlas.evaluation.fingerprint import fingerprint_grading_snapshot
from atlas.evaluation.semantic_contracts import (
    FAKE_SEMANTIC_GRADER_VERSION,
    LIVE_SEMANTIC_GRADER_VERSION,
    SEMANTIC_PROMPT_VERSION,
    SKIPPED_SEMANTIC_GRADER_VERSION,
    SemanticExcerptSource,
    SemanticGradeRequest,
    SemanticGraderVersion,
    SemanticPromptVersion,
)
from atlas.evaluation.semantic_input import assemble_semantic_grade_request
from atlas.evidence.contracts import ClaimStructured


def _candidate() -> EvaluationCandidateInput:
    return EvaluationCandidateInput(
        job_id="job-fp",
        question="Fingerprint probe",
        plan=["Clarify fingerprint scope carefully"],
        findings=["Clarify fingerprint scope carefully in findings"],
        draft="Clarify fingerprint scope carefully in the draft.",
        claims=[ClaimStructured(text="Claim one", evidence_item_ids=["ev-b", "ev-a"])],
        evidence_item_ids=["ev-a", "ev-b"],
        tool_summary=[],
        evaluation_profile="evaluation.candidate.v1",
    )


def _request(*, excerpt_text: str = "body-a") -> SemanticGradeRequest:
    candidate = _candidate()
    return assemble_semantic_grade_request(
        job_id=candidate.job_id,
        claims=list(candidate.claims),
        linked_ids={"ev-a", "ev-b"},
        sources=[
            SemanticExcerptSource(
                evidence_item_id="ev-b",
                trust_label="[untrusted_source]",
                text=excerpt_text.replace("a", "b")
                if excerpt_text == "body-a"
                else "body-b",
            ),
            SemanticExcerptSource(
                evidence_item_id="ev-a",
                trust_label="[untrusted_source]",
                text=excerpt_text,
            ),
        ],
    )


def _fingerprint(
    *,
    grader_version: SemanticGraderVersion = SKIPPED_SEMANTIC_GRADER_VERSION,
    prompt_version: SemanticPromptVersion = SKIPPED_SEMANTIC_GRADER_VERSION,
    request: SemanticGradeRequest | None = None,
    linked: set[str] | None = None,
) -> str:
    candidate = _candidate()
    claims = None if request is None else request.claims
    excerpts = None if request is None else request.excerpts
    return fingerprint_grading_snapshot(
        candidate,
        linked_evidence_ids=linked if linked is not None else {"ev-a", "ev-b"},
        tool_rows=[],
        provenance_ok=True,
        max_logical_calls=6,
        semantic_grader_version=grader_version,
        semantic_prompt_version=prompt_version,
        semantic_claims=claims,
        semantic_excerpts=excerpts,
    )


def test_same_snapshot_replays() -> None:
    request = _request()
    first = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    second = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    assert first == second
    assert len(first) == 64


def test_excerpt_text_change_changes_fingerprint() -> None:
    base = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=_request(excerpt_text="body-a"),
    )
    changed = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=_request(excerpt_text="body-changed"),
    )
    assert base != changed


def test_source_ordering_does_not_change_fingerprint() -> None:
    candidate = _candidate()
    forward = assemble_semantic_grade_request(
        job_id=candidate.job_id,
        claims=list(candidate.claims),
        linked_ids={"ev-a", "ev-b"},
        sources=[
            SemanticExcerptSource(
                evidence_item_id="ev-a",
                trust_label="[untrusted_source]",
                text="alpha",
            ),
            SemanticExcerptSource(
                evidence_item_id="ev-b",
                trust_label="[untrusted_source]",
                text="beta",
            ),
        ],
    )
    reverse = assemble_semantic_grade_request(
        job_id=candidate.job_id,
        claims=list(candidate.claims),
        linked_ids={"ev-a", "ev-b"},
        sources=[
            SemanticExcerptSource(
                evidence_item_id="ev-b",
                trust_label="[untrusted_source]",
                text="beta",
            ),
            SemanticExcerptSource(
                evidence_item_id="ev-a",
                trust_label="[untrusted_source]",
                text="alpha",
            ),
        ],
    )
    assert _fingerprint(
        grader_version=FAKE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=forward,
    ) == _fingerprint(
        grader_version=FAKE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=reverse,
    )


def test_skipped_fake_and_live_fingerprints_differ() -> None:
    request = _request()
    skipped = _fingerprint()
    fake = _fingerprint(
        grader_version=FAKE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    live = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    assert skipped != fake
    assert skipped != live
    assert fake != live


def test_skipped_does_not_hash_unused_excerpts_but_still_differs() -> None:
    skipped_without = _fingerprint()
    skipped_with_unused = _fingerprint(request=_request(excerpt_text="secret-body"))
    assert skipped_without == skipped_with_unused
    live = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=_request(excerpt_text="secret-body"),
    )
    assert skipped_without != live


def test_changed_grader_or_prompt_version_differs() -> None:
    request = _request()
    live = _fingerprint(
        grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    fake_grader = _fingerprint(
        grader_version=FAKE_SEMANTIC_GRADER_VERSION,
        prompt_version=SEMANTIC_PROMPT_VERSION,
        request=request,
    )
    assert live != fake_grader
    # Prompt version is part of the payload for non-skipped modes.
    skipped_prompt_on_live = fingerprint_grading_snapshot(
        _candidate(),
        linked_evidence_ids={"ev-a", "ev-b"},
        tool_rows=[],
        provenance_ok=True,
        max_logical_calls=6,
        semantic_grader_version=LIVE_SEMANTIC_GRADER_VERSION,
        semantic_prompt_version=SKIPPED_SEMANTIC_GRADER_VERSION,
        semantic_claims=request.claims,
        semantic_excerpts=request.excerpts,
    )
    assert live != skipped_prompt_on_live
