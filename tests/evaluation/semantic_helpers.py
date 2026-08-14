"""Shared test helper for Fake/golden semantic-grader requests."""

from __future__ import annotations

from collections.abc import Sequence

from atlas.evaluation.contracts import EvaluationCandidateInput
from atlas.evaluation.semantic_contracts import (
    SemanticExcerptSource,
    SemanticGradeRequest,
)
from atlas.evaluation.semantic_input import assemble_semantic_grade_request
from atlas.evidence.bounds import TRUST_UNTRUSTED


def semantic_request_for_candidate(
    candidate: EvaluationCandidateInput,
    linked_ids: set[str] | Sequence[str],
    *,
    excerpt_text: str = "x",
) -> SemanticGradeRequest:
    """Assemble a bounded request using placeholder excerpt bodies.

    Production fingerprinting hashes the truncated bytes actually sent.
    Tests that do not load durable evidence use this placeholder text.
    """
    linked = set(linked_ids)
    sources = [
        SemanticExcerptSource(
            evidence_item_id=item_id,
            trust_label=TRUST_UNTRUSTED,
            text=excerpt_text,
        )
        for item_id in sorted(linked)
    ]
    return assemble_semantic_grade_request(
        job_id=candidate.job_id,
        claims=list(candidate.claims),
        linked_ids=linked,
        sources=sources,
    )
