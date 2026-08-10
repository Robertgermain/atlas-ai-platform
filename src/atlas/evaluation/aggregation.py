"""Aggregate dimension scores for candidate evaluation profile."""

from __future__ import annotations

from atlas.evaluation.contracts import DimensionName, DimensionResult

HARD_DIMENSIONS: frozenset[DimensionName] = frozenset(
    {
        "citation_integrity",
        "tool_use",
        "report_structure",
    }
)

PROVISIONAL_SOFT_DIMENSIONS: frozenset[DimensionName] = frozenset(
    {
        "coverage",
        "completeness",
        "lexical_id_groundedness",
    }
)

BASE_WEIGHTS: dict[DimensionName, float] = {
    "citation_integrity": 0.25,
    "tool_use": 0.10,
    "report_structure": 0.15,
    "coverage": 0.15,
    "completeness": 0.15,
    "lexical_id_groundedness": 0.20,
}

SEMANTIC_WEIGHT = 0.10
PROVISIONAL_SOFT_PASS_THRESHOLD = 0.70
SEMANTIC_PASS_THRESHOLD = 0.70
HARD_PASS_SCORE = 1.0


def weight_for(name: DimensionName, *, semantic_present: bool = False) -> float:
    """Return the base (pre-renormalization) weight for a dimension name."""
    if name == "semantic_groundedness":
        return SEMANTIC_WEIGHT if semantic_present else 0.0
    return BASE_WEIGHTS[name]


def _applicable_weights(
    dimensions: list[DimensionResult],
) -> dict[DimensionName, float]:
    semantic = next(
        (item for item in dimensions if item.name == "semantic_groundedness"),
        None,
    )
    semantic_present = semantic is not None and semantic.method != "skipped"
    weights: dict[DimensionName, float] = dict(BASE_WEIGHTS)
    if semantic_present:
        total = sum(weights.values()) + SEMANTIC_WEIGHT
        scale = 1.0 / total
        weights = {name: value * scale for name, value in weights.items()}
        weights["semantic_groundedness"] = SEMANTIC_WEIGHT * scale
    return weights


def aggregate_dimensions(
    dimensions: list[DimensionResult],
) -> tuple[float, bool, list[DimensionResult]]:
    """Compute weighted aggregate, overall pass, and weight-stamped dimensions.

    Hard dimensions must score exactly ``1.0``. Provisional soft dimensions
    pass at ``>= 0.70``. Semantic groundedness is ignored when skipped; when
    present it uses weight ``0.10`` with renormalization and passes at
    ``>= 0.70``.
    """
    weights = _applicable_weights(dimensions)
    stamped: list[DimensionResult] = []
    weighted_sum = 0.0
    weight_total = 0.0

    by_name = {item.name: item for item in dimensions}

    for item in dimensions:
        if item.method == "skipped":
            stamped.append(item.model_copy(update={"weight": 0.0, "passed": True}))
            continue
        weight = weights.get(item.name, item.weight)
        passed = item.passed
        if item.name in HARD_DIMENSIONS:
            passed = item.score == HARD_PASS_SCORE
        elif item.name in PROVISIONAL_SOFT_DIMENSIONS:
            passed = item.score >= PROVISIONAL_SOFT_PASS_THRESHOLD
        elif item.name == "semantic_groundedness":
            passed = item.score >= SEMANTIC_PASS_THRESHOLD
        stamped.append(item.model_copy(update={"weight": weight, "passed": passed}))
        weighted_sum += item.score * weight
        weight_total += weight

    aggregate_score = (weighted_sum / weight_total) if weight_total > 0 else 0.0

    hard_ok = all(
        by_name[name].score == HARD_PASS_SCORE
        for name in HARD_DIMENSIONS
        if name in by_name
    )
    soft_ok = all(
        by_name[name].score >= PROVISIONAL_SOFT_PASS_THRESHOLD
        for name in PROVISIONAL_SOFT_DIMENSIONS
        if name in by_name
    )
    semantic = by_name.get("semantic_groundedness")
    if semantic is None or semantic.method == "skipped":
        semantic_ok = True
    else:
        semantic_ok = semantic.score >= SEMANTIC_PASS_THRESHOLD

    passed = hard_ok and soft_ok and semantic_ok
    return aggregate_score, passed, stamped
