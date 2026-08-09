"""Offline retrieval metrics for the checked-in fake-embedding evaluation."""

from __future__ import annotations

from collections.abc import Sequence


def recall_at_k(relevant: Sequence[str], ranked: Sequence[str], *, k: int) -> float:
    if not relevant:
        return 0.0
    top = set(ranked[:k])
    hits = sum(1 for item in relevant if item in top)
    return hits / len(relevant)


def mrr_at_k(relevant: Sequence[str], ranked: Sequence[str], *, k: int) -> float:
    relevant_set = set(relevant)
    for index, item in enumerate(ranked[:k], start=1):
        if item in relevant_set:
            return 1.0 / index
    return 0.0


def mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)
