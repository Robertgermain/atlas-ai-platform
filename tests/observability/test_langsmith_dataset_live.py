"""Opt-in live LangSmith dataset/experiment (never enabled in CI).

Uploads metadata-only examples from ``candidate_goldens.v1`` and runs a
boolean compare against ``grader_expected``. Unique experiments are
retained for manual cleanup. Production code is not involved.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

import pytest
from langsmith import evaluate

from atlas.config.settings import Settings
from atlas.observability.langsmith import configure_langsmith, reset_langsmith_for_tests
from tests.observability.langsmith_dataset_support import (
    DATASET_NAME,
    boolean_compare,
    dataset_examples,
    ensure_golden_dataset,
    listed_example_ids,
    target_from_inputs,
    upsert_golden_examples,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("ATLAS_ENABLE_LIVE_LANGSMITH_TESTS") != "1"
    or not (os.environ.get("ATLAS_LANGSMITH_API_KEY") or "").strip(),
    reason=(
        "Live LangSmith tests require ATLAS_ENABLE_LIVE_LANGSMITH_TESTS=1 "
        "and ATLAS_LANGSMITH_API_KEY"
    ),
)


def _evaluator_results(row: Mapping[str, Any]) -> list[Any]:
    payload = row["evaluation_results"]
    if isinstance(payload, dict):
        results = payload.get("results")
    else:
        results = getattr(payload, "results", None)
    if not isinstance(results, list) or not results:
        pytest.fail("experiment row is missing evaluator results")
    return results


def _score_passed(item: object) -> bool:
    if isinstance(item, dict):
        score = item.get("score")
    else:
        score = getattr(item, "score", None)
    return score is True or score == 1 or score == 1.0


def test_live_dataset_experiment_boolean_compare() -> None:
    reset_langsmith_for_tests()
    settings = Settings()
    handle = None
    try:
        handle = configure_langsmith(settings)
        if not handle.enabled or handle.client is None:
            pytest.fail("LangSmith client did not initialize")
        client = handle.client
        ensure_golden_dataset(client)
        expected_ids = upsert_golden_examples(client)
        listed_ids = listed_example_ids(client)
        assert listed_ids == set(expected_ids)
        assert len(listed_ids) == len(dataset_examples())
        prefix = f"atlas.15b.{uuid4().hex[:12]}"
        results = evaluate(
            target_from_inputs,
            data=DATASET_NAME,
            evaluators=[boolean_compare],
            client=client,
            experiment_prefix=prefix,
            max_concurrency=0,
            upload_results=True,
        )
        rows = list(results)
        assert len(rows) == len(expected_ids)
        for row in rows:
            for item in _evaluator_results(row):
                assert _score_passed(item)
    finally:
        if handle is not None:
            handle.close()
        reset_langsmith_for_tests()
