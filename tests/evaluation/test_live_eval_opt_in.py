"""Opt-in live semantic evaluation is explicit and skipped in CI."""

from __future__ import annotations

import os

import pytest

from atlas.evaluation.aggregation import SEMANTIC_PASS_THRESHOLD
from atlas.evaluation.semantic_contracts import SEMANTIC_PROMPT_VERSION


def test_live_semantic_eval_is_not_inferred_and_not_run_here() -> None:
    """Default CI does not arm live provider or held-out calibration runs."""
    assert os.environ.get("ATLAS_ENABLE_LIVE_SEMANTIC_GRADER_TESTS") != "1"
    assert os.environ.get("ATLAS_ENABLE_LIVE_HELD_OUT_SEMANTIC_TESTS") != "1"
    assert SEMANTIC_PASS_THRESHOLD == 0.70
    assert SEMANTIC_PROMPT_VERSION == "semantic_groundedness.v1"


@pytest.mark.skip(
    reason=(
        "Live held-out calibration is opt-in via "
        "ATLAS_ENABLE_LIVE_HELD_OUT_SEMANTIC_TESTS=1 in "
        "tests/integration/test_held_out_semantic_live.py and is not run here."
    )
)
def test_live_semantic_eval_not_run_in_this_slice() -> None:
    raise AssertionError("live semantic provider tests must not run by default")
