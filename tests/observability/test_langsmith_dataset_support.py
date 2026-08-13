"""Offline dataset/experiment payload contract (no LangSmith network)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.observability.langsmith_dataset_support import (
    DATASET_NAME,
    GOLDENS_PATH,
    boolean_compare,
    dataset_examples,
    example_inputs,
    example_outputs,
    grade_case_booleans,
    load_graded_golden_cases,
)


def test_goldens_path_is_under_tests_evaluation() -> None:
    assert GOLDENS_PATH == (
        Path(__file__).resolve().parents[1] / "evaluation" / "candidate_goldens.v1.json"
    )
    assert GOLDENS_PATH.is_file()
    assert DATASET_NAME == "atlas.candidate_goldens.v1"


def test_examples_are_metadata_only() -> None:
    cases = load_graded_golden_cases()
    assert cases
    for case in cases:
        inputs = example_inputs(case)
        outputs = example_outputs(case)
        blob = f"{inputs}{outputs}"
        question = case["candidate"]["question"]
        draft = case["candidate"]["draft"]
        if question:
            assert question not in blob
        if draft:
            assert draft not in blob
        assert "human_expected" not in blob
        assert "rationale" not in blob
        assert set(inputs) == {"fixture_id", "label", "fingerprint"}
        assert set(outputs) == {"overall_passed", "dimension_passed"}


def test_local_boolean_grade_matches_grader_expected() -> None:
    for case in load_graded_golden_cases():
        predicted = grade_case_booleans(case)
        reference = example_outputs(case)
        result = boolean_compare(predicted, reference)
        assert result["score"] == 1.0


def test_dataset_examples_have_stable_ids() -> None:
    first = dataset_examples()
    second = dataset_examples()
    assert [item["example_id"] for item in first] == [
        item["example_id"] for item in second
    ]


def test_production_observability_does_not_import_tests_or_goldens() -> None:
    src = Path(__file__).resolve().parents[2] / "src" / "atlas"
    forbidden = (
        "candidate_goldens.v1.json",
        "tests.observability.langsmith_dataset_support",
        "tests.evaluation",
    )
    for path in src.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text


class _Example:
    def __init__(self, example_id: object) -> None:
        self.id = example_id


class _FakeDatasetClient:
    def __init__(self) -> None:
        self.dataset: object | None = None
        self.examples: dict[object, dict[str, object]] = {}
        self.create_calls = 0
        self.update_calls = 0

    def read_dataset(self, *, dataset_name: str) -> object:
        from langsmith.utils import LangSmithNotFoundError

        if self.dataset is None:
            raise LangSmithNotFoundError("dataset missing")
        assert dataset_name == DATASET_NAME
        return self.dataset

    def create_dataset(self, dataset_name: str, **_kwargs: object) -> object:
        from langsmith.utils import LangSmithConflictError

        if self.dataset is not None:
            raise LangSmithConflictError("dataset exists")
        self.dataset = {"name": dataset_name}
        return self.dataset

    def list_examples(self, *, dataset_name: str) -> list[_Example]:
        assert dataset_name == DATASET_NAME
        return [_Example(example_id) for example_id in self.examples]

    def create_examples(
        self, *, dataset_name: str, examples: list[dict[str, object]]
    ) -> None:
        assert dataset_name == DATASET_NAME
        self.create_calls += 1
        for item in examples:
            self.examples[item["id"]] = item

    def update_examples(
        self, *, dataset_name: str, updates: list[dict[str, object]]
    ) -> None:
        assert dataset_name == DATASET_NAME
        self.update_calls += 1
        for item in updates:
            self.examples[item["id"]] = item


def test_ensure_dataset_creates_only_on_not_found() -> None:
    from tests.observability.langsmith_dataset_support import ensure_golden_dataset

    client = _FakeDatasetClient()
    created = ensure_golden_dataset(client)
    assert created == client.dataset
    again = ensure_golden_dataset(client)
    assert again == created


def test_ensure_dataset_does_not_swallow_auth_errors() -> None:
    from langsmith.utils import LangSmithAuthError

    from tests.observability.langsmith_dataset_support import ensure_golden_dataset

    class _AuthClient:
        def read_dataset(self, *, dataset_name: str) -> object:
            del dataset_name
            raise LangSmithAuthError("unauthorized")

    with pytest.raises(LangSmithAuthError):
        ensure_golden_dataset(_AuthClient())


def test_upsert_examples_creates_then_updates_stable_ids() -> None:
    from tests.observability.langsmith_dataset_support import (
        listed_example_ids,
        upsert_golden_examples,
    )

    client = _FakeDatasetClient()
    client.dataset = {"name": DATASET_NAME}
    first = upsert_golden_examples(client)
    assert client.create_calls == 1
    assert client.update_calls == 0
    assert listed_example_ids(client) == set(first)
    second = upsert_golden_examples(client)
    assert client.create_calls == 1
    assert client.update_calls == 1
    assert second == first
    assert listed_example_ids(client) == set(first)


def test_upsert_examples_does_not_swallow_create_failures() -> None:
    from tests.observability.langsmith_dataset_support import upsert_golden_examples

    class _FailCreate(_FakeDatasetClient):
        def create_examples(self, **_kwargs: object) -> None:
            raise RuntimeError("upsert failed")

    client = _FailCreate()
    client.dataset = {"name": DATASET_NAME}
    with pytest.raises(RuntimeError, match="upsert failed"):
        upsert_golden_examples(client)
