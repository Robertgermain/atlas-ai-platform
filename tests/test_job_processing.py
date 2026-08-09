"""Unit tests for deterministic research processing."""

from atlas.application.job_processing import process_research_question


def test_process_research_question_format() -> None:
    assert (
        process_research_question("What is Atlas?")
        == "Research completed for: What is Atlas?"
    )
