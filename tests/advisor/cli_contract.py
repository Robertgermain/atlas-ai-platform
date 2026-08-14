"""Shared CLI stdout/stderr contract assertions for Slice 15C2 tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from atlas.advisor.contracts import AdvisoryStdoutEnvelope
from atlas.observability.events import Event

_STRUCTURED_KEYS = frozenset(
    {
        "timestamp",
        "severity",
        "service",
        "event",
        "trace_id",
        "span_id",
        "research_job_id",
        "workflow_execution_id",
        "node_name",
        "model_invocation_id",
        "tool_invocation_id",
        "evaluation_run_id",
        "outbox_event_id",
        "consumer_event_id",
        "error_class",
        "duration_ms",
        "outcome",
    }
)
_UNSTRUCTURED_KEYS = frozenset(
    {
        "timestamp",
        "severity",
        "service",
        "event",
        "logger_category",
    }
)

SECRET_CANARY = "sk-fakekey-hunter2-cli"
QUESTION_CANARY = "What is my SSN 123-45-6789 and password hunter2?"
FIXTURE_CANARY = "sekret.json"


def assert_no_raw_argparse(text: str) -> None:
    lowered = text.lower()
    assert "usage:" not in lowered
    assert "error: the following arguments" not in lowered
    assert "unrecognized arguments" not in lowered
    assert "error: argument" not in lowered
    assert "Traceback (most recent call last)" not in text


def assert_canaries_absent(*texts: str) -> None:
    blob = "\n".join(texts)
    for canary in (SECRET_CANARY, QUESTION_CANARY, FIXTURE_CANARY, "sekret"):
        assert canary not in blob


def parse_atlas_stderr(text: str) -> list[dict[str, Any]]:
    assert_no_raw_argparse(text)
    parsed: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        assert isinstance(payload, dict)
        keys = frozenset(payload)
        assert keys == _STRUCTURED_KEYS or keys == _UNSTRUCTURED_KEYS
        assert payload["service"] == "advisor"
        parsed.append(payload)
    return parsed


def assert_failure_streams(stdout: str, stderr: str) -> list[dict[str, Any]]:
    assert stdout == ""
    lines = parse_atlas_stderr(stderr)
    assert any(
        item.get("event") == Event.ADVISORY_INPUT_REJECTED.value for item in lines
    )
    return lines


def assert_success_streams(stdout: str, stderr: str) -> AdvisoryStdoutEnvelope:
    nonempty = [line for line in stdout.splitlines() if line.strip()]
    assert len(nonempty) == 1
    payload = json.loads(nonempty[0])
    assert isinstance(payload, dict)
    assert "event" not in payload
    assert "service" not in payload
    envelope = AdvisoryStdoutEnvelope.model_validate(payload)
    parse_atlas_stderr(stderr)
    return envelope


def run_advisor_cli(
    args: list[str],
    *,
    cwd: Path,
    extra_env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("ATLAS_ENABLE_LIVE_"):
            env.pop(key, None)
    env.pop("ATLAS_OPENAI_API_KEY", None)
    env.pop("ATLAS_LANGSMITH_API_KEY", None)
    env.pop("ATLAS_ANTHROPIC_API_KEY", None)
    env["ATLAS_ADVISORY_MODE"] = "fake"
    env["ATLAS_MODEL_PROVIDER"] = "fake"
    env["ATLAS_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = ""
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-m", "atlas.advisor", *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
        timeout=60,
        check=False,
    )
