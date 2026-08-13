"""Network-free LangSmith Client doubles for Slice 15B unit tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.observability.langsmith import (
    LangSmithHandle,
    configure_langsmith,
    reset_langsmith_for_tests,
)
from atlas.observability.metrics.catalog import AtlasMetrics

_DUMMY_LANGSMITH_KEY = "lsv2_test_not_a_real_key"


def isolate_langsmith_settings_environment(monkeypatch: Any, tmp_path: Path) -> None:
    """Remove LangSmith process env and the repository ``.env`` before Settings().

    Does not touch ``ATLAS_ENABLE_LIVE_LANGSMITH_TESTS``. Call this before
    constructing ``Settings()``, then pass any dummy key the test itself needs.
    """
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_PROJECT", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_URL", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_TIMEOUT_MS", raising=False)
    monkeypatch.chdir(tmp_path)


class DummyResponse:
    status_code = 200
    headers: dict[str, str] = {"content-type": "application/json"}
    text = "{}"
    content = b"{}"

    def json(self) -> dict[str, object]:
        return {}

    def raise_for_status(self) -> None:
        return None


class DummySession:
    """Accepts LangSmith SDK HTTP calls without a network.

    Records only method+URL, never request bodies, so a failing assertion
    cannot dump prompts, keys, or graph state.
    """

    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []
        self.headers: dict[str, str] = {}

    def mount(self, *_args: object, **_kwargs: object) -> None:
        return None

    def request(self, method: str, url: str, **_kwargs: object) -> DummyResponse:
        self.requests.append((method, url))
        return DummyResponse()

    def close(self) -> None:
        return None


def arm_dummy_langsmith(
    monkeypatch: Any,
    tmp_path: Path,
    *,
    metrics: AtlasMetrics | None = None,
) -> tuple[LangSmithHandle, DummySession]:
    """Configure the process handle with a dummy session (no hosted API)."""
    reset_langsmith_for_tests()
    isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    session = DummySession()
    from langsmith import Client as RealClient

    def _client(**kwargs: object) -> RealClient:
        kwargs["session"] = session
        kwargs["api_url"] = "http://127.0.0.1:9"
        return RealClient(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("atlas.observability.langsmith.client.Client", _client)
    catalog = metrics or AtlasMetrics(CollectorRegistry())
    handle = configure_langsmith(
        Settings(langsmith_api_key=SecretStr(_DUMMY_LANGSMITH_KEY)),
        metrics=catalog,
    )
    return handle, session
