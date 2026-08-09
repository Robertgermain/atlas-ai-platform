"""Composition and live-fetch fail-closed unit tests."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.tools.composition import build_live_registry
from atlas.tools.contracts import ToolId
from atlas.tools.errors import ToolAuthConfigError


def test_tavily_without_fetch_omits_fetch_tool() -> None:
    registry = build_live_registry(
        Settings(
            tool_provider="tavily",
            tool_fetch_enabled=False,
            tavily_api_key=SecretStr("test-key"),
        )
    )
    assert ToolId.WEB_SEARCH in registry.list_ids()
    assert ToolId.FETCH_URL not in registry.list_ids()


def test_tavily_fetch_enabled_fails_composition() -> None:
    with pytest.raises(ToolAuthConfigError):
        build_live_registry(
            Settings(
                tool_provider="tavily",
                tool_fetch_enabled=True,
                tavily_api_key=SecretStr("test-key"),
            )
        )
