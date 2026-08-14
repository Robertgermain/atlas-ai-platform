"""Settings default for the CLI-only advisory mode."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.config.settings import Settings


def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ATLAS_ADVISORY_MODE", raising=False)
    monkeypatch.chdir(tmp_path)


def test_advisory_mode_defaults_to_fake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings()
    assert settings.advisory_mode == "fake"
