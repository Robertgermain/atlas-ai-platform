"""Basic URL validation used by deterministic fake fetch."""

from __future__ import annotations

import pytest

from atlas.tools.errors import ToolSsrfBlockedError
from atlas.tools.security import parse_and_validate_url


def test_rejects_non_http_scheme() -> None:
    with pytest.raises(ToolSsrfBlockedError):
        parse_and_validate_url("ftp://example.com/x")


def test_rejects_userinfo() -> None:
    with pytest.raises(ToolSsrfBlockedError):
        parse_and_validate_url("https://user:pass@example.com/")


def test_rejects_non_allowlisted_port() -> None:
    with pytest.raises(ToolSsrfBlockedError):
        parse_and_validate_url("https://example.com:8080/")


def test_accepts_https_default_port() -> None:
    normalized, hostname, port, scheme = parse_and_validate_url(
        "https://example.com/path"
    )
    assert scheme == "https"
    assert hostname == "example.com"
    assert port == 443
    assert normalized.startswith("https://example.com:443/")
