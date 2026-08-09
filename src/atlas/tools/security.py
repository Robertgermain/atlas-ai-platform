"""Basic URL validation for fake fetch (scheme/port/userinfo only)."""

from __future__ import annotations

from urllib.parse import urlparse

from atlas.tools.errors import ToolInvalidRequestError, ToolSsrfBlockedError

ALLOWED_SCHEMES = frozenset({"http", "https"})
ALLOWED_PORTS = frozenset({80, 443})


def parse_and_validate_url(url: str) -> tuple[str, str, int, str]:
    """Parse URL; reject bad schemes, userinfo, and ports. Does not resolve DNS."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in ALLOWED_SCHEMES:
        raise ToolSsrfBlockedError("unsupported URL scheme")
    if parsed.username is not None or parsed.password is not None:
        raise ToolSsrfBlockedError("credentials in URL are not allowed")
    hostname = parsed.hostname
    if hostname is None or not hostname.strip():
        raise ToolInvalidRequestError("URL host is required")
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    if port not in ALLOWED_PORTS:
        raise ToolSsrfBlockedError("URL port is not allowed")
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    normalized = f"{parsed.scheme}://{hostname}:{port}{path}{query}"
    return normalized, hostname, port, parsed.scheme
