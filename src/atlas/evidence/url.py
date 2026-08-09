"""Conservative URL canonicalization for web-search source identity."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from atlas.evidence.bounds import MAX_SOURCE_URI_CHARS
from atlas.evidence.errors import UrlCanonicalizationError


def canonicalize_http_url(raw_url: str) -> tuple[str, str]:
    """Return ``(canonical_uri, display_uri)`` for an http(s) URL.

    Rules:
    - Lowercase scheme and hostname
    - Remove fragments
    - Normalize default ports (http/80, https/443)
    - Preserve path and query parameters
    - Reject non-http(s), userinfo, empty host, overlong URIs
    - Reject non-numeric or out-of-range ports via ``UrlCanonicalizationError``
    - Reject IPv6 literals in Milestone 10A (bracketed or colon-form hosts)

    Invalid ports and IPv6 failures never expose raw exception text.
    """
    display = raw_url.strip()
    if not display:
        raise UrlCanonicalizationError("URL must be non-empty")
    if len(display) > MAX_SOURCE_URI_CHARS:
        raise UrlCanonicalizationError("URL exceeds maximum length")

    parts = urlsplit(display)
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise UrlCanonicalizationError("URL scheme must be http or https")
    if parts.username is not None or parts.password is not None:
        raise UrlCanonicalizationError("URL must not include userinfo")

    # Milestone 10A: reject IPv6 rather than risk incorrect bracket reconstruction.
    netloc = parts.netloc
    host_part = netloc.rsplit("@", 1)[-1]
    if host_part.startswith("["):
        raise UrlCanonicalizationError("IPv6 URLs are not supported")

    hostname = parts.hostname
    if hostname is None or not hostname.strip():
        raise UrlCanonicalizationError("URL must include a hostname")
    if ":" in hostname:
        raise UrlCanonicalizationError("IPv6 URLs are not supported")
    host = hostname.lower()

    try:
        port = parts.port
    except ValueError:
        raise UrlCanonicalizationError("URL port is invalid") from None

    if port is not None:
        if port < 0 or port > 65535:
            raise UrlCanonicalizationError("URL port is invalid")
        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            canonical_netloc = host
        else:
            canonical_netloc = f"{host}:{port}"
    else:
        canonical_netloc = host

    path = parts.path if parts.path else ""
    query = parts.query
    canonical = urlunsplit((scheme, canonical_netloc, path, query, ""))
    if len(canonical) > MAX_SOURCE_URI_CHARS:
        raise UrlCanonicalizationError("Canonical URL exceeds maximum length")
    return canonical, display
