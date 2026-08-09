"""Deterministic UTF-8 plain-text and Markdown normalization."""

from __future__ import annotations

import re

from atlas.evidence.bounds import (
    MAX_NORMALIZED_TEXT_BYTES,
    MAX_RAW_TEXT_BYTES,
    PARSER_MARKDOWN_V1,
    PARSER_PLAIN_TEXT_V1,
    PARSER_SEARCH_SNIPPET_V1,
)
from atlas.evidence.errors import EvidenceTooLargeError, EvidenceValidationError

_MEDIA_PLAIN = "text/plain"
_MEDIA_MARKDOWN = "text/markdown"

_WHITESPACE_RE = re.compile(r"[ \t]+")
_NEWLINE_RE = re.compile(r"\n{3,}")
_FENCE_OPEN_RE = re.compile(r"^(?P<fence>`{3,}|~{3,})")


def accept_raw_text(text: str) -> bytes:
    """Validate and encode accepted raw text as UTF-8 bytes."""
    if not isinstance(text, str):
        raise EvidenceValidationError("text must be a string")
    try:
        raw = text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise EvidenceValidationError("text must be valid UTF-8") from exc
    if len(raw) == 0:
        raise EvidenceValidationError("text must be non-empty")
    if len(raw) > MAX_RAW_TEXT_BYTES:
        raise EvidenceTooLargeError("raw text exceeds maximum size")
    return raw


def normalize_plain_text(raw_utf8: bytes) -> tuple[str, str]:
    """Normalize plain text; return ``(normalized_text, parser_version)``."""
    text = _decode_raw(raw_utf8)
    normalized = _normalize_plain_whitespace(text)
    _assert_normalized_size(normalized)
    return normalized, PARSER_PLAIN_TEXT_V1


def normalize_markdown(raw_utf8: bytes) -> tuple[str, str]:
    """Normalize Markdown while preserving structural whitespace.

    Parser profile remains ``markdown.normalize.v1`` on this uncommitted Slice
    10A branch: no durable non-test data depends on the prior collapsing
    behavior, so keeping the version while correcting semantics is safe.

    Behavior:
    - CRLF/CR → LF
    - Strip outer leading/trailing document whitespace
    - Collapse runs of 3+ blank lines to a single blank line (``\\n\\n``)
      **outside** fenced code blocks
    - Preserve meaningful leading whitespace, internal repeated spaces,
      trailing spaces (Markdown hard breaks), fenced/indented code, nested
      list indentation, and table alignment/content
    - Reject whitespace-only normalized content
    """
    text = _decode_raw(raw_utf8)
    normalized = _normalize_markdown(text)
    if not normalized.strip():
        raise EvidenceValidationError("normalized markdown must contain non-whitespace")
    _assert_normalized_size(normalized)
    return normalized, PARSER_MARKDOWN_V1


def normalize_search_snippet(*, title: str, snippet: str) -> tuple[str, str, bytes]:
    """Build raw + normalized search-snippet body.

    Returns ``(normalized_text, parser_version, raw_utf8)``.
    """
    title_clean = title.strip()
    snippet_clean = snippet.strip()
    raw_text = f"{title_clean}\n\n{snippet_clean}".strip()
    raw = accept_raw_text(raw_text)
    normalized = _normalize_plain_whitespace(_decode_raw(raw))
    _assert_normalized_size(normalized)
    return normalized, PARSER_SEARCH_SNIPPET_V1, raw


def media_type_for_parser(parser_version: str) -> str:
    if parser_version == PARSER_MARKDOWN_V1:
        return _MEDIA_MARKDOWN
    return _MEDIA_PLAIN


def parser_for_media_type(media_type: str) -> str:
    cleaned = media_type.strip().lower()
    if cleaned == _MEDIA_MARKDOWN:
        return PARSER_MARKDOWN_V1
    if cleaned == _MEDIA_PLAIN:
        return PARSER_PLAIN_TEXT_V1
    raise EvidenceValidationError("media_type must be text/plain or text/markdown")


def normalize_for_media_type(raw_utf8: bytes, media_type: str) -> tuple[str, str]:
    parser = parser_for_media_type(media_type)
    if parser == PARSER_MARKDOWN_V1:
        return normalize_markdown(raw_utf8)
    return normalize_plain_text(raw_utf8)


def _decode_raw(raw_utf8: bytes) -> str:
    try:
        return raw_utf8.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceValidationError("raw bytes must be valid UTF-8") from exc


def _normalize_plain_whitespace(text: str) -> str:
    # Normalize newlines, strip trailing spaces per line, collapse blank runs.
    unified = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [_WHITESPACE_RE.sub(" ", line).rstrip() for line in unified.split("\n")]
    joined = "\n".join(lines).strip()
    return _NEWLINE_RE.sub("\n\n", joined)


def _normalize_markdown(text: str) -> str:
    unified = text.replace("\r\n", "\n").replace("\r", "\n").strip("\n")

    lines = unified.split("\n")
    out: list[str] = []
    fence_marker: str | None = None
    blank_run = 0

    for line in lines:
        if fence_marker is not None:
            out.append(line)
            closer = _FENCE_OPEN_RE.match(line)
            if closer is not None and closer.group("fence")[0] == fence_marker[0]:
                if len(closer.group("fence")) >= len(fence_marker):
                    fence_marker = None
            blank_run = 0
            continue

        open_fence = _FENCE_OPEN_RE.match(line)
        if open_fence is not None:
            fence_marker = open_fence.group("fence")
            out.append(line)
            blank_run = 0
            continue

        if line == "":
            blank_run += 1
            if blank_run <= 1:
                out.append(line)
            continue

        blank_run = 0
        out.append(line)

    return "\n".join(out)


def _assert_normalized_size(normalized: str) -> None:
    encoded = normalized.encode("utf-8")
    if not encoded:
        raise EvidenceValidationError("normalized text must be non-empty")
    if len(encoded) > MAX_NORMALIZED_TEXT_BYTES:
        raise EvidenceTooLargeError("normalized text exceeds maximum size")
