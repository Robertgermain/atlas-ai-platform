"""Unit tests for URL canonicalization, hashing, normalize, chunking, and packs."""

from __future__ import annotations

import pytest

from atlas.evidence.bounds import (
    MAX_DRAFT_EVIDENCE_CONTEXT_BYTES,
    MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT,
    PARSER_MARKDOWN_V1,
)
from atlas.evidence.chunk import chunk_normalized_text
from atlas.evidence.contracts import (
    EvidenceItemView,
    EvidenceStrength,
    MediaType,
    SourceKind,
    TrustClass,
)
from atlas.evidence.errors import (
    EvidenceTooLargeError,
    EvidenceValidationError,
    UrlCanonicalizationError,
)
from atlas.evidence.hash import (
    document_content_sha256,
    evidence_item_content_sha256,
)
from atlas.evidence.normalize import (
    accept_raw_text,
    normalize_markdown,
    normalize_plain_text,
)
from atlas.evidence.pack import (
    build_evidence_context_pack,
    evidence_pack_text_utf8_bytes,
    truncate_display_text,
    utf8_byte_length,
)
from atlas.evidence.url import canonicalize_http_url


def test_canonicalize_lowercases_scheme_and_host_strips_fragment() -> None:
    canonical, display = canonicalize_http_url("HTTPS://Example.COM/Path?q=1#section")
    assert canonical == "https://example.com/Path?q=1"
    assert display == "HTTPS://Example.COM/Path?q=1#section"


def test_canonicalize_removes_default_ports() -> None:
    http_c, _ = canonicalize_http_url("http://example.com:80/a")
    https_c, _ = canonicalize_http_url("https://example.com:443/a")
    assert http_c == "http://example.com/a"
    assert https_c == "https://example.com/a"


def test_canonicalize_preserves_non_default_port_and_query() -> None:
    canonical, _ = canonicalize_http_url("https://example.com:8443/x?b=2&a=1")
    assert canonical == "https://example.com:8443/x?b=2&a=1"


def test_canonicalize_rejects_userinfo_and_non_http() -> None:
    with pytest.raises(UrlCanonicalizationError):
        canonicalize_http_url("https://user:pass@example.com/")
    with pytest.raises(UrlCanonicalizationError):
        canonicalize_http_url("ftp://example.com/file")


def test_canonicalize_rejects_non_numeric_and_out_of_range_ports() -> None:
    with pytest.raises(UrlCanonicalizationError, match="port"):
        canonicalize_http_url("https://example.com:abc/path")
    with pytest.raises(UrlCanonicalizationError, match="port"):
        canonicalize_http_url("https://example.com:99999/path")
    with pytest.raises(UrlCanonicalizationError, match="port"):
        canonicalize_http_url("http://example.com:-1/")


def test_canonicalize_rejects_ipv6() -> None:
    with pytest.raises(UrlCanonicalizationError, match="IPv6"):
        canonicalize_http_url("http://[2001:db8::1]/")
    with pytest.raises(UrlCanonicalizationError, match="IPv6"):
        canonicalize_http_url("https://[::1]:8443/x")


def test_document_hash_is_raw_bytes_not_normalized() -> None:
    raw = accept_raw_text("Hello   world\n\n")
    normalized, _ = normalize_plain_text(raw)
    assert document_content_sha256(raw) != evidence_item_content_sha256(normalized)
    assert document_content_sha256(raw) == document_content_sha256(raw)


def test_chunking_uses_unicode_code_points_and_overlap() -> None:
    text = "a" * 1000
    chunks = chunk_normalized_text(text, chunk_size=800, overlap=100)
    assert len(chunks) == 2
    assert chunks[0].char_start == 0
    assert chunks[0].char_end == 800
    assert chunks[1].char_start == 700
    assert len(chunks[0].text) == 800
    assert chunks[0].content_sha256 != document_content_sha256(text.encode("utf-8"))


def test_chunking_rejects_too_many_chunks() -> None:
    text = "x" * 10_000
    with pytest.raises(EvidenceTooLargeError):
        chunk_normalized_text(text, chunk_size=100, overlap=0, max_chunks=2)


def test_markdown_preserves_fenced_code_and_internal_spaces() -> None:
    raw = accept_raw_text(
        "# Title\n\nParagraph  with   spaces\n\n```python\na  =  1\n\n\n\nb = 2\n```\n"
    )
    normalized, version = normalize_markdown(raw)
    assert version == PARSER_MARKDOWN_V1
    assert "Paragraph  with   spaces" in normalized
    assert "a  =  1" in normalized
    # Blank lines inside the fence are preserved (more than two).
    fence_body = normalized.split("```python\n", 1)[1].split("\n```", 1)[0]
    assert "\n\n\n" in fence_body


def test_markdown_preserves_indented_code_nested_lists_and_tables() -> None:
    raw = accept_raw_text(
        "Intro\n\n"
        "    def hello():\n"
        "        return  42\n\n"
        "- item\n"
        "  - nested\n"
        "    - deeper\n\n"
        "| a | b |\n"
        "|---|---|\n"
        "| 1 |  2 |\n"
    )
    normalized, _ = normalize_markdown(raw)
    assert "    def hello():" in normalized
    assert "        return  42" in normalized
    assert "  - nested" in normalized
    assert "    - deeper" in normalized
    assert "| 1 |  2 |" in normalized


def test_markdown_normalizes_crlf_and_outer_newlines() -> None:
    raw = b"\r\n# Title\r\n\r\nBody  text\r\n"
    normalized, _ = normalize_markdown(raw)
    assert "\r" not in normalized
    assert normalized.startswith("# Title")
    assert "Body  text" in normalized
    assert document_content_sha256(raw) != evidence_item_content_sha256(normalized)


def test_markdown_rejects_whitespace_only_content() -> None:
    with pytest.raises(EvidenceValidationError, match="non-whitespace"):
        normalize_markdown(b"   \n\n\t  \n")
    with pytest.raises(EvidenceValidationError, match="non-whitespace"):
        normalize_markdown(b"\r\n  \r\n")


def test_markdown_collapses_blank_runs_outside_fences_only() -> None:
    raw = accept_raw_text("A\n\n\n\nB\n\n\nC")
    normalized, _ = normalize_markdown(raw)
    assert normalized == "A\n\nB\n\nC"


def _view(item_id: str, text: str) -> EvidenceItemView:
    return EvidenceItemView(
        id=item_id,
        document_id="doc",
        source_id="src",
        source_kind=SourceKind.CORPUS_TEXT,
        canonical_uri="corpus:pack",
        display_uri="corpus:pack",
        trust_class=TrustClass.OPERATOR_CORPUS,
        ordinal=0,
        text=text,
        content_sha256="a" * 64,
        char_start=0,
        char_end=len(text),
        strength=EvidenceStrength.DOCUMENT_CHUNK,
        trust_label="[operator_corpus]",
        document_content_sha256="b" * 64,
        parser_version="plain_text.normalize.v1",
        media_type=MediaType.TEXT_PLAIN,
    )


def test_evidence_pack_enforces_item_char_and_byte_caps_ascii() -> None:
    # Three items: first two fit under a tiny budget; third exceeds remaining.
    views = [
        _view("1", "aaaa"),  # 4 bytes
        _view("2", "bbbb"),  # 4 bytes
        _view("3", "cccc"),  # 4 bytes — should be dropped when budget is 8
    ]
    pack = build_evidence_context_pack(views, max_total_utf8_bytes=8)
    assert [item.evidence_item_id for item in pack] == ["1", "2"]
    assert evidence_pack_text_utf8_bytes(pack) == 8
    assert evidence_pack_text_utf8_bytes(pack) <= MAX_DRAFT_EVIDENCE_CONTEXT_BYTES


def test_evidence_pack_truncates_per_item_and_stops_on_byte_budget_multibyte() -> None:
    emoji = "😀"  # 4 UTF-8 bytes, 1 code point
    long_text = emoji * (MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT + 50)
    stored = long_text  # must remain unchanged by truncation helper
    truncated = truncate_display_text(stored)
    assert len(truncated) == MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT
    assert stored == long_text
    assert utf8_byte_length(truncated) == MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT * 4

    # Budget allows exactly one truncated emoji item (6000 bytes), not two.
    one_item_bytes = utf8_byte_length(truncated)
    views = [_view("1", long_text), _view("2", long_text), _view("3", "ascii-tail")]
    pack = build_evidence_context_pack(
        views,
        max_total_utf8_bytes=one_item_bytes + 10,
    )
    assert [item.evidence_item_id for item in pack] == ["1"]
    assert evidence_pack_text_utf8_bytes(pack) == one_item_bytes
    assert evidence_pack_text_utf8_bytes(pack) <= MAX_DRAFT_EVIDENCE_CONTEXT_BYTES
    # Stored view text was not mutated.
    assert views[0].text == long_text


def test_evidence_pack_respects_max_items_and_default_byte_cap() -> None:
    views = [_view(str(i), "x" * 1000) for i in range(12)]
    pack = build_evidence_context_pack(views)
    assert len(pack) == 8
    assert evidence_pack_text_utf8_bytes(pack) <= MAX_DRAFT_EVIDENCE_CONTEXT_BYTES
    assert evidence_pack_text_utf8_bytes(pack) == 8000
