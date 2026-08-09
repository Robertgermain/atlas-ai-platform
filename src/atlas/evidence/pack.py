"""Drafter evidence-pack selection and display truncation.

The ``MAX_DRAFT_EVIDENCE_CONTEXT_BYTES`` budget applies to the sum of UTF-8
byte lengths of the evidence item display ``text`` fields after per-item
truncation. It does **not** include evidence IDs, source URIs, trust labels,
strength, or LangChain prompt framing outside those text fields.
"""

from __future__ import annotations

from atlas.evidence.bounds import (
    MAX_DRAFT_EVIDENCE_CONTEXT_BYTES,
    MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT,
    MAX_EVIDENCE_ITEMS_TO_DRAFTER,
)
from atlas.evidence.contracts import EvidenceContextItem, EvidenceItemView


def truncate_display_text(
    text: str,
    *,
    max_code_points: int = MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT,
) -> str:
    """Truncate display text to at most ``max_code_points`` Unicode code points.

    Slicing is performed on Python ``str`` (Unicode code points), so encoding
    the result never splits a multi-byte UTF-8 sequence mid-character.
    Stored evidence text must not be mutated by callers; this returns a copy.
    """
    if max_code_points < 0:
        raise ValueError("max_code_points must be non-negative")
    if len(text) <= max_code_points:
        return text
    return text[:max_code_points]


def utf8_byte_length(text: str) -> int:
    """Return the UTF-8 byte length of ``text``."""
    return len(text.encode("utf-8"))


def build_evidence_context_pack(
    views: list[EvidenceItemView],
    *,
    max_items: int = MAX_EVIDENCE_ITEMS_TO_DRAFTER,
    max_code_points_per_item: int = MAX_EVIDENCE_CHARS_PER_ITEM_IN_DRAFT,
    max_total_utf8_bytes: int = MAX_DRAFT_EVIDENCE_CONTEXT_BYTES,
) -> list[EvidenceContextItem]:
    """Build a ranked evidence pack under item-count, per-item, and byte caps.

    ``views`` must already be ordered by desired ranking (highest first).
    Lower-ranked items are skipped once the next truncated text would exceed
    the remaining UTF-8 byte budget.
    """
    pack: list[EvidenceContextItem] = []
    used_bytes = 0
    for view in views[:max_items]:
        display = truncate_display_text(
            view.text,
            max_code_points=max_code_points_per_item,
        )
        item_bytes = utf8_byte_length(display)
        if used_bytes + item_bytes > max_total_utf8_bytes:
            break
        pack.append(
            EvidenceContextItem(
                evidence_item_id=view.id,
                text=display,
                source_display_uri=view.display_uri,
                strength=view.strength,
                trust_label=view.trust_label,
            )
        )
        used_bytes += item_bytes
    return pack


def evidence_pack_text_utf8_bytes(pack: list[EvidenceContextItem]) -> int:
    """Sum UTF-8 bytes of display text fields in a pack."""
    return sum(utf8_byte_length(item.text) for item in pack)
