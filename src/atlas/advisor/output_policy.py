"""Post-parse advisory output policy. Defense in depth, not the primary control.

Structural absence of mutation ports and a READ ONLY snapshot transaction
remain the primary controls. This module rejects unsafe text after parse.
"""

from __future__ import annotations

import json
import re

from atlas.advisor.catalogs import MAX_OUTPUT_UTF8_BYTES
from atlas.advisor.contracts import AdvisoryAnalysis, AdvisoryIncidentFacts
from atlas.advisor.errors import AdvisoryOutputRejectedError
from atlas.models.errors import ModelInvalidStructuredOutputError

_BANNED_SUBSTRINGS = (
    "http://",
    "https://",
    "www.",
    "```",
    "sk-",
    "bearer ",
    "api_key",
    "authorization:",
    "$ ",
    "curl ",
    "kubectl ",
    "docker ",
    "ssh ",
    "python -m atlas.",
    "/v1/research-jobs",
    "alembic ",
)
_CLAIM_PHRASES = (
    "i restarted",
    "has been retried",
    "alert acknowledged",
    "i replayed",
    "deployed",
    "configuration changed",
    "successfully cancelled",
    "i executed",
    "action taken",
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
_BANNED_TOKENS = frozenset({"replay"})


def validate_advisory_output(
    facts: AdvisoryIncidentFacts, analysis: AdvisoryAnalysis
) -> None:
    """Fail closed on unknown signal IDs, banned text, or oversize output."""
    allowed = {item.signal_id for item in facts.signals}
    for hypothesis in analysis.hypotheses:
        _require_ids(hypothesis.signal_ids, allowed)
        _scan_text(hypothesis.statement)
    for recommendation in analysis.recommendations:
        _require_ids(recommendation.signal_ids, allowed)
        _scan_text(recommendation.step)
    _scan_text(analysis.incident_summary)
    for item in analysis.limitations:
        _scan_text(item)
    for item in analysis.unknowns:
        _scan_text(item)
    encoded = json.dumps(
        analysis.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    if len(encoded.encode("utf-8")) > MAX_OUTPUT_UTF8_BYTES:
        raise AdvisoryOutputRejectedError("advisory output exceeds byte bound")


def _require_ids(signal_ids: list[str], allowed: set[str]) -> None:
    for item in signal_ids:
        if item not in allowed:
            raise ModelInvalidStructuredOutputError()


def _scan_text(value: str) -> None:
    lowered = " ".join(value.casefold().split())
    for banned in _BANNED_SUBSTRINGS:
        if banned in lowered:
            raise AdvisoryOutputRejectedError("advisory output failed safety policy")
    for phrase in _CLAIM_PHRASES:
        if phrase in lowered:
            raise AdvisoryOutputRejectedError("advisory output claimed an action")
    tokens = {match.group(0).casefold() for match in _TOKEN_RE.finditer(value)}
    if tokens & _BANNED_TOKENS:
        raise AdvisoryOutputRejectedError("advisory output failed safety policy")
