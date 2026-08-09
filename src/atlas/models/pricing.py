"""Versioned estimated-cost catalog for model invocations."""

from __future__ import annotations

from dataclasses import dataclass

from atlas.models.contracts import ProviderId

# Explicitly labeled estimates for local observability only — not billing truth.
PRICING_VERSION = "2026-08-09.v1"


@dataclass(frozen=True, slots=True)
class ModelPriceEstimate:
    """USD per 1M tokens estimates (input/output)."""

    input_usd_per_1m: float
    output_usd_per_1m: float


# Prices checked against public provider docs during Milestone 8.
# OpenAI: gpt-4o / gpt-4o-mini. Anthropic: Haiku/Sonnet families.
# Estimates only — not billing truth. Unknown models → null cost.
_ESTIMATES: dict[tuple[ProviderId, str], ModelPriceEstimate] = {
    (ProviderId.OPENAI, "gpt-4o-mini"): ModelPriceEstimate(0.15, 0.60),
    (ProviderId.OPENAI, "gpt-4o"): ModelPriceEstimate(2.50, 10.00),
    (ProviderId.ANTHROPIC, "claude-3-5-haiku-latest"): ModelPriceEstimate(0.80, 4.00),
    (ProviderId.ANTHROPIC, "claude-3-5-sonnet-latest"): ModelPriceEstimate(3.00, 15.00),
    (ProviderId.ANTHROPIC, "claude-sonnet-4-0"): ModelPriceEstimate(3.00, 15.00),
    (ProviderId.ANTHROPIC, "claude-haiku-4-5"): ModelPriceEstimate(1.00, 5.00),
}


def estimate_cost_usd(
    *,
    provider: ProviderId,
    model: str,
    input_tokens: int | None,
    output_tokens: int | None,
) -> tuple[float | None, str | None]:
    """Return (estimated_cost_usd, pricing_version) or (None, None) if unknown."""
    if provider is ProviderId.FAKE:
        return None, None
    price = _ESTIMATES.get((provider, model))
    if price is None or input_tokens is None or output_tokens is None:
        return None, None
    cost = (
        input_tokens * price.input_usd_per_1m + output_tokens * price.output_usd_per_1m
    ) / 1_000_000
    return round(cost, 8), PRICING_VERSION
