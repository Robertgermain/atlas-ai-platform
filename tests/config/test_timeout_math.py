"""Network-free unit tests for ``atlas.config.timeout_math`` (correction pass,
Slice 13C2B).

Proves the ceiling-with-floor-of-1 conversion never produces a 0 effective
timeout for any positive configured value, and that it ceiling-rounds
(never banker's/nearest-rounds) fractional values.
"""

from __future__ import annotations

import pytest

from atlas.config.timeout_math import (
    effective_connect_timeout_seconds,
    effective_statement_timeout_ms,
    effective_statement_timeout_seconds,
)


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (5.0, 5),
        (1.0, 1),
        (3.4, 4),  # ceiling, not round-to-nearest (would be 3)
        (2.5, 3),  # ceiling, not banker's rounding (round() rounds 2.5 to 2)
        (0.2, 1),  # sub-second: must floor at 1, never 0
        (0.9999, 1),
        (1e-9, 1),  # arbitrarily tiny positive: still never 0
    ],
)
def test_effective_connect_timeout_seconds_ceiling_rounds_and_floors_at_one(
    configured: float, expected: int
) -> None:
    assert effective_connect_timeout_seconds(configured) == expected


def test_effective_connect_timeout_seconds_is_never_zero_for_any_positive_input() -> (
    None
):
    for configured in (1e-12, 0.001, 0.01, 0.1, 0.4999, 0.5, 0.5001, 0.9999):
        assert effective_connect_timeout_seconds(configured) >= 1


@pytest.mark.parametrize(
    ("configured_seconds", "expected_ms"),
    [
        (5.0, 5000),
        (2.5, 2500),
        (0.0025, 3),  # 2.5ms ceiling-rounds to 3, never 2 (banker's) or 0
        (0.0001, 1),  # 0.1ms: must floor at 1, never 0
        (1e-9, 1),  # arbitrarily tiny positive: still never 0
    ],
)
def test_effective_statement_timeout_ms_ceiling_rounds_and_floors_at_one(
    configured_seconds: float, expected_ms: int
) -> None:
    assert effective_statement_timeout_ms(configured_seconds) == expected_ms


def test_effective_statement_timeout_ms_is_never_zero_for_any_positive_input() -> None:
    for configured_seconds in (1e-12, 1e-6, 0.0001, 0.0004999, 0.0005, 0.0009999):
        assert effective_statement_timeout_ms(configured_seconds) >= 1


def test_effective_statement_timeout_seconds_matches_the_ms_conversion() -> None:
    assert effective_statement_timeout_seconds(5.0) == 5.0
    assert effective_statement_timeout_seconds(2.5) == 2.5
    # 0.0025s -> ceil(2.5ms) == 3ms -> 0.003s (never 0.0025s, and never 0).
    assert effective_statement_timeout_seconds(0.0025) == pytest.approx(0.003)
    assert effective_statement_timeout_seconds(1e-9) == pytest.approx(0.001)
    assert effective_statement_timeout_seconds(1e-9) > 0
