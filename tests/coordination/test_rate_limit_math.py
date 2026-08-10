"""Network-free unit tests for rate-limit boundary math."""

from __future__ import annotations

from atlas.coordination.rate_limit import retry_after_seconds_from_ttl_ms


def test_retry_after_rounds_up_partial_second() -> None:
    # 1001ms remaining should round up to 2s, not truncate to 1s (a caller
    # that retries at exactly the truncated boundary could still be denied).
    assert retry_after_seconds_from_ttl_ms(1001, window_seconds=60) == 2


def test_retry_after_exact_second_does_not_round_up_unnecessarily() -> None:
    assert retry_after_seconds_from_ttl_ms(2000, window_seconds=60) == 2


def test_retry_after_one_ms_rounds_up_to_one_second() -> None:
    assert retry_after_seconds_from_ttl_ms(1, window_seconds=60) == 1


def test_retry_after_zero_ttl_returns_one_second() -> None:
    # Window is about to expire; do not tell the caller to wait the full window.
    assert retry_after_seconds_from_ttl_ms(0, window_seconds=60) == 1


def test_retry_after_falls_back_to_window_for_redis_sentinels() -> None:
    # PTTL returns -2 (key missing) or -1 (no expiry).
    assert retry_after_seconds_from_ttl_ms(-2, window_seconds=60) == 60
    assert retry_after_seconds_from_ttl_ms(-1, window_seconds=60) == 60


def test_retry_after_falls_back_to_window_for_other_negatives() -> None:
    assert retry_after_seconds_from_ttl_ms(-99, window_seconds=60) == 60
