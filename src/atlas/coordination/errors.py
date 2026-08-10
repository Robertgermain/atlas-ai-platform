"""Typed errors for Slice 13A coordination features."""

from __future__ import annotations


class RateLimitExceededError(Exception):
    """Raised by the API layer when a caller exceeds the configured rate limit."""

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__("Rate limit exceeded.")
        self.retry_after_seconds = retry_after_seconds
