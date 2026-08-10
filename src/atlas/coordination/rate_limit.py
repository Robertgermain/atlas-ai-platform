"""Redis-backed fixed-window rate limiting.

Window semantics: the window for a given identity starts at that identity's
first request and lasts ``window_seconds``. A single Redis Lua script
performs ``INCR`` + conditional ``PEXPIRE`` (set only on the first increment)
atomically, so concurrent requests against the same identity are counted
exactly once each with no race between the increment and the expiry set.
This is a *fixed* window in the sense that once started it does not slide on
subsequent requests (unlike a sliding-log limiter), but the window boundary
is anchored to the first request rather than wall-clock-aligned buckets;
that avoids a burst exactly at a wall-clock boundary from resetting the
count early while keeping the implementation to a single key per identity.
"""

from __future__ import annotations

import logging

import redis

from atlas.coordination.contracts import RateLimitDecision
from atlas.coordination.outage_log import OncePerOutageLogger

logger = logging.getLogger(__name__)

_KEY_PREFIX = "atlas:ratelimit:v1:create_research_job"

_SCRIPT = """
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
local ttl = redis.call("PTTL", KEYS[1])
return {current, ttl}
"""


def retry_after_seconds_from_ttl_ms(ttl_ms: int, *, window_seconds: int) -> int:
    """Convert a Redis PTTL reading into a whole-second Retry-After value.

    - ``ttl_ms > 0``: round upward so callers never retry before reset.
    - ``ttl_ms == 0``: return 1 (window about to expire; not the full window).
    - Redis sentinels ``-1`` / ``-2`` and any other negative: full window.
    """
    if ttl_ms > 0:
        return (ttl_ms + 999) // 1000
    if ttl_ms == 0:
        return 1
    return window_seconds


class RedisFixedWindowRateLimiter:
    """Concurrency-safe fixed-window rate limiter. Fails open on Redis errors."""

    def __init__(
        self,
        *,
        client: redis.Redis,
        max_requests: int,
        window_seconds: int,
        key_prefix: str = _KEY_PREFIX,
    ) -> None:
        self._client = client
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._window_ms = window_seconds * 1000
        self._key_prefix = key_prefix
        self._script = client.register_script(_SCRIPT)
        self._outage_log = OncePerOutageLogger(
            logger,
            warning_message=(
                "Redis rate-limit check failed; failing open (allowing request)."
            ),
        )

    def check(self, *, identity: str) -> RateLimitDecision:
        key = f"{self._key_prefix}:{identity}"
        try:
            raw_result = self._script(keys=[key], args=[self._window_ms])
        except redis.RedisError:
            self._outage_log.note_failure()
            return RateLimitDecision(allowed=True, retry_after_seconds=0)

        self._outage_log.note_success()
        current = int(raw_result[0])
        ttl_ms = int(raw_result[1])
        if current <= self._max_requests:
            return RateLimitDecision(allowed=True, retry_after_seconds=0)
        retry_after = retry_after_seconds_from_ttl_ms(
            ttl_ms, window_seconds=self._window_seconds
        )
        return RateLimitDecision(allowed=False, retry_after_seconds=retry_after)
