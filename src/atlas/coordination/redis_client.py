"""Build a pooled, bounded-timeout Redis client from settings."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

import redis

if TYPE_CHECKING:
    from atlas.config.settings import Settings


@lru_cache(maxsize=8)
def _cached_client(
    url: str,
    connect_timeout_seconds: float,
    socket_timeout_seconds: float,
) -> redis.Redis:
    return redis.Redis.from_url(
        url,
        socket_connect_timeout=connect_timeout_seconds,
        socket_timeout=socket_timeout_seconds,
    )


def build_redis_client(settings: Settings) -> redis.Redis:
    """Return a cached, pooled Redis client with bounded connect/socket timeouts.

    Timeouts are intentionally short: coordination features must fail open
    quickly rather than add meaningful latency to a request or worker loop
    when Redis is slow or unavailable. The client is cached per
    (url, timeouts) tuple so repeated calls (e.g. once per API request) reuse
    the same connection pool instead of opening a new one each time.
    """
    return _cached_client(
        settings.redis_url,
        settings.redis_connect_timeout_seconds,
        settings.redis_socket_timeout_seconds,
    )


def reset_redis_client_cache() -> None:
    """Clear the cached Redis client(s) (for tests)."""
    _cached_client.cache_clear()
