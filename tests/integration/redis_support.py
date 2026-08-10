"""Integration-test-only Redis connection and key-cleanup helpers.

These helpers must never be imported by production application code. Unlike
the PostgreSQL integration guard (which checks the database name), Redis has
no per-test-database naming convention, so isolation is achieved by only
ever deleting keys under Atlas's own namespaced prefixes.
"""

from __future__ import annotations

import os

import redis

DEFAULT_TEST_REDIS_URL = "redis://127.0.0.1:6380/0"

ATLAS_KEY_PATTERNS = (
    "atlas:ratelimit:v1:*",
    "atlas:heartbeat:v1:*",
)


def test_redis_url() -> str:
    return os.environ.get("ATLAS_REDIS_URL", DEFAULT_TEST_REDIS_URL)


def build_test_redis_client() -> redis.Redis:
    return redis.Redis.from_url(
        test_redis_url(),
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
    )


def cleanup_atlas_keys(client: redis.Redis) -> None:
    """Delete only Atlas-namespaced keys, never a blind FLUSHDB."""
    for pattern in ATLAS_KEY_PATTERNS:
        keys = list(client.scan_iter(match=pattern, count=500))
        if keys:
            client.delete(*keys)
