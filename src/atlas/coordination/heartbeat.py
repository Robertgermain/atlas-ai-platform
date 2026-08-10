"""Redis-backed worker heartbeat recorder. Fails open on Redis errors."""

from __future__ import annotations

import logging

import redis

from atlas.coordination.outage_log import OncePerOutageLogger

logger = logging.getLogger(__name__)

_KEY_PREFIX = "atlas:heartbeat:v1:worker"


class RedisHeartbeatRecorder:
    """Write a TTL-bound liveness key for a worker process.

    The key expires automatically after ``ttl_seconds`` if the worker stops
    refreshing it (crash, hang, or network partition). PostgreSQL claim/lease
    fencing remains the sole authority for job ownership; this heartbeat is
    an observability/liveness signal only.
    """

    def __init__(self, *, client: redis.Redis, ttl_seconds: int) -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._outage_log = OncePerOutageLogger(
            logger,
            warning_message="Redis heartbeat write failed; continuing (fail-open).",
        )

    def beat(self, *, worker_id: str) -> None:
        key = f"{_KEY_PREFIX}:{worker_id}"
        try:
            self._client.set(key, "1", ex=self._ttl_seconds)
        except redis.RedisError:
            self._outage_log.note_failure()
            return
        self._outage_log.note_success()
