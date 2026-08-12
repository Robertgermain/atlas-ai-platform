"""``atlas_worker_heartbeat_last_success_timestamp_seconds`` gauge (15A2 correction).

Uses a ``MagicMock`` Redis client double (no real Redis) so this stays a fast
unit test isolating only the gauge-update behavior; real Redis TTL/outage
behavior already has dedicated integration coverage in
``tests/integration/test_redis_heartbeat.py``.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import redis
from prometheus_client import CollectorRegistry

from atlas.coordination.heartbeat import RedisHeartbeatRecorder
from atlas.observability.metrics.catalog import AtlasMetrics


def _gauge_value(metrics: AtlasMetrics) -> float | None:
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name == "atlas_worker_heartbeat_last_success_timestamp_seconds":
                return sample.value
    return None


def test_successful_beat_marks_last_success_timestamp() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    client = MagicMock()
    recorder = RedisHeartbeatRecorder(client=client, ttl_seconds=15, metrics=metrics)

    before = time.time()
    recorder.beat(worker_id="worker-1")
    after = time.time()

    value = _gauge_value(metrics)
    assert value is not None
    assert before <= value <= after


def test_failed_beat_does_not_mark_last_success_timestamp() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    client = MagicMock()
    client.set.side_effect = redis.ConnectionError("unreachable")
    recorder = RedisHeartbeatRecorder(client=client, ttl_seconds=15, metrics=metrics)

    recorder.beat(worker_id="worker-1")

    assert _gauge_value(metrics) == 0.0


def test_later_failure_does_not_erase_a_prior_successful_timestamp() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    client = MagicMock()
    client.set.side_effect = [True, redis.ConnectionError("unreachable")]
    recorder = RedisHeartbeatRecorder(client=client, ttl_seconds=15, metrics=metrics)

    recorder.beat(worker_id="worker-1")
    first_value = _gauge_value(metrics)
    assert first_value is not None and first_value > 0.0

    recorder.beat(worker_id="worker-1")
    second_value = _gauge_value(metrics)

    assert second_value == first_value
