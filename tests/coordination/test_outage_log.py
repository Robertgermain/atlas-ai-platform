"""Deterministic unit tests for once-per-outage Redis logging."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import redis

from atlas.coordination.heartbeat import RedisHeartbeatRecorder
from atlas.coordination.outage_log import OncePerOutageLogger
from atlas.coordination.rate_limit import RedisFixedWindowRateLimiter
from atlas.observability.events import Event
from atlas.observability.logging import AtlasJSONFormatter


def test_once_per_outage_logger_emits_one_warning_then_silences(
    caplog: Any,
) -> None:
    log = logging.getLogger("atlas.test.outage")
    outage = OncePerOutageLogger(
        log, event=Event.DEPENDENCY_OPERATION_FAILED_OPEN, outcome="redis_test"
    )
    with caplog.at_level(logging.WARNING, logger="atlas.test.outage"):
        outage.note_failure()
        outage.note_failure()
        outage.note_failure()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].event == Event.DEPENDENCY_OPERATION_FAILED_OPEN.value
    assert warnings[0].outcome == "redis_test"
    assert outage.in_outage is True


def test_once_per_outage_logger_recovery_allows_one_later_warning(
    caplog: Any,
) -> None:
    log = logging.getLogger("atlas.test.outage.recover")
    outage = OncePerOutageLogger(
        log, event=Event.DEPENDENCY_OPERATION_FAILED_OPEN, outcome="redis_test"
    )
    with caplog.at_level(logging.WARNING, logger="atlas.test.outage.recover"):
        outage.note_failure()
        outage.note_failure()
        outage.note_success()
        outage.note_failure()
        outage.note_failure()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2


def test_rate_limiter_logs_once_per_outage_episode(caplog: Any) -> None:
    client = MagicMock()
    client.register_script.return_value = MagicMock(
        side_effect=redis.ConnectionError("boom")
    )
    limiter = RedisFixedWindowRateLimiter(
        client=client, max_requests=10, window_seconds=60
    )
    with caplog.at_level(logging.WARNING, logger="atlas.coordination.rate_limit"):
        for _ in range(5):
            decision = limiter.check(identity="1.2.3.4")
            assert decision.allowed is True
    warnings = [
        r
        for r in caplog.records
        if r.name == "atlas.coordination.rate_limit" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].event == Event.DEPENDENCY_OPERATION_FAILED_OPEN.value
    assert warnings[0].outcome == "redis_rate_limit_check"
    rendered = AtlasJSONFormatter().format(warnings[0])
    assert "boom" not in rendered
    assert "redis://" not in rendered


def test_rate_limiter_recovery_allows_later_warning(caplog: Any) -> None:
    client = MagicMock()
    script = MagicMock(
        side_effect=[
            redis.ConnectionError("first"),
            [1, 60000],
            redis.ConnectionError("second"),
            redis.ConnectionError("second-repeat"),
        ]
    )
    client.register_script.return_value = script
    limiter = RedisFixedWindowRateLimiter(
        client=client, max_requests=10, window_seconds=60
    )
    with caplog.at_level(logging.WARNING, logger="atlas.coordination.rate_limit"):
        assert limiter.check(identity="1.2.3.4").allowed is True
        assert limiter.check(identity="1.2.3.4").allowed is True
        assert limiter.check(identity="1.2.3.4").allowed is True
        assert limiter.check(identity="1.2.3.4").allowed is True
    warnings = [
        r
        for r in caplog.records
        if r.name == "atlas.coordination.rate_limit" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 2


def test_heartbeat_recorder_logs_once_per_outage_episode(caplog: Any) -> None:
    client = MagicMock()
    client.set.side_effect = redis.TimeoutError("slow")
    recorder = RedisHeartbeatRecorder(client=client, ttl_seconds=15)
    with caplog.at_level(logging.WARNING, logger="atlas.coordination.heartbeat"):
        for _ in range(4):
            recorder.beat(worker_id="worker-1")
    warnings = [
        r
        for r in caplog.records
        if r.name == "atlas.coordination.heartbeat" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].event == Event.DEPENDENCY_OPERATION_FAILED_OPEN.value
    assert warnings[0].outcome == "redis_heartbeat_write"
    assert "slow" not in AtlasJSONFormatter().format(warnings[0])


def test_heartbeat_recorder_recovery_allows_later_warning(caplog: Any) -> None:
    client = MagicMock()
    client.set.side_effect = [
        redis.ConnectionError("a"),
        True,
        redis.ConnectionError("b"),
        redis.ConnectionError("b2"),
    ]
    recorder = RedisHeartbeatRecorder(client=client, ttl_seconds=15)
    with caplog.at_level(logging.WARNING, logger="atlas.coordination.heartbeat"):
        recorder.beat(worker_id="worker-1")
        recorder.beat(worker_id="worker-1")
        recorder.beat(worker_id="worker-1")
        recorder.beat(worker_id="worker-1")
    warnings = [
        r
        for r in caplog.records
        if r.name == "atlas.coordination.heartbeat" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 2
