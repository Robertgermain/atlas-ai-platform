"""Network-free unit tests for the dedicated heartbeat thread.

Uses a fake in-memory HeartbeatRecorder; never touches Redis.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import pytest

from atlas.coordination.heartbeat_thread import HeartbeatThread
from atlas.observability.events import Event
from atlas.observability.logging import AtlasJSONFormatter


class _FakeRecorder:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.beats: list[str] = []

    def beat(self, *, worker_id: str) -> None:
        with self.lock:
            self.beats.append(worker_id)


class _BoomingRecorder:
    """Raises on every beat to prove the thread survives unexpected errors."""

    def beat(self, *, worker_id: str) -> None:
        del worker_id
        raise RuntimeError("boom")


def test_heartbeat_thread_beats_immediately_on_start() -> None:
    recorder = _FakeRecorder()
    thread = HeartbeatThread(
        recorder=recorder, worker_id="worker-1", interval_seconds=10.0
    )
    try:
        thread.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not recorder.beats:
            time.sleep(0.01)
        assert recorder.beats == ["worker-1"]
    finally:
        thread.stop()


def test_heartbeat_thread_beats_repeatedly_on_interval() -> None:
    recorder = _FakeRecorder()
    thread = HeartbeatThread(
        recorder=recorder, worker_id="worker-1", interval_seconds=0.05
    )
    try:
        thread.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and len(recorder.beats) < 3:
            time.sleep(0.01)
        assert len(recorder.beats) >= 3
        assert set(recorder.beats) == {"worker-1"}
    finally:
        thread.stop()


def test_heartbeat_thread_stop_is_prompt_and_final() -> None:
    recorder = _FakeRecorder()
    thread = HeartbeatThread(
        recorder=recorder, worker_id="worker-1", interval_seconds=0.05
    )
    thread.start()
    time.sleep(0.12)
    started = time.monotonic()
    thread.stop(join_timeout_seconds=2.0)
    elapsed = time.monotonic() - started
    assert elapsed < 1.0

    count_at_stop = len(recorder.beats)
    time.sleep(0.2)
    assert len(recorder.beats) == count_at_stop


def test_heartbeat_thread_survives_recorder_exceptions() -> None:
    thread = HeartbeatThread(
        recorder=_BoomingRecorder(), worker_id="worker-1", interval_seconds=0.05
    )
    try:
        thread.start()
        time.sleep(0.2)
        # Thread must still be alive despite every beat raising.
        assert thread.is_alive is True
    finally:
        started = time.monotonic()
        thread.stop(join_timeout_seconds=2.0)
        assert time.monotonic() - started < 1.0


def test_heartbeat_thread_unexpected_errors_log_once_without_traceback(
    caplog: Any,
) -> None:
    thread = HeartbeatThread(
        recorder=_BoomingRecorder(), worker_id="worker-1", interval_seconds=0.05
    )
    try:
        with caplog.at_level(
            logging.WARNING, logger="atlas.coordination.heartbeat_thread"
        ):
            thread.start()
            time.sleep(0.2)
        warnings = [
            r
            for r in caplog.records
            if r.name == "atlas.coordination.heartbeat_thread"
            and r.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert warnings[0].event == Event.HEARTBEAT_RECORDER_UNEXPECTED_ERROR.value
        assert warnings[0].error_class == "RuntimeError"
        assert warnings[0].exc_info is None
        rendered = AtlasJSONFormatter().format(warnings[0])
        assert "boom" not in rendered
    finally:
        thread.stop(join_timeout_seconds=2.0)


class _SlowRecorder:
    """Blocks inside ``beat()`` past a short join timeout, on purpose."""

    def __init__(self, release: threading.Event) -> None:
        self._release = release

    def beat(self, *, worker_id: str) -> None:
        del worker_id
        self._release.wait(timeout=5.0)


def test_heartbeat_thread_stop_timeout_logs_shutdown_wait_abandoned(
    caplog: Any,
) -> None:
    """``stop()`` abandoning its join must log a distinct, observable event."""
    release = threading.Event()
    thread = HeartbeatThread(
        recorder=_SlowRecorder(release), worker_id="worker-1", interval_seconds=10.0
    )
    try:
        thread.start()
        time.sleep(0.05)  # let the blocking beat() call actually start
        with caplog.at_level(
            logging.WARNING, logger="atlas.coordination.heartbeat_thread"
        ):
            thread.stop(join_timeout_seconds=0.05)
        warnings = [
            r
            for r in caplog.records
            if r.name == "atlas.coordination.heartbeat_thread"
            and r.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert warnings[0].event == "shutdown_wait_abandoned"
        assert warnings[0].outcome == "heartbeat_thread"
        assert warnings[0].duration_ms == pytest.approx(50.0)
    finally:
        release.set()
