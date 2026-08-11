"""Network-free unit tests for ``atlas.consumer.retention.build_retention``."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from atlas.consumer.retention import build_retention
from atlas.eventing import build_research_job_created

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def test_tier_b_untrusted_payload_stores_only_hash_and_length_no_raw_bytes() -> None:
    raw_value = b'{"password": "hunter2"}'
    retention = build_retention(
        failure_code="invalid_json", raw_value=raw_value, decoded_event=None
    )
    assert retention.replay_eligible is False
    assert retention.payload_sha256 == hashlib.sha256(raw_value).hexdigest()
    assert retention.payload_byte_length == len(raw_value)
    assert retention.retained_canonical_value is None
    assert retention.retained_header_event_type is None
    assert retention.retained_header_event_version is None
    assert retention.retained_header_aggregate_type is None
    assert retention.event_id is None
    assert retention.event_type is None
    assert retention.event_version is None
    assert retention.aggregate_type is None
    assert retention.aggregate_id is None


def test_tier_b_handles_a_missing_raw_value() -> None:
    retention = build_retention(
        failure_code="missing_value", raw_value=None, decoded_event=None
    )
    assert retention.payload_sha256 == hashlib.sha256(b"").hexdigest()
    assert retention.payload_byte_length == 0
    assert retention.replay_eligible is False


def test_tier_b_oversized_payload_is_dlq_able_without_storing_bytes() -> None:
    oversized = b"x" * (10 * 1024 * 1024)
    retention = build_retention(
        failure_code="value_too_large", raw_value=oversized, decoded_event=None
    )
    assert retention.replay_eligible is False
    assert retention.payload_byte_length == len(oversized)
    assert retention.retained_canonical_value is None


def test_tier_a_lifecycle_violation_stores_canonical_reserialization() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    raw_value = b'{"irrelevant": "original bytes"}'
    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=raw_value,
        decoded_event=event,
    )
    assert retention.replay_eligible is True
    assert retention.event_id == event.event_id
    assert retention.event_type == event.event_type
    assert retention.event_version == int(event.event_version)
    assert retention.aggregate_type == event.aggregate_type
    assert retention.aggregate_id == event.aggregate_id
    assert retention.retained_canonical_value is not None
    assert retention.retained_header_event_type == event.event_type
    assert retention.retained_header_event_version == str(int(event.event_version))
    assert retention.retained_header_aggregate_type == event.aggregate_type
    # The hash is over the original raw bytes, not the canonical reserialization.
    assert retention.payload_sha256 == hashlib.sha256(raw_value).hexdigest()


def test_tier_a_failure_code_without_a_decoded_event_is_rejected() -> None:
    with pytest.raises(ValueError, match="Tier-A eligible"):
        build_retention(
            failure_code="lifecycle_order_violation",
            raw_value=b"{}",
            decoded_event=None,
        )


def test_tier_b_failure_code_with_a_decoded_event_is_rejected() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    with pytest.raises(ValueError, match="Tier-A eligible"):
        build_retention(
            failure_code="invalid_json", raw_value=b"{}", decoded_event=event
        )
