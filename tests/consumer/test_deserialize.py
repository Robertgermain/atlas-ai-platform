"""Network-free unit tests for ``atlas.consumer.deserialize.decode_message``."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from atlas.consumer.deserialize import MAX_MESSAGE_VALUE_BYTES, decode_message
from atlas.consumer.errors import (
    AggregateTypeHeaderMismatchError,
    DuplicateHeaderKeyError,
    EventTypeHeaderMismatchError,
    EventVersionHeaderMismatchError,
    InvalidJsonError,
    MissingHeadersError,
    MissingValueError,
    NullHeaderValueError,
    SchemaValidationFailedError,
    UndecodableHeaderValueError,
    UndecodableValueError,
    UnexpectedHeaderKeysError,
    ValueNotAnObjectError,
    ValueTooLargeError,
)
from atlas.consumer.fakes import FakeKafkaMessage, build_kafka_message_for_event
from atlas.eventing import ResearchJobCreatedEvent, build_research_job_created

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def _event() -> ResearchJobCreatedEvent:
    return build_research_job_created(
        research_job_id="job-decode-1", created_at=T0, event_id=uuid4()
    )


def test_decode_message_round_trips_a_valid_record() -> None:
    event = _event()
    message = build_kafka_message_for_event(event, partition=0, offset=7)
    decoded = decode_message(message)
    assert decoded.event_id == event.event_id
    assert decoded.event_type == event.event_type
    assert decoded.aggregate_id == event.aggregate_id


def test_missing_headers_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_headers = None
    with pytest.raises(MissingHeadersError):
        decode_message(message)


def test_duplicate_header_key_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    assert message.raw_headers is not None
    message.raw_headers = [
        *message.raw_headers,
        ("event_type", b"research_job.created"),
    ]
    with pytest.raises(DuplicateHeaderKeyError):
        decode_message(message)


def test_null_header_value_is_rejected() -> None:
    message = FakeKafkaMessage(
        value=b"{}",
        headers=[
            ("event_type", None),  # type: ignore[list-item]
            ("event_version", b"1"),
            ("aggregate_type", b"research_job"),
        ],
    )
    with pytest.raises(NullHeaderValueError):
        decode_message(message)


def test_undecodable_header_value_is_rejected() -> None:
    message = FakeKafkaMessage(
        value=b"{}",
        headers=[
            ("event_type", b"\xff\xfe"),
            ("event_version", b"1"),
            ("aggregate_type", b"research_job"),
        ],
    )
    with pytest.raises(UndecodableHeaderValueError):
        decode_message(message)


def test_unexpected_header_keys_missing_one_is_rejected() -> None:
    message = FakeKafkaMessage(
        value=b"{}",
        headers=[
            ("event_type", b"research_job.created"),
            ("event_version", b"1"),
        ],
    )
    with pytest.raises(UnexpectedHeaderKeysError):
        decode_message(message)


def test_unexpected_header_keys_extra_one_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    assert message.raw_headers is not None
    message.raw_headers = [*message.raw_headers, ("extra_header", b"value")]
    with pytest.raises(UnexpectedHeaderKeysError):
        decode_message(message)


def test_missing_value_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_value = None
    with pytest.raises(MissingValueError):
        decode_message(message)


def test_oversized_value_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_value = b"x" * (MAX_MESSAGE_VALUE_BYTES + 1)
    with pytest.raises(ValueTooLargeError):
        decode_message(message)


def test_undecodable_value_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_value = b"\xff\xfe\xfd"
    with pytest.raises(UndecodableValueError):
        decode_message(message)


def test_invalid_json_value_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_value = b"{not json"
    with pytest.raises(InvalidJsonError):
        decode_message(message)


def test_value_not_an_object_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_value = b"[1, 2, 3]"
    with pytest.raises(ValueNotAnObjectError):
        decode_message(message)


def test_unknown_event_type_fails_schema_validation() -> None:
    message = FakeKafkaMessage(
        value=b'{"event_id": "%s", "event_version": 1, "aggregate_type": '
        b'"research_job", "aggregate_id": "job-x", "occurred_at": '
        b'"2026-08-10T12:00:00Z", "event_type": "research_job.unknown", '
        b'"payload": {}}' % str(uuid4()).encode("ascii"),
        headers=[
            ("event_type", b"research_job.unknown"),
            ("event_version", b"1"),
            ("aggregate_type", b"research_job"),
        ],
    )
    with pytest.raises(SchemaValidationFailedError):
        decode_message(message)


def test_unsupported_event_version_fails_schema_validation() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    assert message.raw_value is not None
    message.raw_value = message.raw_value.replace(
        b'"event_version":1', b'"event_version":2'
    )
    message.raw_headers = [
        ("event_type", b"research_job.created"),
        ("event_version", b"2"),
        ("aggregate_type", b"research_job"),
    ]
    with pytest.raises(SchemaValidationFailedError):
        decode_message(message)


def test_event_type_header_mismatch_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_headers = [
        ("event_type", b"research_job.completed"),
        ("event_version", b"1"),
        ("aggregate_type", b"research_job"),
    ]
    with pytest.raises(EventTypeHeaderMismatchError):
        decode_message(message)


def test_event_version_header_mismatch_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_headers = [
        ("event_type", b"research_job.created"),
        ("event_version", b"2"),
        ("aggregate_type", b"research_job"),
    ]
    with pytest.raises(EventVersionHeaderMismatchError):
        decode_message(message)


def test_aggregate_type_header_mismatch_is_rejected() -> None:
    event = _event()
    message = build_kafka_message_for_event(event)
    message.raw_headers = [
        ("event_type", b"research_job.created"),
        ("event_version", b"1"),
        ("aggregate_type", b"other_aggregate"),
    ]
    with pytest.raises(AggregateTypeHeaderMismatchError):
        decode_message(message)


def test_no_sensitive_raw_bytes_ever_appear_in_exception_text() -> None:
    """The raw value bytes must never leak into any raised exception's message."""
    secret_looking_value = (
        b'{"password": "hunter2", "database_url": '
        b'"postgresql://atlas:hunter2@10.0.0.5/atlas"}'
    )
    message = FakeKafkaMessage(
        value=secret_looking_value,
        headers=[
            ("event_type", b"research_job.created"),
            ("event_version", b"1"),
            ("aggregate_type", b"research_job"),
        ],
    )
    with pytest.raises(SchemaValidationFailedError) as excinfo:
        decode_message(message)
    assert "hunter2" not in str(excinfo.value)
    assert "10.0.0.5" not in str(excinfo.value)
