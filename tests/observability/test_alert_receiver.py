"""Atlas-owned internal Alertmanager webhook receiver (Slice 15A3 final
condition #3): bounded request handling, safe logging, and the bounded
in-memory ring buffer."""

from __future__ import annotations

import io
import json

from atlas.observability.alert_receiver import (
    MAX_BODY_BYTES,
    _ReceivedAlertRingBuffer,
    build_wsgi_app,
)
from atlas.observability.events import Event
from atlas.observability.testing import capture_logs


def _environ(
    *,
    method: str = "GET",
    path: str = "/health",
    body: bytes = b"",
    content_length: str | None = None,
    transfer_encoding: str | None = None,
) -> dict[str, object]:
    environ: dict[str, object] = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "wsgi.input": io.BytesIO(body),
    }
    if content_length is not None:
        environ["CONTENT_LENGTH"] = content_length
    if transfer_encoding is not None:
        environ["HTTP_TRANSFER_ENCODING"] = transfer_encoding
    return environ


class _StartResponseRecorder:
    def __init__(self) -> None:
        self.status: str | None = None
        self.headers: list[tuple[str, str]] = []

    def __call__(self, status: str, headers: list[tuple[str, str]]) -> None:
        self.status = status
        self.headers = headers


def _call(app: object, environ: dict[str, object]) -> tuple[str, bytes]:
    recorder = _StartResponseRecorder()
    body_chunks = app(environ, recorder)  # type: ignore[operator]
    assert recorder.status is not None
    return recorder.status, b"".join(body_chunks)


def _alertmanager_payload(*, status: str, alerts: list[dict[str, object]]) -> bytes:
    return json.dumps({"status": status, "alerts": alerts}).encode("utf-8")


def test_health_endpoint_returns_200() -> None:
    app, _buffer = build_wsgi_app()
    status, body = _call(app, _environ(method="GET", path="/health"))
    assert status == "200 OK"
    assert body == b"ok"


def test_unknown_route_returns_404() -> None:
    app, _buffer = build_wsgi_app()
    status, _body = _call(app, _environ(method="GET", path="/nope"))
    assert status == "404 Not Found"


def test_unknown_method_on_known_path_returns_404() -> None:
    app, _buffer = build_wsgi_app()
    status, _body = _call(app, _environ(method="DELETE", path="/webhook"))
    assert status == "404 Not Found"


def test_webhook_missing_content_length_is_rejected_before_reading_body() -> None:
    app, buffer = build_wsgi_app()
    status, _body = _call(
        app, _environ(method="POST", path="/webhook", body=b"{}", content_length=None)
    )
    assert status == "411 Length Required"
    assert buffer.snapshot() == []


def test_webhook_non_numeric_content_length_is_rejected() -> None:
    app, buffer = build_wsgi_app()
    status, _body = _call(
        app,
        _environ(
            method="POST", path="/webhook", body=b"{}", content_length="not-a-number"
        ),
    )
    assert status == "400 Bad Request"
    assert buffer.snapshot() == []


def test_webhook_negative_content_length_is_rejected() -> None:
    app, buffer = build_wsgi_app()
    status, _body = _call(
        app, _environ(method="POST", path="/webhook", body=b"{}", content_length="-1")
    )
    assert status == "400 Bad Request"
    assert buffer.snapshot() == []


def test_webhook_oversized_content_length_is_rejected_before_reading_body() -> None:
    app, buffer = build_wsgi_app()
    oversized = str(MAX_BODY_BYTES + 1)
    # The body itself is tiny; the rejection must trigger purely off the
    # declared Content-Length, before any read() call against wsgi.input.
    status, _body = _call(
        app,
        _environ(method="POST", path="/webhook", body=b"{}", content_length=oversized),
    )
    assert status == "413 Content Too Large"
    assert buffer.snapshot() == []


def test_webhook_rejects_chunked_transfer_encoding_before_reading_body() -> None:
    app, buffer = build_wsgi_app()
    status, _body = _call(
        app,
        _environ(
            method="POST",
            path="/webhook",
            body=b"{}",
            content_length="2",
            transfer_encoding="chunked",
        ),
    )
    assert status == "501 Not Implemented"
    assert buffer.snapshot() == []


def test_webhook_rejects_malformed_json() -> None:
    app, buffer = build_wsgi_app()
    body = b"{not-json"
    status, _body = _call(
        app,
        _environ(
            method="POST", path="/webhook", body=body, content_length=str(len(body))
        ),
    )
    assert status == "400 Bad Request"
    assert buffer.snapshot() == []


def test_webhook_rejects_a_non_object_json_body() -> None:
    app, buffer = build_wsgi_app()
    body = b"[1, 2, 3]"
    status, _body = _call(
        app,
        _environ(
            method="POST", path="/webhook", body=body, content_length=str(len(body))
        ),
    )
    assert status == "400 Bad Request"
    assert buffer.snapshot() == []


def test_webhook_accepts_a_firing_alert_and_records_it_in_the_ring_buffer() -> None:
    app, buffer = build_wsgi_app()
    body = _alertmanager_payload(
        status="firing",
        alerts=[
            {
                "status": "firing",
                "labels": {"alertname": "AtlasHighHttpErrorRatio"},
                "fingerprint": "abc123",
            }
        ],
    )
    status, response_body = _call(
        app,
        _environ(
            method="POST", path="/webhook", body=body, content_length=str(len(body))
        ),
    )
    assert status == "200 OK"
    assert response_body == b"ok"
    entries = buffer.snapshot()
    assert entries == [
        {
            "alertname": "AtlasHighHttpErrorRatio",
            "fingerprint": "abc123",
            "status": "firing",
        }
    ]


def test_webhook_logs_only_the_fixed_event_and_outcome_never_alert_details() -> None:
    app, _buffer = build_wsgi_app(logger_name="test.alert_receiver.webhook")
    body = _alertmanager_payload(
        status="firing",
        alerts=[
            {
                "status": "firing",
                "labels": {"alertname": "sekret-alert-name-should-never-log"},
                "fingerprint": "sekret-fingerprint-should-never-log",
                "annotations": {"summary": "sekret-annotation-should-never-log"},
            }
        ],
    )
    with capture_logs("test.alert_receiver.webhook") as captured:
        status, _response_body = _call(
            app,
            _environ(
                method="POST",
                path="/webhook",
                body=body,
                content_length=str(len(body)),
            ),
        )

    assert status == "200 OK"
    assert captured.events == [Event.ALERT_WEBHOOK_RECEIVED.value]
    record = captured.json(0)
    assert record["outcome"] == "firing"
    assert "sekret-alert-name-should-never-log" not in captured.text
    assert "sekret-fingerprint-should-never-log" not in captured.text
    assert "sekret-annotation-should-never-log" not in captured.text
    # No arbitrary alert-specific field was ever added to the fixed schema.
    assert "alertname" not in record
    assert "fingerprint" not in record
    assert "labels" not in record
    assert "annotations" not in record


def test_webhook_normalizes_unknown_status_to_other() -> None:
    app, buffer = build_wsgi_app()
    body = _alertmanager_payload(
        status="some-unexpected-future-status",
        alerts=[
            {
                "status": "some-unexpected-future-status",
                "labels": {"alertname": "Foo"},
                "fingerprint": "f1",
            }
        ],
    )
    with capture_logs("atlas.observability.alert_receiver") as captured:
        status, _response_body = _call(
            app,
            _environ(
                method="POST",
                path="/webhook",
                body=body,
                content_length=str(len(body)),
            ),
        )
    assert status == "200 OK"
    assert captured.json(0)["outcome"] == "other"
    entries = buffer.snapshot()
    assert entries[0]["status"] == "other"


def test_webhook_handles_a_resolved_alert() -> None:
    app, buffer = build_wsgi_app()
    body = _alertmanager_payload(
        status="resolved",
        alerts=[
            {"status": "resolved", "labels": {"alertname": "Foo"}, "fingerprint": "f1"}
        ],
    )
    status, _body = _call(
        app,
        _environ(
            method="POST", path="/webhook", body=body, content_length=str(len(body))
        ),
    )
    assert status == "200 OK"
    assert buffer.snapshot()[0]["status"] == "resolved"


def test_received_endpoint_returns_the_current_buffer_contents_as_json() -> None:
    app, buffer = build_wsgi_app()
    buffer.append(alertname="Foo", fingerprint="f1", status="firing")
    status, body = _call(app, _environ(method="GET", path="/received"))
    assert status == "200 OK"
    assert json.loads(body) == [
        {"alertname": "Foo", "fingerprint": "f1", "status": "firing"}
    ]


def test_ring_buffer_discards_the_oldest_entry_once_capacity_is_exceeded() -> None:
    buffer = _ReceivedAlertRingBuffer(capacity=3)
    for i in range(5):
        buffer.append(alertname=f"alert-{i}", fingerprint=f"fp-{i}", status="firing")
    entries = buffer.snapshot()
    assert len(entries) == 3
    assert [entry["alertname"] for entry in entries] == [
        "alert-2",
        "alert-3",
        "alert-4",
    ]


def test_ring_buffer_truncates_oversized_alertname_and_fingerprint() -> None:
    buffer = _ReceivedAlertRingBuffer()
    oversized = "x" * 10_000
    buffer.append(alertname=oversized, fingerprint=oversized, status="firing")
    entry = buffer.snapshot()[0]
    assert len(entry["alertname"]) <= 256
    assert len(entry["fingerprint"]) <= 256


def test_ring_buffer_normalizes_an_unknown_status_to_other() -> None:
    buffer = _ReceivedAlertRingBuffer()
    buffer.append(alertname="Foo", fingerprint="f1", status="totally-unexpected")
    assert buffer.snapshot()[0]["status"] == "other"
