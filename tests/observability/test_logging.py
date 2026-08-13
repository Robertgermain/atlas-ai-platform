"""Structured JSON logging (Slice 15A1): schema, redaction, and policy.

Uses sensitive-looking fixture values throughout -- never real secrets.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime

import pytest

from atlas.observability.context import bind_context
from atlas.observability.events import Event
from atlas.observability.logging import (
    _ALLOWED_STRUCTURED_FIELDS,
    _LOGGER_CATEGORIES,
    AtlasJSONFormatter,
    _normalize_logger_category,
    configure_logging,
    log_event,
    log_exception_boundary,
)

_APPROVED_FIELDS = frozenset(
    {
        "timestamp",
        "severity",
        "service",
        "event",
        "trace_id",
        "span_id",
        "research_job_id",
        "workflow_execution_id",
        "node_name",
        "model_invocation_id",
        "tool_invocation_id",
        "evaluation_run_id",
        "outbox_event_id",
        "consumer_event_id",
        "error_class",
        "duration_ms",
        "outcome",
    }
)

_FAKE_API_KEY = "sk-fakekey-hunter2-abc123"
_FAKE_DB_URL = "postgresql://atlas:hunter2@10.0.0.5:5432/atlas_prod"
_FAKE_AUTH_HEADER = "Bearer fake.jwt.hunter2token"
_FAKE_USER_QUESTION = "What is my SSN 123-45-6789 and password hunter2?"
_FAKE_URL_WITH_QUERY = "https://api.example.com/search?q=hunter2&token=abc123"


@pytest.fixture(autouse=True)
def _reset_service_role() -> None:
    """Every test gets a clean, deterministic ``service`` value."""
    configure_logging(service_role="worker")


def _make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger


class _Capture:
    """Captures both the raw record and its JSON rendering, at emit time.

    Rendering must happen synchronously at emit time (inside ``emit()``),
    exactly like a real handler -- not lazily afterwards. Ambient
    correlation context (:mod:`atlas.observability.context`) is only
    valid for the duration of its own ``with`` block, so formatting a
    record after that block has already exited would read the *wrong*
    (already-cleared) context and silently misrepresent production
    behavior, in which the handler's ``format()`` call happens
    synchronously inside ``logger.log()``.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.records: list[logging.LogRecord] = []
        self.rendered: list[str] = []
        self._formatter = AtlasJSONFormatter()
        capture = self

        class _Collector(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                capture.records.append(record)
                capture.rendered.append(capture._formatter.format(record))

        self._handler = _Collector()
        self._logger = logger
        logger.addHandler(self._handler)

    def __enter__(self) -> _Capture:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self._logger.removeHandler(self._handler)

    def json(self, index: int = 0) -> dict[str, object]:
        return dict(json.loads(self.rendered[index]))


# --- exact JSON schema -------------------------------------------------


def test_structured_line_has_exactly_the_approved_fields() -> None:
    logger = _make_logger("atlas.test.schema")
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED)
    assert set(cap.json()) == _APPROVED_FIELDS


def test_unset_fields_are_present_as_null_not_omitted() -> None:
    logger = _make_logger("atlas.test.schema_nulls")
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED)
    rendered = cap.json()
    assert rendered["trace_id"] is None
    assert rendered["span_id"] is None
    assert rendered["research_job_id"] is None
    assert rendered["error_class"] is None
    assert rendered["duration_ms"] is None
    assert rendered["outcome"] is None


def test_timestamp_severity_service_event_are_correct() -> None:
    logger = _make_logger("atlas.test.fields")
    with _Capture(logger) as cap:
        log_event(logger, Event.TOPIC_ADMIN_SUCCEEDED, level=logging.WARNING)
    rendered = cap.json()
    assert rendered["severity"] == "WARNING"
    assert rendered["service"] == "worker"
    assert rendered["event"] == "topic_admin_succeeded"
    # ISO-8601 with an explicit UTC offset, parseable back.
    parsed = datetime.fromisoformat(str(rendered["timestamp"]))
    assert parsed.utcoffset() is not None


def test_business_fields_round_trip() -> None:
    logger = _make_logger("atlas.test.business_fields")
    with _Capture(logger) as cap:
        log_event(
            logger,
            Event.PROCESS_STARTED,
            research_job_id="job-123",
            workflow_execution_id="wf-456",
            node_name="planner",
            duration_ms=12.5,
            outcome="published",
        )
    rendered = cap.json()
    assert rendered["research_job_id"] == "job-123"
    assert rendered["workflow_execution_id"] == "wf-456"
    assert rendered["node_name"] == "planner"
    assert rendered["duration_ms"] == 12.5
    assert rendered["outcome"] == "published"


def test_ambient_context_fills_fields_not_passed_explicitly() -> None:
    logger = _make_logger("atlas.test.ambient")
    with _Capture(logger) as cap:
        with bind_context(research_job_id="job-from-context"):
            log_event(logger, Event.PROCESS_STARTED)
    assert cap.json()["research_job_id"] == "job-from-context"


def test_explicit_field_overrides_ambient_context() -> None:
    logger = _make_logger("atlas.test.override")
    with _Capture(logger) as cap:
        with bind_context(research_job_id="job-from-context"):
            log_event(logger, Event.PROCESS_STARTED, research_job_id="job-explicit")
    assert cap.json()["research_job_id"] == "job-explicit"


# --- class-only exception reporting ------------------------------------


def test_log_exception_boundary_reports_only_the_class_name() -> None:
    logger = _make_logger("atlas.test.exc_boundary")
    exc = RuntimeError(f"connect failed: {_FAKE_DB_URL}")
    with _Capture(logger) as cap:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
    rendered = cap.json()
    assert rendered["error_class"] == "RuntimeError"
    assert rendered["event"] == "startup_failed"
    assert _FAKE_DB_URL not in cap.rendered[0]


def test_log_exception_boundary_rejects_explicit_error_class_override() -> None:
    logger = _make_logger("atlas.test.exc_boundary_override")
    exc = RuntimeError("boom")
    with pytest.raises(ValueError, match="error_class"):
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc, error_class="Spoofed")


def test_log_event_rejects_passing_the_exception_object_itself() -> None:
    """A caller must never be able to pass ``exc`` (or any non-primitive) as

    a field value -- this would otherwise be a bypass for ``str(exc)`` via
    a permissive JSON ``default=`` fallback. This formatter has none.
    """
    logger = _make_logger("atlas.test.no_exc_object")

    class _NotPrimitive:
        def __str__(self) -> str:
            return _FAKE_API_KEY

    with pytest.raises(TypeError):
        log_event(
            logger,
            Event.STARTUP_FAILED,
            error_class=_NotPrimitive(),  # type: ignore[arg-type]
        )


# --- unsupported field / event rejection, without leaking the value ----


def test_unsupported_field_is_rejected_without_leaking_its_value() -> None:
    logger = _make_logger("atlas.test.unsupported_field")
    with pytest.raises(ValueError) as exc_info:
        log_event(logger, Event.PROCESS_STARTED, raw_prompt=_FAKE_USER_QUESTION)
    assert _FAKE_USER_QUESTION not in str(exc_info.value)
    assert "raw_prompt" in str(exc_info.value)


def test_non_event_member_is_rejected() -> None:
    logger = _make_logger("atlas.test.bad_event")
    with pytest.raises(TypeError):
        log_event(logger, "process_started")  # type: ignore[arg-type]


def test_unsupported_field_produces_no_log_record_at_all() -> None:
    logger = _make_logger("atlas.test.no_record_on_reject")
    with _Capture(logger) as cap:
        with pytest.raises(ValueError):
            log_event(logger, Event.PROCESS_STARTED, bogus="x")
    assert cap.records == []


def test_oversized_string_field_is_truncated_not_rejected() -> None:
    logger = _make_logger("atlas.test.oversized")
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED, node_name="x" * 1000)
    node_name = str(cap.json()["node_name"])
    assert len(node_name) <= 256
    assert node_name.endswith("...<truncated>")


# --- arbitrary extra / traceback / stack suppression --------------------


def test_arbitrary_extra_on_a_raw_stdlib_call_never_appears() -> None:
    """A plain ``logger.info(..., extra=...)`` call -- not going through

    ``log_event`` at all -- must still never leak an arbitrary attribute:
    the formatter only ever looks up its own fixed, approved attribute
    names, regardless of how the record was constructed.
    """
    logger = _make_logger("atlas.test.raw_extra")
    with _Capture(logger) as cap:
        logger.info("unconverted call site", extra={"secret_token": _FAKE_API_KEY})
    assert _FAKE_API_KEY not in cap.rendered[0]
    assert "secret_token" not in cap.rendered[0]


def test_no_traceback_or_stack_info_in_atlas_structured_lines() -> None:
    logger = _make_logger("atlas.test.no_traceback_structured")
    with _Capture(logger) as cap:
        try:
            raise RuntimeError(_FAKE_API_KEY)
        except RuntimeError as exc:
            log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
    rendered_str = cap.rendered[0]
    assert _FAKE_API_KEY not in rendered_str
    assert "Traceback" not in rendered_str
    assert (
        "test_no_traceback_or_stack_info_in_atlas_structured_lines" not in rendered_str
    )


def test_no_traceback_even_when_caller_passes_exc_info() -> None:
    """Even a raw stdlib call with ``exc_info=True`` must not leak a

    traceback through this formatter: it never calls ``formatException``.
    """
    logger = _make_logger("atlas.test.no_traceback_legacy")
    with _Capture(logger) as cap:
        try:
            raise RuntimeError(_FAKE_API_KEY)
        except RuntimeError:
            logger.error("legacy call site", exc_info=True)
    rendered_str = cap.rendered[0]
    assert _FAKE_API_KEY not in rendered_str
    assert "Traceback" not in rendered_str


def test_legacy_record_is_reduced_to_a_safe_fixed_category_event() -> None:
    """The other (non-Atlas-structured) branch: a third-party/unconverted

    call site's record is reduced to exactly ``timestamp``/``severity``/
    ``service``/``event``/``logger_category`` -- no message, no raw
    logger name, no ``args``.
    """
    logger = _make_logger("atlas.test.legacy_message")
    with _Capture(logger) as cap:
        logger.warning("plain message %s", "with-arg")
    rendered = cap.json()
    assert rendered["event"] == "unstructured_log_suppressed"
    assert rendered["logger_category"] == "atlas_unconverted"
    assert set(rendered) == {
        "timestamp",
        "severity",
        "service",
        "event",
        "logger_category",
    }
    assert "message" not in rendered
    assert "logger" not in rendered
    assert "with-arg" not in cap.rendered[0]
    assert "plain message" not in cap.rendered[0]


def test_legacy_record_message_and_args_never_rendered() -> None:
    """A fake secret placed in ``record.msg`` and in a formatting argument

    must never appear in the rendered line.
    """
    logger = _make_logger("atlas.test.legacy_msg_and_args")
    with _Capture(logger) as cap:
        logger.warning("leaked in msg: %s", _FAKE_API_KEY)
    assert _FAKE_API_KEY not in cap.rendered[0]

    with _Capture(logger) as cap2:
        logger.warning(_FAKE_DB_URL)
    assert _FAKE_DB_URL not in cap2.rendered[0]


def test_legacy_record_exc_info_and_stack_info_never_rendered() -> None:
    logger = _make_logger("atlas.test.legacy_exc_and_stack")
    with _Capture(logger) as cap:
        try:
            raise RuntimeError(_FAKE_API_KEY)
        except RuntimeError:
            logger.error("boom", exc_info=True, stack_info=True)
    rendered_str = cap.rendered[0]
    assert _FAKE_API_KEY not in rendered_str
    assert "Traceback" not in rendered_str
    assert "Stack (most recent call last)" not in rendered_str
    assert set(cap.json()) == {
        "timestamp",
        "severity",
        "service",
        "event",
        "logger_category",
    }


def test_legacy_record_arbitrary_extra_never_rendered() -> None:
    logger = _make_logger("atlas.test.legacy_extra")
    with _Capture(logger) as cap:
        logger.warning(
            "x",
            extra={"connection_string": _FAKE_DB_URL, "prompt": _FAKE_USER_QUESTION},
        )
    rendered_str = cap.rendered[0]
    assert _FAKE_DB_URL not in rendered_str
    assert _FAKE_USER_QUESTION not in rendered_str
    assert "connection_string" not in rendered_str
    assert "prompt" not in rendered_str


def test_legacy_record_arbitrary_logger_name_never_rendered_verbatim() -> None:
    """An unrecognized logger name is normalized to ``"other"``, and the

    raw name -- even one deliberately containing sensitive-looking and
    control-character content -- never appears in the rendered line.
    """
    malicious_name = f"some.random.vendor.logger\ninjected:{_FAKE_API_KEY}"
    logger = _make_logger(malicious_name)
    with _Capture(logger) as cap:
        logger.warning("irrelevant")
    rendered_str = cap.rendered[0]
    assert malicious_name not in rendered_str
    assert _FAKE_API_KEY not in rendered_str
    assert cap.json()["logger_category"] == "other"
    assert len(rendered_str.splitlines()) == 1


def test_legacy_record_with_unformattable_args_never_crashes() -> None:
    """A ``%``-formatting bug in a legacy call must never crash logging --

    and, unlike an earlier design, this is now true unconditionally,
    because ``record.getMessage()`` is never called at all for a
    non-Atlas-structured record. Built directly via ``Logger.makeRecord``
    and formatted in isolation (bypassing ``Logger.handle()``/other
    handlers such as pytest's own log capture) purely so this test does
    not depend on any other handler's own behavior.
    """
    logger = _make_logger("atlas.test.bad_format")
    # Two placeholders, one arg: would raise inside ``record.getMessage()``
    # if it were ever called -- it is not.
    record = logger.makeRecord(
        logger.name, logging.WARNING, __file__, 0, "%s and %s", ("only-one-arg",), None
    )
    rendered = json.loads(AtlasJSONFormatter().format(record))
    assert rendered["event"] == "unstructured_log_suppressed"
    assert set(rendered) == {
        "timestamp",
        "severity",
        "service",
        "event",
        "logger_category",
    }


@pytest.mark.parametrize(
    ("name", "expected_category"),
    [
        ("uvicorn", "uvicorn"),
        ("uvicorn.error", "uvicorn"),
        ("psycopg.pool", "psycopg"),
        ("httpx", "httpx_httpcore"),
        ("httpcore._sync.connection", "httpx_httpcore"),
        ("redis.client", "redis"),
        ("sqlalchemy.engine", "sqlalchemy"),
        ("atlas.some.unconverted.module", "atlas_unconverted"),
        ("some_unrecognized_vendor_lib", "other"),
    ],
)
def test_logger_category_normalization(name: str, expected_category: str) -> None:
    category = _normalize_logger_category(name)
    assert category == expected_category
    assert category in _LOGGER_CATEGORIES


# --- banned strings absent from the fully rendered line -----------------


@pytest.mark.parametrize(
    "fake_secret",
    [
        _FAKE_API_KEY,
        _FAKE_DB_URL,
        _FAKE_AUTH_HEADER,
        _FAKE_USER_QUESTION,
        _FAKE_URL_WITH_QUERY,
    ],
)
def test_banned_content_never_appears_via_log_exception_boundary(
    fake_secret: str,
) -> None:
    logger = _make_logger("atlas.test.banned_content")
    exc = RuntimeError(fake_secret)
    with _Capture(logger) as cap:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
    assert fake_secret not in cap.rendered[0]


# --- newline / control-character escaping --------------------------------


def test_control_characters_in_a_field_value_are_json_escaped() -> None:
    logger = _make_logger("atlas.test.control_chars")
    malicious = 'job\n{"forged": true}\t\r"quoted"'
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED, research_job_id=malicious)
    rendered_str = cap.rendered[0]
    # Exactly one line: no raw, unescaped newline was ever written.
    assert len(rendered_str.splitlines()) == 1
    assert cap.json()["research_job_id"] == malicious


# --- strict JSON-safe numeric values -------------------------------------


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_log_event_rejects_non_finite_duration_ms(bad_value: float) -> None:
    logger = _make_logger("atlas.test.non_finite_duration")
    with pytest.raises(ValueError) as exc_info:
        log_event(logger, Event.PROCESS_STARTED, duration_ms=bad_value)
    assert "duration_ms" in str(exc_info.value)
    assert "nan" not in str(exc_info.value).lower()
    assert "inf" not in str(exc_info.value).lower()


@pytest.mark.parametrize("bad_value", [-1, -1.0, -0.001])
def test_log_event_rejects_negative_duration_ms(bad_value: float) -> None:
    logger = _make_logger("atlas.test.negative_duration")
    with pytest.raises(ValueError) as exc_info:
        log_event(logger, Event.PROCESS_STARTED, duration_ms=bad_value)
    assert "duration_ms" in str(exc_info.value)
    assert "-1" not in str(exc_info.value)


@pytest.mark.parametrize("good_value", [0, 0.0, 12.5, 100_000])
def test_log_event_accepts_finite_non_negative_duration_ms(good_value: float) -> None:
    logger = _make_logger("atlas.test.finite_duration")
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED, duration_ms=good_value)
    assert cap.json()["duration_ms"] == good_value


def test_log_event_rejects_non_finite_value_on_a_non_duration_field_too() -> None:
    """The finite-number check is generic, not special-cased to

    ``duration_ms`` alone: any approved field that happens to receive a
    ``float`` must still be finite.
    """
    logger = _make_logger("atlas.test.non_finite_other_field")
    with pytest.raises(ValueError):
        log_event(logger, Event.PROCESS_STARTED, model_invocation_id=float("nan"))


def test_rejected_numeric_value_produces_no_log_record_at_all() -> None:
    logger = _make_logger("atlas.test.no_record_on_bad_numeric")
    with _Capture(logger) as cap:
        with pytest.raises(ValueError):
            log_event(logger, Event.PROCESS_STARTED, duration_ms=float("nan"))
    assert cap.records == []


def test_formatter_rejects_nan_even_if_a_record_bypasses_log_event_validation() -> None:
    """Defense in depth: ``json.dumps(..., allow_nan=False)`` is configured

    on the formatter itself, independent of ``log_event``'s own
    validation, in case a record is ever constructed by a path that
    bypasses it (e.g. directly via ``Logger.makeRecord``, as here).
    """
    logger = _make_logger("atlas.test.allow_nan_defense")
    record = logger.makeRecord(
        logger.name,
        logging.INFO,
        __file__,
        0,
        "",
        (),
        None,
        extra={
            "atlas_structured": True,
            "event": Event.PROCESS_STARTED.value,
            "duration_ms": float("nan"),
        },
    )
    with pytest.raises(ValueError):
        AtlasJSONFormatter().format(record)


# --- third-party logger policy ------------------------------------------


def test_uvicorn_access_is_fully_suppressed() -> None:
    """``propagate=False`` is the actual suppression mechanism.

    ``.handlers`` is deliberately not asserted here at all: pytest's own
    log-capturing machinery (``_pytest.logging.catching_logs``) attaches
    its *own* transient handler directly to any non-propagating logger
    for the duration of a test run, precisely because it would otherwise
    miss such loggers -- that is test-harness instrumentation, not a real
    destination, and is not something this configuration controls either
    way (verified directly against ``configure_logging`` outside pytest:
    ``uvicorn.access`` has zero handlers there).
    """
    access_logger = logging.getLogger("uvicorn.access")
    assert access_logger.propagate is False


def test_uvicorn_and_uvicorn_error_propagate_through_atlas_formatter() -> None:
    for name in ("uvicorn", "uvicorn.error"):
        third_party = logging.getLogger(name)
        assert third_party.handlers == []
        assert third_party.propagate is True


def test_psycopg_pool_is_raised_to_error() -> None:
    assert logging.getLogger("psycopg.pool").level == logging.ERROR


def test_httpx_and_httpcore_are_raised_to_warning() -> None:
    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_redis_is_raised_to_warning() -> None:
    assert logging.getLogger("redis").level == logging.WARNING


def test_sqlalchemy_engine_is_held_at_warning() -> None:
    assert logging.getLogger("sqlalchemy.engine").level == logging.WARNING


def test_configure_logging_rejects_unknown_service_role() -> None:
    with pytest.raises(ValueError):
        configure_logging(service_role="not-a-real-role")


def test_configure_logging_is_idempotent_no_duplicate_handlers() -> None:
    root = logging.getLogger()
    configure_logging(service_role="api")
    handlers_after_first = list(root.handlers)
    configure_logging(service_role="api")
    handlers_after_second = list(root.handlers)
    assert len(handlers_after_second) == len(handlers_after_first)


def test_configure_logging_removes_a_pre_existing_foreign_handler() -> None:
    """A handler ``configure_logging`` did not itself install (e.g. a

    library that called ``logging.basicConfig()`` first) must not survive
    a ``configure_logging`` call: leaving it in place would let it
    independently render a record's raw content through its own
    formatter, bypassing the safe :class:`AtlasJSONFormatter` entirely.
    """
    root = logging.getLogger()
    foreign = logging.NullHandler()
    root.addHandler(foreign)
    try:
        configure_logging(service_role="worker")
        assert foreign not in root.handlers
    finally:
        root.removeHandler(foreign)


class _UnsafeRawFormatter(logging.Formatter):
    """Mirrors an unsafe pre-``configure_logging`` handler: renders the raw
    ``%``-formatted message verbatim, exactly like a naive
    ``logging.basicConfig()`` default formatter would.
    """

    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


def test_configure_logging_prevents_a_pre_existing_unsafe_handler_from_leaking(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A deliberately unsafe pre-existing handler (one whose own formatter

    would render a raw message verbatim, carrying a fake secret) must not
    receive -- and therefore never emit -- that secret once
    ``configure_logging`` has run: the handler itself is removed, not
    merely left in place with hope that nothing routes to it.
    """
    root = logging.getLogger()
    unsafe_handler = logging.StreamHandler(stream=sys.stdout)
    unsafe_handler.setFormatter(_UnsafeRawFormatter())
    root.addHandler(unsafe_handler)
    try:
        configure_logging(service_role="worker")
        assert unsafe_handler not in root.handlers
        logger = _make_logger("atlas.test.unsafe_handler_removed")
        logger.warning("leaked-fake-secret: %s", _FAKE_API_KEY)
    finally:
        root.removeHandler(unsafe_handler)
    out = capsys.readouterr().out
    assert _FAKE_API_KEY not in out
    assert "leaked-fake-secret" not in out


def test_configure_logging_after_a_simulated_uvicorn_root_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Simulates Uvicorn installing its own root handler/formatter before

    Atlas's own entrypoint calls ``configure_logging`` -- a real ordering
    risk for the API service role. The foreign handler must be replaced,
    and the resulting root logger must carry exactly one handler.
    """
    root = logging.getLogger()
    uvicorn_style_handler = logging.StreamHandler(stream=sys.stdout)
    uvicorn_style_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root.addHandler(uvicorn_style_handler)
    try:
        configure_logging(service_role="api")
        assert uvicorn_style_handler not in root.handlers
        assert len(root.handlers) == 1
        logger = _make_logger("atlas.test.after_simulated_uvicorn_handler")
        logger.warning("plain message %s", _FAKE_API_KEY)
    finally:
        root.removeHandler(uvicorn_style_handler)
    out = capsys.readouterr().out
    assert _FAKE_API_KEY not in out


def test_configure_logging_repeated_calls_produce_no_duplicate_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(service_role="worker")
    configure_logging(service_role="worker")
    configure_logging(service_role="worker")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    logger = _make_logger("atlas.test.no_duplicate_output")
    logger.info("hello")
    out = capsys.readouterr().out
    assert len(out.strip().splitlines()) == 1


def test_configure_logging_unknown_role_leaves_prior_config_unchanged() -> None:
    """A rejected ``service_role`` must not touch the root logger at all --

    not even to remove the previously-installed Atlas handler.
    """
    configure_logging(service_role="worker")
    root = logging.getLogger()
    handlers_before = list(root.handlers)
    with pytest.raises(ValueError):
        configure_logging(service_role="not-a-real-role")
    assert list(root.handlers) == handlers_before
    logger = _make_logger("atlas.test.rejected_role_unchanged")
    with _Capture(logger) as cap:
        log_event(logger, Event.PROCESS_STARTED)
    assert cap.json()["service"] == "worker"


# --- allowed-field-set consistency --------------------------------------


def test_allowed_structured_fields_excludes_the_always_present_envelope_fields() -> (
    None
):
    always_present = {"timestamp", "severity", "service", "event"}
    assert always_present.isdisjoint(_ALLOWED_STRUCTURED_FIELDS)
    assert _ALLOWED_STRUCTURED_FIELDS | always_present == _APPROVED_FIELDS
