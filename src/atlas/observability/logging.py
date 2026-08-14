"""Centralized structured JSON logging (Slice 15A1).

This module is the only place in Atlas that decides *what a log line looks
like*. It provides:

- :func:`configure_logging` -- installs one JSON-emitting handler on the
  root logger and applies the third-party logger policy (see below). Call
  exactly once, at process entry, before any other Atlas code runs.
  ``stream=None`` keeps the existing ``sys.stdout`` destination; a caller
  may pass another text stream (the advisory CLI uses ``sys.stderr``)
  without changing the formatter or the one-handler policy.
- :class:`AtlasJSONFormatter` -- the JSON formatter itself.
- :func:`log_event` / :func:`log_exception_boundary` -- the only
  sanctioned way for Atlas's own code to emit a structured log line.

Two lines, two contracts -- both fully sanitized
--------------------------------------------------

Every :class:`logging.LogRecord` this formatter renders falls into exactly
one of two categories, and the two are deliberately not the same shape --
but neither shape ever carries free-form content:

1. **Atlas-structured lines** -- created only via :func:`log_event` /
   :func:`log_exception_boundary`. These carry *only* the approved field
   set below and nothing else: no message text, no ``extra`` beyond that
   allowlist, no traceback, no stack info. This is the strict contract
   Slice 15A1 is required to guarantee.
2. **Legacy/third-party lines** -- everything else: any log record from a
   logger Atlas does not control (``uvicorn``, ``uvicorn.error``,
   ``psycopg.pool``, etc.) and any Atlas call site not yet converted to
   :func:`log_event` (see each converted module's own docstring for what
   remains unconverted). Earlier revisions of this module rendered these
   with the record's own already-``%``-formatted message text and raw
   logger name; that violated Atlas's universal no-raw-content telemetry
   policy (a third-party or unconverted call site can format an exception
   message, a URL, a connection string, or arbitrary caller-supplied text
   directly into ``record.msg``/``args``, and nothing in ``logging``
   itself prevents that). **This formatter never reads or renders
   ``record.getMessage()``, ``record.msg``, ``record.args``,
   ``record.exc_info``, ``record.stack_info``, or any ``extra`` attribute
   for a non-Atlas-structured record.** Instead, every such record is
   reduced to a single fixed event, :attr:`~atlas.observability.events.
   Event.UNSTRUCTURED_LOG_SUPPRESSED`, plus a normalized, fixed
   ``logger_category`` (see below) derived from the record's logger name
   through a closed allowlist -- the raw logger name itself is never
   emitted unless it exactly equals its own normalized category. Atlas
   therefore records that an approved third-party/unconverted signal
   *occurred*, its category, and its severity -- never its original
   message, arguments, or any other free-form content. This is honest and
   deliberately less informative than the earlier design; it is not a
   claim that Atlas "sanitizes" arbitrary third-party text -- there is no
   text left to sanitize.

Approved Atlas-structured JSON fields
--------------------------------------

``timestamp``, ``severity``, ``service``, ``event``, ``trace_id``,
``span_id``, ``research_job_id``, ``workflow_execution_id``, ``node_name``,
``model_invocation_id``, ``tool_invocation_id``, ``evaluation_run_id``,
``outbox_event_id``, ``consumer_event_id``, ``error_class``,
``duration_ms``, ``outcome``. Every Atlas-structured line always includes
every one of these keys; any field the caller (or the ambient correlation
context -- see :mod:`atlas.observability.context`) did not supply is
present with a JSON ``null`` value rather than omitted, so the schema is
identical on every line. ``trace_id``/``span_id`` are always ``null`` in
this slice and remain ``null`` through Slice 15A2 (Prometheus metric
production, a separate concern) -- Slice 15A3/OpenTelemetry is what
populates them, once it exists. A legacy/third-party line
never carries any of these fields at all (they would all be meaningless
``null`` noise on a line that is not describing an Atlas-structured
event) -- its own fixed shape is documented next.

Legacy/third-party line shape: normalized logger category, no raw text
-------------------------------------------------------------------------

A non-Atlas-structured record renders as exactly: ``timestamp``,
``severity``, ``service`` (the three envelope fields every line carries),
``event`` (always the literal string
``"unstructured_log_suppressed"`` -- see
:attr:`atlas.observability.events.Event.UNSTRUCTURED_LOG_SUPPRESSED`), and
``logger_category``. ``logger_category`` is one of a small, fixed, closed
set of labels (:data:`_LOGGER_CATEGORIES`) that :func:`_normalize_logger_category`
maps a record's ``record.name`` to via prefix matching against the same
loggers :func:`_configure_third_party_loggers` names below, plus
``"atlas_unconverted"`` for any not-yet-converted Atlas call site
(``record.name`` starting with ``"atlas."``) and ``"other"`` for anything
that matches no known prefix. The raw logger name itself is never rendered
-- only its normalized category -- so an unusual or unexpected logger name
containing incidental sensitive-looking text is never a leak vector
either. No message, arguments, exception info, stack info, or ``extra``
of any kind is ever read from a non-Atlas-structured record.

Third-party logger policy
--------------------------

:func:`configure_logging` applies a small, explicit, per-logger *level and
routing* policy, chosen from what this codebase's dependencies actually do
(verified against the installed packages, not assumed). This policy
controls which third-party loggers are silenced or level-raised and how
their records are routed -- it does not and cannot make the resulting
lines carry more information than the fixed, normalized shape described
above; every record that does reach :class:`AtlasJSONFormatter` is reduced
to that same safe shape regardless of which policy bucket its logger falls
into.

- ``uvicorn.access`` -- suppressed entirely (handlers cleared,
  ``propagate=False``, the same mechanism Uvicorn itself uses for its own
  ``access_log=False`` option), so its records never even reach this
  formatter. Uvicorn's default access-log line embeds the raw request
  line, including the full path and query string; even reduced to a fixed
  category, a *volume* of ``uvicorn_access`` lines could still leak
  request timing/frequency information this slice has not evaluated, so
  it remains fully silenced rather than category-reduced.
- ``uvicorn`` / ``uvicorn.error`` -- kept, but their own directly-attached
  handlers are cleared and ``propagate`` is forced ``True`` so their
  records flow through Atlas's own formatter (as the normalized
  ``"uvicorn"`` category, never their own plain-text message).
- ``psycopg.pool`` -- raised to ``ERROR`` (from its own default
  ``WARNING``). ``psycopg_pool`` has its own background per-connection-
  attempt warning logging (a known pre-existing gap recorded in
  ``docs/TECHNICAL_DESIGN.md``); Atlas's own sanitized error handling at
  each call site already reports this condition safely, so the duplicate
  third-party signal is suppressed at the source rather than reproduced.
- ``httpx`` / ``httpcore`` -- raised to ``WARNING`` defensively.
  ``httpx``'s own ``DEBUG``-level logging includes full request URLs
  (relevant because Atlas's Tavily search tool sends the search query as
  part of the request); the root logger is already ``INFO``, so this is
  currently inert, but is set explicitly so a future accidental
  ``DEBUG``-level enable elsewhere cannot resurrect it silently -- and even
  if it did, the resulting record would still only ever render as the
  fixed ``"httpx_httpcore"`` category, never the URL itself.
- ``redis`` -- raised to ``WARNING`` for the same defense-in-depth reason;
  redis-py's own logging is minimal by default.
- ``confluent_kafka`` -- deliberately **not** configured here. No Atlas
  Kafka client (verified across ``atlas.outbox.kafka_producer`` and
  ``atlas.consumer.kafka_consumer``) passes a ``'logger'`` callback into
  its client configuration, so librdkafka's own diagnostics never enter
  Python's ``logging`` module at all -- there is nothing to suppress or
  restrict at this layer, and this module does not change that.
- ``sqlalchemy.engine`` -- explicitly held at ``WARNING``. No Atlas
  ``create_engine`` call passes ``echo=True`` anywhere in this codebase
  today, so this logger is already silent; the explicit level is defense
  in depth against a future accidental ``echo=True``, which would
  otherwise log raw SQL (and in some configurations, literal parameter
  values) -- and even then, the fixed ``"sqlalchemy"`` category, never
  the SQL text, is all that could ever be rendered.

As of this correction pass, every Atlas-owned ``logger.*`` call site under
``src/atlas`` that represents meaningful operational behavior (worker
retry/recovery/finalization, the outbox relay's ownership-loss paths, the
dead-letter replay CLI, Redis/heartbeat once-per-outage signals, and the
API's dependency-unavailable responses) has been converted to
:func:`log_event`/:func:`log_exception_boundary`. Atlas-owned application
code therefore no longer depends on ``UNSTRUCTURED_LOG_SUPPRESSED`` during
normal operation -- it remains only as defense in depth for third-party
loggers and any future call site added without a matching
:class:`~atlas.observability.events.Event` member. Should such a call site
be added later, *whatever* it passes as a message, argument, ``exc_info``,
or ``extra`` is still structurally unable to reach a rendered line -- the
fallback branch above reads none of it. This is a stronger guarantee than
"this call site happens to already be sanitized"; it holds regardless of
what any unconverted call site does.

Root-handler replacement
--------------------------

:func:`configure_logging` does not merely *add* its own handler: it
atomically replaces the root logger's entire handler list with exactly
one Atlas-owned handler, every time it is called (see its own docstring).
A foreign handler left in place -- even alongside a safe Atlas handler --
could independently render a record's raw content through its own
formatter, so leaving one behind is not a safe partial state. This means
every active root output handler after :func:`configure_logging` returns
is guaranteed to be this module's own :class:`AtlasJSONFormatter`-backed
handler; there is no way for a pre-existing or later-added foreign
handler to bypass the safe formatter for records that reach the root
logger.
"""

from __future__ import annotations

import json
import logging
import math
import sys
from datetime import UTC, datetime
from typing import Final, TextIO

from atlas.observability.context import current_context
from atlas.observability.events import Event

#: The complete, closed set of fields an Atlas-structured line may carry
#: beyond ``timestamp``/``severity``/``service``/``event`` (which are
#: never passed by callers -- see their own handling below).
_ALLOWED_STRUCTURED_FIELDS: Final[frozenset[str]] = frozenset(
    {
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

#: String-valued fields are opaque and bounded, matching
#: ``atlas.observability.context``'s own limit.
_MAX_FIELD_LENGTH: Final[int] = 256
_TRUNCATION_SUFFIX: Final[str] = "...<truncated>"

#: Recognized process roles for ``configure_logging(service_role=...)``.
#: Deliberately fixed rather than an arbitrary string: a typo here should
#: fail loudly at startup, not silently tag every line with a wrong role.
_KNOWN_SERVICE_ROLES: Final[frozenset[str]] = frozenset(
    {
        "api",
        "worker",
        "outbox-relay",
        "consumer",
        "kafka-topic-init",
        "consumer-replay",
        "alert-receiver",
        "advisor",
    }
)

#: Fixed, closed set of category labels a legacy/third-party record's
#: ``record.name`` is normalized to. The raw logger name is never rendered
#: -- only one of these labels. Kept in sync with
#: ``_configure_third_party_loggers``'s own known logger names below, plus
#: ``"atlas_unconverted"`` (an Atlas call site not yet using ``log_event``)
#: and ``"other"`` (anything matching no known prefix -- a safety net, not
#: an expected steady-state value).
_LOGGER_CATEGORIES: Final[frozenset[str]] = frozenset(
    {
        "uvicorn",
        "psycopg",
        "httpx_httpcore",
        "redis",
        "sqlalchemy",
        "opentelemetry",
        "langsmith",
        "atlas_unconverted",
        "other",
    }
)

#: Ordered ``(prefix, category)`` pairs for :func:`_normalize_logger_category`.
#: A name matches a prefix if it equals the prefix exactly or starts with
#: ``f"{prefix}."`` (dotted-logger-hierarchy semantics, matching how
#: ``logging.getLogger`` names are conventionally structured).
_LOGGER_CATEGORY_PREFIXES: Final[tuple[tuple[str, str], ...]] = (
    ("uvicorn", "uvicorn"),
    ("psycopg", "psycopg"),
    ("httpx", "httpx_httpcore"),
    ("httpcore", "httpx_httpcore"),
    ("redis", "redis"),
    ("sqlalchemy", "sqlalchemy"),
    # opentelemetry's own SDK/exporter internals (Slice 15A3) -- e.g. a
    # queue-full drop warning or an unreachable-Collector export failure.
    # Never configured for level/routing below (unlike the loggers that
    # are): the OTel SDK's own logging already stays at WARNING/ERROR by
    # default and this formatter already renders it exactly as safely as
    # every other unconverted logger regardless -- this prefix only makes
    # that occurrence's category honestly distinguishable from "other".
    ("opentelemetry", "opentelemetry"),
    ("langsmith", "langsmith"),
    ("atlas", "atlas_unconverted"),
)
_DEFAULT_LOGGER_CATEGORY: Final[str] = "other"

_UNSET: Final[object] = object()

#: Set once by ``configure_logging``; read by ``AtlasJSONFormatter`` for
#: every record. Process-lifetime constant -- not a ``contextvars`` value,
#: since it never varies within a process and has no push/pop semantics.
_service_role: str | None = None

#: The handler ``configure_logging`` itself last installed, if any. Tracked
#: only so tests/introspection can identify "the current Atlas handler";
#: ``configure_logging`` itself does not need it to decide what to remove
#: (it unconditionally clears every root handler -- see its own docstring).
_installed_handler: logging.Handler | None = None


class AtlasJSONFormatter(logging.Formatter):
    """Renders every record as one JSON line, using one of the two contracts.

    Never calls ``self.formatException``/reads ``exc_info``/``stack_info``
    on either path -- a formatted traceback or stack dump must never reach
    application JSON logs regardless of how a record was created. For a
    non-Atlas-structured record specifically, never reads
    ``record.getMessage()``, ``record.msg``, ``record.args``, or any
    ``extra`` attribute either -- see this module's docstring for why.

    ``json.dumps(..., allow_nan=False)`` is used for every rendered line:
    a ``NaN``/``Infinity``/``-Infinity`` float is already rejected earlier,
    at :func:`log_event`'s own field validation, but this is defense in
    depth against a record ever being constructed by a path that bypasses
    that validation (e.g. a direct ``Logger.makeRecord`` call in a test, or
    a future bug) -- such a record fails to serialize instead of producing
    invalid JSON.
    """

    def format(self, record: logging.LogRecord) -> str:
        envelope: dict[str, object | None] = {
            "timestamp": _timestamp(record),
            "severity": record.levelname,
            "service": _service_role,
        }
        if getattr(record, "atlas_structured", False):
            envelope.update(_structured_fields(record))
        else:
            envelope["event"] = Event.UNSTRUCTURED_LOG_SUPPRESSED.value
            envelope["logger_category"] = _normalize_logger_category(record.name)
        return json.dumps(envelope, sort_keys=True, allow_nan=False)


def _timestamp(record: logging.LogRecord) -> str:
    return datetime.fromtimestamp(record.created, tz=UTC).isoformat()


def _structured_fields(record: logging.LogRecord) -> dict[str, object | None]:
    ambient = current_context()
    fields: dict[str, object | None] = {"event": getattr(record, "event", None)}
    for name in _ALLOWED_STRUCTURED_FIELDS:
        value = getattr(record, name, _UNSET)
        if value is _UNSET:
            value = ambient.get(name)
        fields[name] = value
    return fields


def _normalize_logger_category(name: str) -> str:
    """Map a record's raw logger name to a fixed, closed category label.

    The raw ``name`` itself is never returned unless it happens to be
    byte-identical to one of the fixed category labels themselves (none of
    which collide with any real logger name in this codebase) -- there is
    no code path through which an arbitrary logger name reaches a rendered
    line.
    """
    for prefix, category in _LOGGER_CATEGORY_PREFIXES:
        if name == prefix or name.startswith(prefix + "."):
            return category
    return _DEFAULT_LOGGER_CATEGORY


def configure_logging(*, service_role: str, stream: TextIO | None = None) -> None:
    """Install Atlas's JSON logging and apply the third-party logger policy.

    Call exactly once, as early as possible in each process entrypoint
    (before constructing any dependency that might itself log).

    ``stream`` selects the handler destination. ``None`` (the default)
    keeps ``sys.stdout``, which is the destination every existing
    service entrypoint relies on. Passing another text stream (the
    advisory CLI uses ``sys.stderr``) does not install a second handler,
    a second formatter, or a weaker replacement policy -- the same
    single :class:`AtlasJSONFormatter`-backed handler is installed on
    the chosen stream.

    Atomically replaces *every* existing root-logger handler -- whatever
    installed it, including a foreign/pre-existing handler this function
    did not itself install (e.g. a library that called
    ``logging.basicConfig()`` first, or a WSGI/ASGI server's own default
    handler) -- with exactly one new Atlas-owned handler using
    :class:`AtlasJSONFormatter`. This is deliberate, not an oversight: a
    foreign handler can carry its own formatter, and nothing about
    :class:`AtlasJSONFormatter` being safe prevents a *different* handler
    on the same root logger from independently rendering a record's raw
    ``msg``/``args``/``exc_info``/``stack_info``/``extra`` through its own
    formatter. The only way to guarantee every active output handler is
    safe is to guarantee there is only ever one, and that it is this one.

    An unknown ``service_role`` raises :class:`ValueError` *before* any
    handler is touched, so a rejected call leaves whatever logging
    configuration already existed completely unchanged.

    Safe to call more than once (e.g. once from application code and
    again from a later boundary): each call still ends with exactly one
    handler on the root logger, so repeated calls never produce duplicate
    Atlas-owned lines and never leave a stale/duplicate handler behind.

    This intentionally does not preserve a test harness's own root-level
    log-capture handler (e.g. pytest's ``caplog``) -- keeping any such
    exception would reintroduce exactly the foreign-handler leak this
    function exists to close. Tests that need to observe what Atlas would
    log should use :func:`atlas.observability.testing.capture_logs`
    instead, which attaches directly to the logger under test and is
    unaffected by this function's root-handler management either way.
    """
    if service_role not in _KNOWN_SERVICE_ROLES:
        raise ValueError(f"unknown service_role: {service_role!r}")

    global _service_role, _installed_handler
    _service_role = service_role

    handler = logging.StreamHandler(stream=sys.stdout if stream is None else stream)
    handler.setFormatter(AtlasJSONFormatter())

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    _installed_handler = handler

    _configure_third_party_loggers()


def _configure_third_party_loggers() -> None:
    """Apply the fixed per-logger third-party policy documented above."""
    for name in ("uvicorn", "uvicorn.error"):
        third_party = logging.getLogger(name)
        third_party.handlers = []
        third_party.propagate = True

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers = []
    access_logger.propagate = False

    logging.getLogger("psycopg.pool").setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("langsmith").setLevel(logging.WARNING)


def log_event(
    logger: logging.Logger,
    event: Event,
    *,
    level: int = logging.INFO,
    **fields: str | int | float | bool | None,
) -> None:
    """Emit one Atlas-structured JSON log line for ``event``.

    ``event`` must be an :class:`~atlas.observability.events.Event`
    member -- there is no f-string/free-text event-name path. Every
    keyword in ``fields`` must be one of the approved structured fields
    (see this module's docstring); anything else raises :class:`ValueError`
    naming only the offending field, never its value, before anything is
    serialized. Every value must be ``None``, ``str``, ``int``, ``float``,
    or ``bool``; anything else (in particular, an exception object itself)
    raises :class:`TypeError` naming only the field and the value's type.

    This is a development-time contract: every call site converted in
    this slice is written to only ever pass allowed fields, so in
    practice this validation exists to be caught by tests, not to recover
    from a real production mistake. It still fails *before* any
    serialization in every case, so even an incorrect call can never leak
    a rejected value -- and if such a call happened to occur from inside
    an existing ``except Exception:`` boundary, the raised
    :class:`ValueError`/:class:`TypeError` itself would be caught there
    and reported (as ``error_class``) rather than propagate further.
    """
    if not isinstance(event, Event):
        raise TypeError("event must be an Event member")

    unsupported = sorted(set(fields) - _ALLOWED_STRUCTURED_FIELDS)
    if unsupported:
        raise ValueError(f"unsupported structured-log field(s): {unsupported!r}")

    validated: dict[str, str | int | float | bool | None] = {}
    for name, value in fields.items():
        validated[name] = _validate_field_value(name, value)

    logger.log(
        level, "", extra={"atlas_structured": True, "event": event.value, **validated}
    )


def _validate_field_value(
    name: str, value: str | int | float | bool | None
) -> str | int | float | bool | None:
    """Validate one field's value; never includes the value itself in an error.

    Numeric values (excluding ``bool``, which is passed through unchanged
    -- a flag, not a quantity) must be finite: ``NaN``/``Infinity``/
    ``-Infinity`` are rejected regardless of field name, since none of
    them survive strict JSON round-tripping safely. ``duration_ms``
    specifically must also be non-negative -- a duration cannot be
    negative; every other approved field currently only ever holds an
    opaque ID/label string in practice, but the type itself still permits
    a number, so the finite check applies to any field that receives one.
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"field {name!r} must be a finite number")
        if name == "duration_ms" and value < 0:
            raise ValueError(f"field {name!r} must be non-negative")
        return value
    if isinstance(value, str):
        if len(value) <= _MAX_FIELD_LENGTH:
            return value
        keep = _MAX_FIELD_LENGTH - len(_TRUNCATION_SUFFIX)
        return value[:keep] + _TRUNCATION_SUFFIX
    raise TypeError(
        f"field {name!r} must be str/int/float/bool/None, got {type(value).__name__}"
    )


def log_exception_boundary(
    logger: logging.Logger,
    event: Event,
    exc: BaseException,
    *,
    level: int = logging.ERROR,
    **fields: str | int | float | bool | None,
) -> None:
    """Emit an Atlas-structured line reporting only ``exc``'s class name.

    Only ``exc.__class__.__name__`` may represent an exception. Never
    ``str(exc)``, ``repr(exc)``, ``exc.args``, ``exc_info``, or
    ``stack_info`` -- none of those are accepted anywhere in this
    function's signature, so there is no way to pass them by mistake.
    """
    if "error_class" in fields:
        raise ValueError("error_class is derived from exc; do not pass it explicitly")
    log_event(logger, event, level=level, error_class=exc.__class__.__name__, **fields)
