"""Fixed, closed set of Atlas structured-log event names (Slice 15A1).

Every call to :func:`atlas.observability.logging.log_event` or
:func:`atlas.observability.logging.log_exception_boundary` must pass a
member of :class:`Event`. There is no free-text or f-string event-name
path: :func:`atlas.observability.logging.log_event` rejects (see its own
validation) anything that is not an :class:`Event` member, so this list is
the complete inventory of Atlas-structured events emitted anywhere in the
codebase, not merely a suggested set. The one exception is
:attr:`Event.UNSTRUCTURED_LOG_SUPPRESSED`, which
:class:`atlas.observability.logging.AtlasJSONFormatter` assigns directly
(never through :func:`~atlas.observability.logging.log_event`) to every
record that is *not* Atlas-structured -- it is still a member of this same
closed set, not a separate free-text path.

The event set here is intentionally coarse-grained and reusable across the
API/worker/outbox-relay/consumer/topic-admin boundaries this slice converts:
the ``service`` field (set once per process by
:func:`atlas.observability.logging.configure_logging`) already identifies
*which* process role emitted a line, and the optional ``error_class``/
``outcome``/business-ID fields already carry *what specifically* happened.
Adding a separate near-duplicate event member per source file/failure would
not add real information and would make the fixed set harder to keep
closed and reviewed over time. New members are added only when an existing
one cannot honestly describe a new boundary.

``CLAIM_OWNERSHIP_LOST`` through ``API_DEPENDENCY_UNAVAILABLE`` (a later
correction pass) convert the remaining meaningful Atlas-owned ``logger.*``
call sites in ``atlas.application.worker``, ``atlas.outbox.relay``,
``atlas.consumer.replay``, ``atlas.coordination.outage_log`` (and its
callers), ``atlas.coordination.heartbeat_thread``, and ``atlas.api.errors``.
After that conversion, no Atlas-owned call site depends on
``UNSTRUCTURED_LOG_SUPPRESSED`` during normal operation -- that fallback
remains only as defense in depth for third-party loggers and any future
call site added without a matching event.
"""

from __future__ import annotations

from enum import StrEnum


class Event(StrEnum):
    """Closed set of event names Atlas-owned structured logging may emit."""

    #: A process-role entrypoint (API/worker/outbox-relay/consumer) finished
    #: its startup sequence and entered its main loop / began serving.
    PROCESS_STARTED = "process_started"

    #: A process-role entrypoint reached its terminal shutdown point (either
    #: a clean stop or after an error path already logged separately).
    PROCESS_STOPPED = "process_stopped"

    #: SIGINT/SIGTERM was received; the process is stopping after the
    #: current unit of work rather than terminating immediately.
    SIGNAL_RECEIVED = "signal_received"

    #: A startup dependency (settings load, database/session construction,
    #: Kafka producer/consumer construction, advisory-lock acquisition,
    #: signal-handler installation) failed before the process could begin
    #: normal operation.
    STARTUP_FAILED = "startup_failed"

    #: Startup-time Kafka broker/topic verification failed (broker
    #: unreachable, topic missing, or an unexpected partition count).
    STARTUP_VERIFICATION_FAILED = "startup_verification_failed"

    #: A best-effort shutdown/cleanup step (producer close, advisory-lock
    #: release, consumer close, signal-handler restoration) failed. Cleanup
    #: steps are always attempted regardless of this outcome; see the
    #: converted entrypoints' own module docstrings for the exact ordering
    #: guarantees this event does not change.
    SHUTDOWN_CLEANUP_FAILED = "shutdown_cleanup_failed"

    #: The poll loop observed a recoverable/backoff-worthy condition
    #: (e.g. a transient Kafka error before a record was in hand, or an
    #: EMPTY/RECOVERABLE_FAILURE/OWNERSHIP_LOST relay outcome) and is
    #: continuing rather than terminating the process.
    POLL_LOOP_RECOVERABLE_ERROR = "poll_loop_recoverable_error"

    #: The poll loop observed a terminal/unexpected condition and is
    #: terminating the process with a nonzero exit code.
    POLL_LOOP_TERMINAL_ERROR = "poll_loop_terminal_error"

    #: Shutdown was requested while a retry backoff was already in
    #: progress; the loop is stopping cleanly with no offset committed for
    #: the in-flight record.
    POLL_LOOP_SHUTDOWN_DURING_BACKOFF = "poll_loop_shutdown_during_backoff"

    #: The API's ``/ready`` endpoint reported not-ready.
    READINESS_CHECK_FAILED = "readiness_check_failed"

    #: The one-shot ``python -m atlas.outbox.topic_admin`` job verified
    #: broker connectivity, created/verified the reserved topic, and
    #: verified its partition count -- all three succeeded.
    TOPIC_ADMIN_SUCCEEDED = "topic_admin_succeeded"

    #: A worker lost claim ownership of a research job (a concurrent
    #: reclaim/expiry raced it) at one of three distinct points --
    #: ``outcome`` distinguishes which: ``"processing"`` (a
    #: ``ClaimOwnershipError`` raised by the processor itself while running),
    #: ``"completion"``, or ``"failure"`` (the finalize step observed it no
    #: longer owned the claim). The claim-token fencing this reports on
    #: already prevented any incorrect finalization; this event is a
    #: visibility signal only.
    CLAIM_OWNERSHIP_LOST = "claim_ownership_lost"

    #: The job processor returned an outcome that intentionally requires no
    #: worker finalization (``PausedForReview``/``RetryScheduled``);
    #: ``outcome`` carries which. Not a failure.
    PROCESSOR_OUTCOME_DEFERRED = "processor_outcome_deferred"

    #: A finalize-completion/finalize-failure database transaction itself
    #: raised (rolled back, job left non-terminal); ``outcome`` is
    #: ``"completion"`` or ``"failure"``, ``error_class`` the exception
    #: class. Always re-raised by the caller after this is logged.
    FINALIZATION_FAILED = "finalization_failed"

    #: A bounded shutdown wait for background work (a job processor or the
    #: heartbeat thread) elapsed before the work stopped; the wait was
    #: abandoned without killing anything. ``outcome`` names which
    #: subsystem (``"processor"``/``"heartbeat_thread"``), ``duration_ms``
    #: the grace period that elapsed.
    SHUTDOWN_WAIT_ABANDONED = "shutdown_wait_abandoned"

    #: The outbox relay lost ownership of a claimed row's lease before it
    #: could finalize the row (mark published, or release a failed claim);
    #: a later owner will resolve it. ``outbox_event_id`` identifies the
    #: row, ``outcome`` the finalize step that lost ownership
    #: (``"mark_published"``/``"release_failed_claim"``).
    OUTBOX_OWNERSHIP_LOST = "outbox_ownership_lost"

    #: The dead-letter replay CLI (``python -m atlas.consumer.replay``)
    #: rejected a command-line argument as invalid before attempting any
    #: replay. ``outcome`` names the rejected flag (e.g.
    #: ``"--dead-letter-id"``).
    REPLAY_INPUT_REJECTED = "replay_input_rejected"

    #: The dead-letter replay CLI's replay attempt did not succeed.
    #: ``outcome`` is ``"rejected"`` (a typed ``ReplayError``, e.g. not
    #: found/not eligible/conflicting idempotency key) or
    #: ``"unexpected_error"``; ``error_class`` the exception class.
    REPLAY_ATTEMPT_FAILED = "replay_attempt_failed"

    #: The dead-letter replay CLI completed a replay attempt without
    #: raising. ``outcome`` is the resulting ``ReplayOutcome`` value (e.g.
    #: ``"applied"``, ``"duplicate"``, ``"rejected"``); the process exit
    #: code separately reflects whether this counts as success.
    REPLAY_FINISHED = "replay_finished"

    #: A fail-open dependency operation (Redis rate-limit check, Redis
    #: heartbeat write) failed; the caller continues under its documented
    #: fail-open policy rather than failing the request/operation.
    #: Suppressed to at most one line per outage episode. ``outcome``
    #: names which operation (``"redis_rate_limit_check"``/
    #: ``"redis_heartbeat_write"``).
    DEPENDENCY_OPERATION_FAILED_OPEN = "dependency_operation_failed_open"

    #: The dedicated heartbeat thread's last-resort catch observed the
    #: configured ``HeartbeatRecorder`` raise something its own fail-open
    #: handling did not already absorb; the thread continues on its next
    #: interval regardless. Suppressed to at most one line per outage
    #: episode. ``error_class`` the exception class.
    HEARTBEAT_RECORDER_UNEXPECTED_ERROR = "heartbeat_recorder_unexpected_error"

    #: An API request failed because a backing dependency (the database, an
    #: embedding provider) was unavailable, misconfigured, rate-limited, or
    #: timed out; the API returns a controlled 503. ``outcome`` names the
    #: dependency/category (e.g. ``"database"``,
    #: ``"embedding_auth_config"``, ``"embedding_timeout"``,
    #: ``"embedding_rate_limited"``, ``"embedding_provider_failed"``).
    API_DEPENDENCY_UNAVAILABLE = "api_dependency_unavailable"

    #: A process's internal-only Prometheus metrics HTTP server (worker,
    #: outbox relay, or Kafka consumer -- the API instead serves ``/metrics``
    #: on its own existing port) failed to bind its configured port at
    #: startup. Fail-open: the process continues without a metrics endpoint.
    #: ``error_class`` the exception class.
    METRICS_SERVER_BIND_FAILED = "metrics_server_bind_failed"

    #: A Prometheus metric observation itself raised (e.g. a label value
    #: rejected by the underlying client library). Contained at the
    #: observation boundary so a metrics failure can never affect the
    #: business operation being observed. ``outcome`` names the observer
    #: boundary (e.g. ``"http_request"``, ``"worker_processing"``),
    #: ``error_class`` the exception class.
    METRIC_OBSERVATION_FAILED = "metric_observation_failed"

    #: The outbox relay's periodic backlog-gauge collection query failed;
    #: the previous gauge values are left unchanged (never a misleading
    #: fresh value) and the separate collection-success timestamp gauge is
    #: not advanced, making the staleness visible to a scraper.
    #: ``error_class`` the exception class.
    OUTBOX_BACKLOG_COLLECTION_FAILED = "outbox_backlog_collection_failed"

    #: A Prometheus scrape's ``generate_latest()`` call itself raised (e.g.
    #: a corrupted collector registered elsewhere in the process). The
    #: caller (the API's ``/metrics`` route, or a role's internal metrics
    #: HTTP server) returns a sanitized ``503`` instead of propagating a
    #: raw traceback; the internal server itself remains alive for a later
    #: scrape. ``error_class`` the exception class.
    METRIC_EXPOSITION_FAILED = "metric_exposition_failed"

    #: A role's internal-only Prometheus metrics HTTP server's shutdown
    #: itself raised or did not complete within its documented bound while
    #: closing (``MetricsServerHandle.close()``). Best-effort: the socket is
    #: still closed and the underlying thread is still joined with a bounded
    #: timeout regardless of this outcome. ``error_class`` the exception
    #: class, when one was raised.
    METRICS_SERVER_SHUTDOWN_FAILED = "metrics_server_shutdown_failed"

    #: A process's OpenTelemetry ``TracerProvider``/exporter/processor
    #: construction failed at startup (Slice 15A3). Fail-open: the process
    #: continues with tracing effectively disabled (spans are still created
    #: in-process by the API, but never exported) rather than failing
    #: startup. ``error_class`` the exception class.
    TRACING_INIT_FAILED = "tracing_init_failed"

    #: A process's bounded OpenTelemetry shutdown/flush
    #: (``TracingProviderHandle.close()``) itself raised or did not complete
    #: within its documented bound. Best-effort: the process continues
    #: shutting down regardless. ``error_class`` the exception class, when
    #: one was raised.
    TRACING_SHUTDOWN_FAILED = "tracing_shutdown_failed"

    #: Worker LangSmith Client construction failed (Slice 15B). Fail-open:
    #: the process continues with LangSmith export disabled rather than
    #: failing startup. ``error_class`` the exception class.
    LANGSMITH_INIT_FAILED = "langsmith_init_failed"

    #: A LangSmith enqueue, context enter/exit, hide callback, or background
    #: export failed. Contained: never fails a research job.
    #: ``outcome`` names the phase (``"enqueue"``/``"export"``);
    #: ``error_class`` the exception class.
    LANGSMITH_EXPORT_FAILED = "langsmith_export_failed"

    #: Bounded LangSmith ``Client.flush`` at process shutdown raised or did
    #: not complete within its documented bound. Best-effort: the process
    #: continues shutting down. ``error_class`` the exception class, when
    #: one was raised.
    LANGSMITH_SHUTDOWN_FAILED = "langsmith_shutdown_failed"

    #: The Atlas-owned internal Alertmanager webhook receiver
    #: (``atlas.observability.alert_receiver``) accepted and recorded one
    #: webhook delivery into its bounded in-memory ring buffer. ``outcome``
    #: is the normalized alert status (``"firing"``/``"resolved"``/
    #: ``"other"``). Never logs the request body, headers, annotations,
    #: labels, URL, or alert fingerprint -- those are available only via the
    #: receiver's own bounded ``GET /received`` endpoint, never through this
    #: structured-log line.
    ALERT_WEBHOOK_RECEIVED = "alert_webhook_received"

    #: A log record arrived from a logger Atlas does not control (a
    #: third-party dependency) or from an Atlas call site not yet converted
    #: to :func:`atlas.observability.logging.log_event`. Atlas records only
    #: that this occurred, plus a normalized, fixed logger category (see
    #: :mod:`atlas.observability.logging`) -- never the record's own
    #: message, formatting arguments, exception info, stack info, ``extra``,
    #: or raw logger name. This is the *only* event a non-Atlas-structured
    #: record may ever be rendered as.
    UNSTRUCTURED_LOG_SUPPRESSED = "unstructured_log_suppressed"

    #: Advisory CLI rejected argv or a missing research job (Slice 15C2).
    ADVISORY_INPUT_REJECTED = "advisory_input_rejected"

    #: Advisory snapshot assembly failed closed (schema, catalog, or byte bound).
    ADVISORY_SNAPSHOT_REJECTED = "advisory_snapshot_rejected"

    #: Advisory analysis finished (success or contained failure). Outcome is
    #: bounded. Success already wrote the non-authoritative envelope to
    #: stdout; this event is emitted on stderr with the other process logs.
    ADVISORY_ANALYSIS_FINISHED = "advisory_analysis_finished"
