"""Worker process entrypoint: ``python -m atlas.worker``."""

from __future__ import annotations

import logging
import signal
import sys

from atlas.application.worker import ResearchJobWorker
from atlas.config import get_settings
from atlas.coordination.composition import build_heartbeat_recorder
from atlas.evaluation.composition import (
    require_evaluation_composition,
    resolved_evaluation_profile,
)
from atlas.evaluation.errors import SemanticGraderConfigurationError
from atlas.observability.events import Event
from atlas.observability.langsmith import (
    configure_langsmith,
    require_langsmith_for_live_ai,
)
from atlas.observability.langsmith.errors import LangSmithConfigurationError
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.observability.metrics import start_metrics_http_server
from atlas.observability.tracing import configure_tracing
from atlas.persistence.db import get_session_factory
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import (
    LangGraphResearchProcessor,
    create_checkpoint_runtime,
    initialize_checkpointer_schema,
)

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the research-job worker until interrupted."""
    configure_logging(service_role="worker")
    settings = get_settings()
    try:
        require_langsmith_for_live_ai(settings)
        require_evaluation_composition(settings)
    except (LangSmithConfigurationError, SemanticGraderConfigurationError) as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1
    tracing_handle = configure_tracing(
        service_name="atlas-worker",
        deployment_environment=settings.otel_deployment_environment,
        otlp_traces_endpoint=settings.otel_exporter_otlp_traces_endpoint,
    )
    metrics_server = start_metrics_http_server(port=settings.metrics_port)
    session_factory = get_session_factory()
    checkpoint_runtime = create_checkpoint_runtime(settings.database_url)
    try:
        initialize_checkpointer_schema(checkpoint_runtime)
    except Exception as exc:
        # No worker/signal-handler state exists yet -- the pool behind
        # checkpoint_runtime is the only resource to release. Never log
        # str(exc)/repr(exc): a PostgreSQL-driver connection failure can
        # otherwise embed the configured host/port (or, for other
        # exception types, arbitrary text) directly in its own message.
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        try:
            checkpoint_runtime.close()
        except Exception as close_exc:
            # A close failure here must never mask the original
            # classification above -- it is logged separately, and the
            # function still returns 1 for the original failure either way.
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, close_exc)
        metrics_server.close()
        tracing_handle.close()
        return 1
    langsmith_handle = configure_langsmith(settings)
    processor = LangGraphResearchProcessor(
        checkpointer=checkpoint_runtime.checkpointer,
        session_factory=session_factory,
    )
    heartbeat_recorder = build_heartbeat_recorder(settings)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=processor,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        processing_timeout_seconds=settings.worker_processing_timeout_seconds,
        lease_seconds=settings.worker_lease_seconds,
        heartbeat_recorder=heartbeat_recorder,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        evaluation_profile=resolved_evaluation_profile(settings),
    )

    def _handle_signal(signum: int, _frame: object) -> None:
        log_event(logger, Event.SIGNAL_RECEIVED)
        worker.request_shutdown()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log_event(logger, Event.PROCESS_STARTED)
    try:
        worker.run_forever()
    finally:
        checkpoint_runtime.close()
        metrics_server.close()
        langsmith_handle.close()
        tracing_handle.close()
    log_event(logger, Event.PROCESS_STOPPED)
    return 0


if __name__ == "__main__":
    sys.exit(main())
