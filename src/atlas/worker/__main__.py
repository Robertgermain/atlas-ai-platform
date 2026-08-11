"""Worker process entrypoint: ``python -m atlas.worker``."""

from __future__ import annotations

import logging
import signal
import sys

from atlas.application.worker import ResearchJobWorker
from atlas.config import get_settings
from atlas.coordination.composition import build_heartbeat_recorder
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
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
        logger.error(
            "Failed to initialize the LangGraph checkpoint schema; exiting. "
            "error_class=%s",
            exc.__class__.__name__,
        )
        try:
            checkpoint_runtime.close()
        except Exception as close_exc:
            # A close failure here must never mask the original
            # classification above -- it is logged separately, and the
            # function still returns 1 for the original failure either way.
            logger.error(
                "Failed to close the checkpoint connection pool during "
                "startup-failure cleanup. error_class=%s",
                close_exc.__class__.__name__,
            )
        return 1
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
    )

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s; requesting worker shutdown", signum)
        worker.request_shutdown()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Starting research-job worker %s (poll=%ss timeout=%ss lease=%ss "
        "coordination_provider=%s heartbeat_interval=%ss)",
        worker.worker_id,
        settings.worker_poll_interval_seconds,
        settings.worker_processing_timeout_seconds,
        settings.worker_lease_seconds,
        settings.coordination_provider,
        settings.heartbeat_interval_seconds,
    )
    try:
        worker.run_forever()
    finally:
        checkpoint_runtime.close()
    logger.info("Research-job worker stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
