"""Worker process entrypoint: ``python -m atlas.worker``."""

from __future__ import annotations

import logging
import signal
import sys

from atlas.application.worker import ResearchJobWorker
from atlas.config import get_settings
from atlas.persistence.db import get_session_factory
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the research-job worker until interrupted."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    worker = ResearchJobWorker(
        session_factory=get_session_factory(),
        repository=SqlAlchemyResearchJobRepository(),
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        processing_timeout_seconds=settings.worker_processing_timeout_seconds,
        lease_seconds=settings.worker_lease_seconds,
    )

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s; requesting worker shutdown", signum)
        worker.request_shutdown()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Starting research-job worker (poll=%ss timeout=%ss lease=%ss)",
        settings.worker_poll_interval_seconds,
        settings.worker_processing_timeout_seconds,
        settings.worker_lease_seconds,
    )
    worker.run_forever()
    logger.info("Research-job worker stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
