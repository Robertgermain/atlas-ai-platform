"""Local operator advisory CLI: ``python -m atlas.advisor``.

Production surface:

    python -m atlas.advisor --research-job-id <id>

No HTTP route, no fixture/path/stdin input, no actor ID, no mutation
capability. Snapshot assembly uses a short PostgreSQL READ ONLY
transaction that is rolled back and closed before analysis. Successful
stdout is exactly one non-authoritative ``AdvisoryStdoutEnvelope`` JSON
line; stderr is Atlas structured JSON logs only. Failures leave stdout
empty. Fake mode is the default.

Prometheus observations made by this process are process-local only and
disappear on exit. Durable/external observability is structured logs,
OpenTelemetry, and LangSmith (live).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from typing import Never

from atlas.advisor.catalogs import JOB_ID_PATTERN, MAX_RESEARCH_JOB_ID_LENGTH
from atlas.advisor.composition import (
    build_advisory_service,
    require_advisory_composition,
)
from atlas.advisor.errors import (
    AdvisoryConfigurationError,
    AdvisoryError,
    AdvisoryInputRejectedError,
    AdvisoryJobNotFoundError,
    AdvisorySnapshotRejectedError,
)
from atlas.config import get_settings
from atlas.observability.events import Event
from atlas.observability.langsmith import configure_langsmith
from atlas.observability.langsmith.errors import LangSmithConfigurationError
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)
from atlas.observability.tracing import configure_tracing
from atlas.persistence.db import get_session_factory

logger = logging.getLogger(__name__)


class _AdvisoryArgumentParser(argparse.ArgumentParser):
    """Reject argv without writing argparse usage or error text."""

    def error(self, message: str) -> Never:
        del message
        raise AdvisoryInputRejectedError()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = _AdvisoryArgumentParser(
        prog="python -m atlas.advisor",
        description=(
            "Analyze one research job from sanitized operational facts "
            "(local operator CLI, no HTTP surface, no mutation)."
        ),
    )
    parser.add_argument(
        "--research-job-id",
        required=True,
        help="Research job identifier to analyze.",
    )
    return parser.parse_args(argv)


def _validate_job_id(value: str) -> str:
    cleaned = value.strip()
    if (
        not cleaned
        or len(cleaned) > MAX_RESEARCH_JOB_ID_LENGTH
        or re.fullmatch(JOB_ID_PATTERN, cleaned) is None
    ):
        raise AdvisoryInputRejectedError()
    return cleaned


def main(argv: list[str] | None = None) -> int:
    """Run one advisory analysis. Returns the process exit code."""
    configure_logging(service_role="advisor", stream=sys.stderr)
    try:
        args = _parse_args(argv if argv is not None else sys.argv[1:])
        research_job_id = _validate_job_id(args.research_job_id)
    except AdvisoryInputRejectedError as exc:
        log_exception_boundary(logger, Event.ADVISORY_INPUT_REJECTED, exc)
        return 1
    except SystemExit as exc:
        code = exc.code
        if isinstance(code, int) and code != 0:
            log_event(
                logger,
                Event.ADVISORY_INPUT_REJECTED,
                level=logging.ERROR,
                outcome="argv",
            )
            return 1
        raise

    settings = get_settings()
    try:
        require_advisory_composition(settings)
    except (AdvisoryConfigurationError, LangSmithConfigurationError) as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1

    tracing_handle = configure_tracing(
        service_name="atlas-advisor",
        deployment_environment=settings.otel_deployment_environment,
        otlp_traces_endpoint=settings.otel_exporter_otlp_traces_endpoint,
    )
    langsmith_handle = None
    if settings.advisory_mode == "live":
        langsmith_handle = configure_langsmith(settings)

    exit_code = 1
    try:
        service = build_advisory_service(
            settings, session_factory=get_session_factory()
        )
        envelope = service.analyze_job(research_job_id)
        sys.stdout.write(
            json.dumps(
                envelope.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            + "\n"
        )
        log_event(
            logger,
            Event.ADVISORY_ANALYSIS_FINISHED,
            research_job_id=research_job_id,
            node_name="advise",
            outcome="succeeded",
        )
        exit_code = 0
    except AdvisoryJobNotFoundError as exc:
        log_exception_boundary(
            logger,
            Event.ADVISORY_INPUT_REJECTED,
            exc,
            research_job_id=research_job_id,
        )
    except AdvisorySnapshotRejectedError as exc:
        log_exception_boundary(
            logger,
            Event.ADVISORY_SNAPSHOT_REJECTED,
            exc,
            research_job_id=research_job_id,
        )
    except AdvisoryError as exc:
        log_exception_boundary(
            logger,
            Event.ADVISORY_ANALYSIS_FINISHED,
            exc,
            research_job_id=research_job_id,
            node_name="advise",
            outcome="failed",
        )
    except Exception as exc:
        log_exception_boundary(
            logger,
            Event.ADVISORY_ANALYSIS_FINISHED,
            exc,
            research_job_id=research_job_id,
            node_name="advise",
            outcome="failed",
        )
    finally:
        if langsmith_handle is not None:
            try:
                langsmith_handle.close()
            except Exception as exc:
                log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
                exit_code = 1
        try:
            tracing_handle.close()
        except Exception as exc:
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
            exit_code = 1
        log_event(logger, Event.PROCESS_STOPPED, outcome=str(exit_code))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
