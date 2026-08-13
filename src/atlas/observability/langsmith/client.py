"""Worker-only LangSmith Client lifecycle (Slice 15B).

Construction and shutdown are fail-open. A missing key disables export
without failing the process; a live-provider key requirement is enforced
separately at worker AI composition (see ``composition``).
"""

from __future__ import annotations

import logging
import threading
from typing import Final

from langsmith import Client

from atlas.config.settings import Settings
from atlas.observability.events import Event
from atlas.observability.langsmith.redaction import hide_metadata
from atlas.observability.logging import log_exception_boundary
from atlas.observability.metrics import AtlasMetrics, default_metrics

logger = logging.getLogger(__name__)

FLUSH_BOUND_SECONDS: Final[float] = 5.0


def _export_outcome(exc: BaseException) -> str:
    name = type(exc).__name__
    if "Timeout" in name:
        return "timeout"
    return "error"


class LangSmithHandle:
    """Owns one process's LangSmith Client, or a disabled no-op handle."""

    def __init__(
        self,
        client: Client | None,
        *,
        project: str,
        metrics: AtlasMetrics,
        bound: bool,
    ) -> None:
        self._client = client
        self._project = project
        self._metrics = metrics
        self._bound = bound
        self._closed = False
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._bound and self._client is not None

    @property
    def client(self) -> Client | None:
        return self._client

    @property
    def project(self) -> str:
        return self._project

    def observe(self, *, operation: str, outcome: str) -> None:
        self._metrics.observe_langsmith_operation(operation=operation, outcome=outcome)

    def close(self) -> None:
        """Bounded best-effort ``Client.flush(timeout=...)``; never raises."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if not self.enabled or self._client is None:
            self.observe(operation="flush", outcome="disabled")
            return

        result: dict[str, str] = {"outcome": "success"}

        def _flush() -> None:
            try:
                self._client.flush(timeout=FLUSH_BOUND_SECONDS)  # type: ignore[union-attr]
            except Exception as exc:
                result["outcome"] = _export_outcome(exc)
                log_exception_boundary(
                    logger,
                    Event.LANGSMITH_SHUTDOWN_FAILED,
                    exc,
                    level=logging.WARNING,
                )

        thread = threading.Thread(
            target=_flush, name="atlas-langsmith-flush", daemon=True
        )
        thread.start()
        thread.join(timeout=FLUSH_BOUND_SECONDS)
        if thread.is_alive():
            result["outcome"] = "timeout"
            log_exception_boundary(
                logger,
                Event.LANGSMITH_SHUTDOWN_FAILED,
                TimeoutError(),
                level=logging.WARNING,
            )
        self.observe(operation="flush", outcome=result["outcome"])


_handle: LangSmithHandle | None = None


def current_langsmith() -> LangSmithHandle:
    """Return the process handle, or a disabled handle if none was configured."""
    if _handle is not None:
        return _handle
    return LangSmithHandle(
        None, project="atlas-local", metrics=default_metrics(), bound=False
    )


def configure_langsmith(
    settings: Settings,
    *,
    metrics: AtlasMetrics | None = None,
) -> LangSmithHandle:
    """Construct the process LangSmith Client, or a disabled handle.

    Never raises. A missing key disables export. A Client constructor
    failure logs :attr:`Event.LANGSMITH_INIT_FAILED` and disables export.
    """
    global _handle
    catalog = metrics or default_metrics()
    project = settings.langsmith_project
    key = settings.langsmith_api_key
    secret = key.get_secret_value().strip() if key is not None else ""
    if not secret:
        handle = LangSmithHandle(None, project=project, metrics=catalog, bound=False)
        handle.observe(operation="initialize", outcome="disabled")
        _handle = handle
        return handle

    def _on_export_error(exc: Exception) -> None:
        log_exception_boundary(
            logger,
            Event.LANGSMITH_EXPORT_FAILED,
            exc,
            level=logging.WARNING,
            outcome="export",
        )
        catalog.observe_langsmith_operation(
            operation="export",
            outcome=_export_outcome(exc),
        )

    try:
        kwargs: dict[str, object] = {
            "api_key": secret,
            "hide_inputs": True,
            "hide_outputs": True,
            "hide_metadata": hide_metadata,
            "timeout_ms": settings.langsmith_timeout_ms,
            "auto_batch_tracing": True,
            "tracing_error_callback": _on_export_error,
            "tracing_mode": "langsmith",
            "disable_prompt_cache": True,
            "omit_traced_runtime_info": True,
        }
        if settings.langsmith_api_url is not None:
            kwargs["api_url"] = settings.langsmith_api_url
        client = Client(**kwargs)  # type: ignore[arg-type]
    except Exception as exc:
        log_exception_boundary(logger, Event.LANGSMITH_INIT_FAILED, exc)
        handle = LangSmithHandle(None, project=project, metrics=catalog, bound=False)
        handle.observe(operation="initialize", outcome="error")
        _handle = handle
        return handle

    handle = LangSmithHandle(client, project=project, metrics=catalog, bound=True)
    handle.observe(operation="initialize", outcome="success")
    _handle = handle
    return handle


def reset_langsmith_for_tests() -> None:
    """Drop the process singleton (tests only)."""
    global _handle
    _handle = None
