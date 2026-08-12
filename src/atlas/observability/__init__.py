"""Atlas observability foundation (Milestone 15).

Slice 15A1 provides structured, sanitized JSON logging and business
correlation context only. It intentionally does not provide tracing
(OpenTelemetry), metrics (Prometheus), dashboards (Grafana), alerting
(Alertmanager), or AI-specific observability (LangSmith) -- those are later
Milestone 15 slices (15A2, 15A3, 15B) and are not implemented here.

Public surface:

- :mod:`atlas.observability.events` -- the fixed, closed ``Event`` name set.
- :mod:`atlas.observability.logging` -- ``configure_logging``, the JSON
  formatter, and the ``log_event``/``log_exception_boundary`` helpers.
- :mod:`atlas.observability.context` -- ``contextvars``-based correlation
  binding (``bind_context``, ``current_context``).
"""

from __future__ import annotations
