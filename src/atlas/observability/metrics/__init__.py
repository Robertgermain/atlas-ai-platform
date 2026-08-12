"""Atlas Prometheus metrics (Slice 15A2). See ``catalog``/``exposition``."""

from __future__ import annotations

from atlas.observability.metrics.catalog import AtlasMetrics, default_metrics
from atlas.observability.metrics.exposition import (
    MetricsServerBindError,
    MetricsServerHandle,
    render_metrics,
    render_metrics_safe,
    start_metrics_http_server,
)
from atlas.observability.metrics.normalize import (
    normalize_http_method,
    normalize_http_route,
    normalize_http_status,
)

__all__ = [
    "AtlasMetrics",
    "MetricsServerBindError",
    "MetricsServerHandle",
    "default_metrics",
    "normalize_http_method",
    "normalize_http_route",
    "normalize_http_status",
    "render_metrics",
    "render_metrics_safe",
    "start_metrics_http_server",
]
