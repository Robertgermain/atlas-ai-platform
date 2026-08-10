"""Ephemeral coordination (Milestone 13 Slice 13A): rate limiting and worker
heartbeats.

Redis (when enabled) is never authoritative storage: PostgreSQL remains the
source of truth for research jobs. Every port in this package fails open
(favors availability over strict enforcement) when its backing coordination
provider is unavailable, so a Redis outage degrades rate limiting/heartbeats
rather than blocking API requests or worker progress.
"""

from __future__ import annotations

from atlas.coordination.contracts import (
    HeartbeatRecorder,
    RateLimitDecision,
    RateLimiter,
)
from atlas.coordination.errors import RateLimitExceededError

__all__ = [
    "HeartbeatRecorder",
    "RateLimitDecision",
    "RateLimitExceededError",
    "RateLimiter",
]
