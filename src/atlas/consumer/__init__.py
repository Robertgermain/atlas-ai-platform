"""Business Kafka consumer: inbox deduplication and lifecycle projection.

Slice 13C2A. See ``python -m atlas.consumer`` (``atlas/consumer/__main__.py``)
for the standalone runtime entry point.
"""

from __future__ import annotations

from atlas.consumer.errors import (
    ConsumerConfigurationError,
    ConsumerError,
    InvalidHeaderError,
    LifecycleOrderViolationError,
    MalformedEnvelopeError,
)
from atlas.consumer.identity import (
    ALLOWED_CONSUMER_GROUP_IDS,
    RESEARCH_JOB_PROJECTION_CLIENT_ID_V1,
    RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
)
from atlas.consumer.ports import (
    ApplyEffect,
    InboxOutcome,
    InboxRepository,
    ProjectionPort,
)
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome

__all__ = [
    "ALLOWED_CONSUMER_GROUP_IDS",
    "RESEARCH_JOB_PROJECTION_CLIENT_ID_V1",
    "RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1",
    "ApplyEffect",
    "ConsumerConfigurationError",
    "ConsumerError",
    "ConsumerRunner",
    "InboxOutcome",
    "InboxRepository",
    "InvalidHeaderError",
    "LifecycleOrderViolationError",
    "MalformedEnvelopeError",
    "ProcessOutcome",
    "ProjectionPort",
]
