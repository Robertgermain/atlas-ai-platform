"""Business Kafka consumer: inbox deduplication, lifecycle projection,
bounded retry, dead-letter storage, and operator replay.

Slice 13C2A/13C2B. See ``python -m atlas.consumer`` (``atlas/consumer/
__main__.py``) for the standalone runtime entry point, and
``python -m atlas.consumer.replay`` for the operator replay CLI.
"""

from __future__ import annotations

from atlas.consumer.errors import (
    ConsumerConfigurationError,
    ConsumerError,
    InvalidHeaderError,
    LifecycleOrderViolationError,
    MalformedEnvelopeError,
    PoisonEventError,
)
from atlas.consumer.identity import (
    ALLOWED_CONSUMER_GROUP_IDS,
    RESEARCH_JOB_PROJECTION_CLIENT_ID_V1,
    RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1,
)
from atlas.consumer.ports import (
    ApplyEffect,
    DeadLetterRepository,
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
    "DeadLetterRepository",
    "InboxOutcome",
    "InboxRepository",
    "InvalidHeaderError",
    "LifecycleOrderViolationError",
    "MalformedEnvelopeError",
    "PoisonEventError",
    "ProcessOutcome",
    "ProjectionPort",
]
