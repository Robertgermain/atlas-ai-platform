"""Fixed, allowlisted Kafka consumer identities (Slice 13C2A).

Mirrors ``atlas.eventing.topic.RESEARCH_JOB_EVENTS_TOPIC_V1``: every
constant here is fixed at import time and there is no settings-driven or
runtime-supplied way to select a different consumer group or client id.
``KafkaEventConsumer`` rejects any ``group_id`` not present in
``ALLOWED_CONSUMER_GROUP_IDS`` and never accepts a caller-supplied
``client_id`` at all -- it looks up the fixed client id for the (already
validated) group from :data:`CLIENT_ID_BY_CONSUMER_GROUP_ID` internally.
"""

from __future__ import annotations

#: The sole business consumer implemented in Slice 13C2A: a durable,
#: non-authoritative research-job lifecycle projection.
RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1 = "atlas.research-job-projection.v1"
RESEARCH_JOB_PROJECTION_CLIENT_ID_V1 = "atlas-research-job-projection-consumer"

#: Fixed group-id -> client-id mapping. Extend this (via a future migration
#: adding the new group id to the database CHECK constraint too) when a
#: second business consumer is justified. Never widen it to accept an
#: arbitrary runtime group or client id.
CLIENT_ID_BY_CONSUMER_GROUP_ID: dict[str, str] = {
    RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1: RESEARCH_JOB_PROJECTION_CLIENT_ID_V1,
}

ALLOWED_CONSUMER_GROUP_IDS: frozenset[str] = frozenset(CLIENT_ID_BY_CONSUMER_GROUP_ID)
