"""Reserved Kafka topic names for research-job domain events.

Kafka producers and consumers are deferred to Slice 13C. This constant
documents the reserved topic so envelopes and the outbox stay aligned.
"""

from __future__ import annotations

RESEARCH_JOB_EVENTS_TOPIC_V1 = "atlas.research-job-events.v1"
