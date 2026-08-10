"""PostgreSQL transactional outbox and fake-producer relay (Slice 13B)."""

from atlas.outbox.clock import ControllableClock, utc_now
from atlas.outbox.errors import (
    OutboxEnqueueError,
    OutboxError,
    RelayNotOwnerError,
    RelayOwnershipError,
)
from atlas.outbox.fakes import (
    ClockAdvancingProducer,
    FakeEventProducer,
    RecordingOutbox,
)
from atlas.outbox.ports import (
    ClaimedOutboxRecord,
    EventProducer,
    OutboxEnqueuer,
    OutboxRepository,
)
from atlas.outbox.relay import DEFAULT_OUTBOX_BATCH_SIZE, OutboxRelay
from atlas.outbox.relay_lock import (
    OUTBOX_RELAY_ADVISORY_LOCK_KEY,
    PostgresOutboxRelayLock,
)
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository

__all__ = [
    "DEFAULT_OUTBOX_BATCH_SIZE",
    "OUTBOX_RELAY_ADVISORY_LOCK_KEY",
    "ClaimedOutboxRecord",
    "ClockAdvancingProducer",
    "ControllableClock",
    "EventProducer",
    "FakeEventProducer",
    "OutboxEnqueueError",
    "OutboxEnqueuer",
    "OutboxError",
    "OutboxRelay",
    "OutboxRepository",
    "PostgresOutboxRelayLock",
    "RecordingOutbox",
    "RelayNotOwnerError",
    "RelayOwnershipError",
    "SqlAlchemyOutboxRepository",
    "utc_now",
]
