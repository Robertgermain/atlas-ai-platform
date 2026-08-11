"""PostgreSQL transactional outbox and fake-producer relay (Slice 13B)."""

from atlas.outbox.clock import ControllableClock, utc_now
from atlas.outbox.errors import (
    EventPublishError,
    FatalEventPublishError,
    KafkaFatalProducerError,
    KafkaProducerConfigurationError,
    KafkaPublishError,
    KafkaPublishTimeoutError,
    KafkaTopicVerificationError,
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
from atlas.outbox.relay import (
    DEFAULT_OUTBOX_BATCH_SIZE,
    OutboxRelay,
    RelayBatchResult,
    RelayRunOutcome,
)
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
    "EventPublishError",
    "FakeEventProducer",
    "FatalEventPublishError",
    "KafkaFatalProducerError",
    "KafkaProducerConfigurationError",
    "KafkaPublishError",
    "KafkaPublishTimeoutError",
    "KafkaTopicVerificationError",
    "OutboxEnqueueError",
    "OutboxEnqueuer",
    "OutboxError",
    "OutboxRelay",
    "OutboxRepository",
    "PostgresOutboxRelayLock",
    "RecordingOutbox",
    "RelayBatchResult",
    "RelayNotOwnerError",
    "RelayOwnershipError",
    "RelayRunOutcome",
    "SqlAlchemyOutboxRepository",
    "utc_now",
]
