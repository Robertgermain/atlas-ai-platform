"""Transactional outbox errors (Milestone 13 Slice 13B)."""

from __future__ import annotations


class OutboxError(Exception):
    """Base class for outbox and relay failures."""


class OutboxEnqueueError(OutboxError):
    """Raised when a typed event cannot be durably inserted."""


class RelayOwnershipError(OutboxError):
    """Raised when the singleton outbox-relay advisory lock cannot be acquired."""


class RelayNotOwnerError(OutboxError):
    """Raised when relay work is attempted without holding the advisory lock."""
