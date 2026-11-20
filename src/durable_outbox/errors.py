"""Exception types."""

from __future__ import annotations


class OutboxError(Exception):
    """Base class for all durable-outbox errors."""


class IdempotencyConflictError(OutboxError):
    """The idempotency key already exists with a different payload."""


class StaleReceiptError(OutboxError):
    """The receipt is unknown, already settled, superseded, or its lease expired."""
