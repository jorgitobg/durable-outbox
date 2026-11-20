"""Claim -> send -> ack/nack glue. The send happens with no database transaction open."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from .errors import StaleReceiptError
from .outbox import Outbox


class Transport(Protocol):
    def __call__(self, payload: Any, /, *, key: str) -> None: ...


def _error_message(exc: Exception) -> str:
    try:
        detail = str(exc)
    except Exception:
        try:
            detail = repr(exc)
        except Exception:
            detail = "<unprintable exception>"
    return f"{type(exc).__name__}: {detail}".encode("utf-8", errors="replace").decode("utf-8")


DeliveryOutcome = Literal[
    "delivered", "retry", "dead", "sent_but_ack_failed", "failed_and_lease_lost"
]


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    key: str
    outcome: DeliveryOutcome


def deliver_next(
    outbox: Outbox, transport: Transport, *, lease_seconds: float
) -> DeliveryResult | None:
    """Claim one ready item and hand it to ``transport``.

    Delivery is at-least-once. If ``transport`` succeeds but the ack is rejected (lease expired,
    process crash between the two steps), the item will be delivered again; receivers must be
    idempotent (the item ``key`` is a suitable idempotency key).
    """
    claim = outbox.claim(lease_seconds=lease_seconds)
    if claim is None:
        return None
    try:
        transport(claim.payload, key=claim.key)
    except Exception as exc:
        message = _error_message(exc)
        try:
            nack = outbox.nack(claim.receipt, error=message)
        except StaleReceiptError:
            return DeliveryResult(claim.key, "failed_and_lease_lost")
        return DeliveryResult(claim.key, "dead" if nack.dead else "retry")
    try:
        outbox.ack(claim.receipt)
    except StaleReceiptError:
        return DeliveryResult(claim.key, "sent_but_ack_failed")
    return DeliveryResult(claim.key, "delivered")
