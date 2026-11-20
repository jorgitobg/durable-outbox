"""Delivery explicitly carries the idempotency key and handles malformed failures."""

from __future__ import annotations

from durable_outbox import Outbox
from durable_outbox.delivery import deliver_next
from durable_outbox.testing import FakeTransport


def test_transport_receives_key_kwarg(outbox: Outbox) -> None:
    outbox.enqueue("key-1", {"v": 2})
    received: list[tuple[object, str]] = []

    def send(payload: object, *, key: str) -> None:
        received.append((payload, key))

    result = deliver_next(outbox, send, lease_seconds=10)
    assert result is not None and result.outcome == "delivered"
    assert received == [({"v": 2}, "key-1")]


def test_fake_transport_records_key(outbox: Outbox) -> None:
    outbox.enqueue("key-1", 1)
    transport = FakeTransport()
    assert deliver_next(outbox, transport, lease_seconds=10) is not None
    assert transport.sent == [("key-1", 1)]


class BrokenStrError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("cannot stringify")


def test_broken_exception_string_does_not_orphan_claim(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)

    def fail(_payload: object, *, key: str) -> None:
        raise BrokenStrError(key)

    result = deliver_next(outbox, fail, lease_seconds=10)
    assert result is not None and result.outcome == "retry"
    item = outbox.get("k")
    assert item is not None and item.status == "pending"
    assert item.last_error is not None and "BrokenStrError" in item.last_error


def test_surrogate_exception_and_direct_nack_do_not_orphan_claim(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)

    def fail(_payload: object, *, key: str) -> None:
        raise RuntimeError("bad \ud800 text " + key)

    result = deliver_next(outbox, fail, lease_seconds=10)
    assert result is not None and result.outcome == "retry"
    item = outbox.get("k")
    assert item is not None and item.status == "pending"
    assert item.last_error is not None and "\ud800" not in item.last_error
    assert outbox.claim(lease_seconds=10) is None  # retry not due yet
    outbox.enqueue("other", 2)
    second = outbox.claim(lease_seconds=10)
    assert second is not None and second.key == "other"
    outbox.nack(second.receipt, error="direct \ud800 error")
    other = outbox.get("other")
    assert other is not None and other.status == "pending"
    assert other.last_error is not None and "\ud800" not in other.last_error
