from __future__ import annotations

from pathlib import Path

import pytest

from durable_outbox import Outbox, StaleReceiptError
from durable_outbox.testing import ManualClock


def test_expired_lease_makes_item_claimable_again(outbox: Outbox, clock: ManualClock) -> None:
    outbox.enqueue("k", 1)
    first = outbox.claim(lease_seconds=30)
    assert first is not None
    clock.advance(29.9)
    assert outbox.claim(lease_seconds=30) is None
    clock.advance(0.1)  # exactly at the deadline: lease is over
    second = outbox.claim(lease_seconds=30)
    assert second is not None
    assert second.item_id == first.item_id
    assert second.attempts == 2
    assert second.receipt != first.receipt


def test_stale_receipt_cannot_ack_newly_claimed_item(outbox: Outbox, clock: ManualClock) -> None:
    outbox.enqueue("k", 1)
    first = outbox.claim(lease_seconds=30)
    assert first is not None
    clock.advance(31)
    second = outbox.claim(lease_seconds=30)
    assert second is not None
    with pytest.raises(StaleReceiptError):
        outbox.ack(first.receipt)
    item = outbox.get("k")
    assert item is not None and item.status == "claimed"
    outbox.ack(second.receipt)  # the live receipt still works
    item = outbox.get("k")
    assert item is not None and item.status == "done"


def test_ack_after_expiry_is_rejected_even_if_not_reclaimed(
    outbox: Outbox, clock: ManualClock
) -> None:
    outbox.enqueue("k", 1)
    claim = outbox.claim(lease_seconds=30)
    assert claim is not None
    clock.advance(30)
    with pytest.raises(StaleReceiptError):
        outbox.ack(claim.receipt)


def test_abandoned_lease_recovered_after_restart(db_path: Path, clock: ManualClock) -> None:
    box = Outbox(db_path, clock=clock)
    box.enqueue("k", {"n": 1})
    abandoned = box.claim(lease_seconds=60)
    assert abandoned is not None
    box.close()  # simulates the worker dying without ack/nack
    clock.advance(61)
    with Outbox(db_path, clock=clock) as restarted:
        recovered = restarted.claim(lease_seconds=60)
        assert recovered is not None
        assert recovered.payload == {"n": 1}
        assert recovered.attempts == 2
        with pytest.raises(StaleReceiptError):
            restarted.ack(abandoned.receipt)


def test_recover_expired_returns_count_and_reopens_items(
    outbox: Outbox, clock: ManualClock
) -> None:
    outbox.enqueue("a", 1)
    outbox.enqueue("b", 2)
    outbox.claim(lease_seconds=10)
    outbox.claim(lease_seconds=100)
    clock.advance(10)
    assert outbox.recover_expired() == 1
    assert outbox.stats() == {"pending": 1, "claimed": 1, "done": 0, "dead": 0}
