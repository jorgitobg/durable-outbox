from __future__ import annotations

from pathlib import Path

import pytest

from durable_outbox import Outbox, StaleReceiptError
from durable_outbox.testing import ManualClock


def test_claim_on_empty_returns_none(outbox: Outbox) -> None:
    assert outbox.claim(lease_seconds=30) is None


def test_claim_returns_item_with_receipt_and_deadline(outbox: Outbox, clock: ManualClock) -> None:
    outbox.enqueue("k", {"v": 1})
    claim = outbox.claim(lease_seconds=30)
    assert claim is not None
    assert claim.key == "k"
    assert claim.payload == {"v": 1}
    assert claim.attempts == 1
    assert claim.lease_expires_at == clock() + 30
    assert isinstance(claim.receipt, str) and claim.receipt
    item = outbox.get("k")
    assert item is not None and item.status == "claimed"


def test_claimed_item_is_not_claimed_twice(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    assert outbox.claim(lease_seconds=30) is not None
    assert outbox.claim(lease_seconds=30) is None


def test_ack_marks_done_and_removes_from_queue(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    claim = outbox.claim(lease_seconds=30)
    assert claim is not None
    outbox.ack(claim.receipt)
    item = outbox.get("k")
    assert item is not None and item.status == "done"
    assert outbox.claim(lease_seconds=30) is None
    assert outbox.stats()["done"] == 1


def test_ack_twice_is_rejected(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    claim = outbox.claim(lease_seconds=30)
    assert claim is not None
    outbox.ack(claim.receipt)
    with pytest.raises(StaleReceiptError):
        outbox.ack(claim.receipt)


def test_unknown_receipt_is_rejected(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    with pytest.raises(StaleReceiptError):
        outbox.ack("not-a-receipt")


def test_done_item_dedupes_later_enqueue(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    claim = outbox.claim(lease_seconds=30)
    assert claim is not None
    outbox.ack(claim.receipt)
    again = outbox.enqueue("k", 1)
    assert again.created is False
    assert outbox.claim(lease_seconds=30) is None


def test_ack_state_survives_reopen(db_path: Path, clock: ManualClock) -> None:
    with Outbox(db_path, clock=clock) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        box.ack(claim.receipt)
    with Outbox(db_path, clock=clock) as box:
        item = box.get("k")
        assert item is not None and item.status == "done"


def test_live_lease_survives_reopen_and_receipt_still_valid(
    db_path: Path, clock: ManualClock
) -> None:
    with Outbox(db_path, clock=clock) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
    clock.advance(10)
    with Outbox(db_path, clock=clock) as box:
        assert box.claim(lease_seconds=30) is None  # lease still live
        box.ack(claim.receipt)
        item = box.get("k")
        assert item is not None and item.status == "done"
