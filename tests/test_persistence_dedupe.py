from __future__ import annotations

from pathlib import Path

import pytest

from durable_outbox import IdempotencyConflictError, Outbox
from durable_outbox.testing import ManualClock


def test_enqueue_persists_pending_item(outbox: Outbox) -> None:
    result = outbox.enqueue("order-1", {"to": "a", "n": 1})
    assert result.created is True
    item = outbox.get("order-1")
    assert item is not None
    assert item.item_id == result.item_id
    assert item.status == "pending"
    assert item.payload == {"to": "a", "n": 1}
    assert item.attempts == 0


def test_get_unknown_key_returns_none(outbox: Outbox) -> None:
    assert outbox.get("missing") is None


def test_duplicate_same_payload_reuses_identity(outbox: Outbox) -> None:
    first = outbox.enqueue("k", {"a": 1, "b": [1, 2]})
    second = outbox.enqueue("k", {"b": [1, 2], "a": 1})  # key order is irrelevant
    assert second.item_id == first.item_id
    assert second.created is False
    assert outbox.stats()["pending"] == 1


def test_conflicting_payload_raises_and_keeps_original(outbox: Outbox) -> None:
    outbox.enqueue("k", {"a": 1})
    with pytest.raises(IdempotencyConflictError):
        outbox.enqueue("k", {"a": 2})
    item = outbox.get("k")
    assert item is not None
    assert item.payload == {"a": 1}
    assert outbox.stats()["pending"] == 1


def test_items_survive_close_and_reopen(db_path: Path, clock: ManualClock) -> None:
    with Outbox(db_path, clock=clock) as box:
        first = box.enqueue("k", ["x"])
    with Outbox(db_path, clock=clock) as box:
        item = box.get("k")
        assert item is not None
        assert item.item_id == first.item_id
        assert box.enqueue("k", ["x"]).created is False


def test_distinct_keys_get_distinct_ids(outbox: Outbox) -> None:
    a = outbox.enqueue("a", 1)
    b = outbox.enqueue("b", 1)
    assert a.item_id != b.item_id
