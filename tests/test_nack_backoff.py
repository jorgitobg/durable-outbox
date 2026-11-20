from __future__ import annotations

from pathlib import Path

import pytest

from durable_outbox import Outbox, StaleReceiptError
from durable_outbox.testing import ManualClock


def make(
    tmp_path: Path,
    clock: ManualClock,
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
) -> Outbox:
    return Outbox(
        tmp_path / "o.sqlite3",
        clock=clock,
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
    )


def test_stale_receipt_cannot_nack_newly_claimed_item(outbox: Outbox, clock: ManualClock) -> None:
    outbox.enqueue("k", 1)
    first = outbox.claim(lease_seconds=30)
    assert first is not None
    clock.advance(31)
    second = outbox.claim(lease_seconds=30)
    assert second is not None
    with pytest.raises(StaleReceiptError):
        outbox.nack(first.receipt)
    item = outbox.get("k")
    assert item is not None and item.status == "claimed"
    assert item.attempts == 2


def test_nack_schedules_retry_with_exponential_backoff(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock, base_delay=2.0, max_delay=100.0, max_attempts=10) as box:
        box.enqueue("k", 1)
        expected = [2.0, 4.0, 8.0, 16.0]
        for delay in expected:
            claim = box.claim(lease_seconds=30)
            assert claim is not None
            outcome = box.nack(claim.receipt)
            assert outcome.dead is False
            assert outcome.retry_at == clock() + delay
            assert box.claim(lease_seconds=30) is None  # not ready yet
            clock.advance(delay - 0.001)
            assert box.claim(lease_seconds=30) is None
            clock.advance(0.001)


def test_backoff_is_capped_by_max_delay(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock, base_delay=10.0, max_delay=25.0, max_attempts=10) as box:
        box.enqueue("k", 1)
        delays: list[float] = []
        for _ in range(4):
            claim = box.claim(lease_seconds=30)
            assert claim is not None
            out = box.nack(claim.receipt)
            assert out.retry_at is not None
            delays.append(out.retry_at - clock())
            clock.advance(1000)
        assert delays == [10.0, 20.0, 25.0, 25.0]


def test_huge_attempt_count_does_not_overflow_backoff(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock, base_delay=1.0, max_delay=60.0, max_attempts=5000) as box:
        box.enqueue("k", 1)
        box._conn.execute("UPDATE outbox SET attempts = 4000")  # pyright: ignore[reportPrivateUsage]
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        out = box.nack(claim.receipt)
        assert out.retry_at == clock() + 60.0


def test_dead_letter_after_max_attempts(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock, max_attempts=3, base_delay=1.0) as box:
        box.enqueue("k", {"x": 1})
        for attempt in (1, 2, 3):
            claim = box.claim(lease_seconds=30)
            assert claim is not None and claim.attempts == attempt
            out = box.nack(claim.receipt, error=f"boom {attempt}")
            clock.advance(1000)
        assert out.dead is True and out.retry_at is None
        item = box.get("k")
        assert item is not None
        assert item.status == "dead" and item.attempts == 3 and item.last_error == "boom 3"
        assert box.claim(lease_seconds=30) is None
        assert box.stats()["dead"] == 1
        assert [d.key for d in box.dead_letters()] == ["k"]


def test_abandoned_claims_also_exhaust_attempts(tmp_path: Path, clock: ManualClock) -> None:
    """A poison item that keeps crashing its worker must not loop forever."""
    with make(tmp_path, clock, max_attempts=2) as box:
        box.enqueue("k", 1)
        for _ in range(2):
            assert box.claim(lease_seconds=5) is not None
            clock.advance(5)
        assert box.claim(lease_seconds=5) is None
        item = box.get("k")
        assert item is not None and item.status == "dead"
        assert item.last_error == "lease expired"


def test_nack_twice_is_rejected(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        box.nack(claim.receipt)
        with pytest.raises(StaleReceiptError):
            box.nack(claim.receipt)
        with pytest.raises(StaleReceiptError):
            box.ack(claim.receipt)


def test_nack_after_expiry_is_rejected(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        clock.advance(30)
        with pytest.raises(StaleReceiptError):
            box.nack(claim.receipt)


def test_retry_state_survives_reopen(tmp_path: Path, clock: ManualClock) -> None:
    with make(tmp_path, clock, base_delay=5.0) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        box.nack(claim.receipt)
    with make(tmp_path, clock, base_delay=5.0) as box:
        assert box.claim(lease_seconds=30) is None
        clock.advance(5)
        again = box.claim(lease_seconds=30)
        assert again is not None and again.attempts == 2
