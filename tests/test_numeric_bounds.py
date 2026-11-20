"""Numeric boundaries: every bad number is a ValueError, raised before any state changes."""

from __future__ import annotations

from pathlib import Path

import pytest

from durable_outbox import Outbox
from durable_outbox.testing import ManualClock

MAX_ATTEMPTS_LIMIT = 2**31 - 1  # documented in docs/semantics.md
MAX_CLOCK = 1e12  # documented in docs/semantics.md


@pytest.mark.parametrize("bad", [2**31, 2**63, 2**64, 10**100])
def test_huge_max_attempts_is_value_error_before_database_is_opened(
    tmp_path: Path, clock: ManualClock, bad: int
) -> None:
    path = tmp_path / "never-created.sqlite3"
    with pytest.raises(ValueError, match="max_attempts"):
        Outbox(path, clock=clock, max_attempts=bad)
    assert not path.exists()


def test_max_attempts_upper_bound_is_usable(tmp_path: Path, clock: ManualClock) -> None:
    with Outbox(tmp_path / "o.sqlite3", clock=clock, max_attempts=MAX_ATTEMPTS_LIMIT) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=10)
        assert claim is not None and claim.attempts == 1
        assert box.nack(claim.receipt).dead is False


def test_manual_clock_huge_integers_are_value_errors() -> None:
    with pytest.raises(ValueError, match="start"):
        ManualClock(10**1000)
    clock = ManualClock(1000)
    with pytest.raises(ValueError, match="seconds"):
        clock.advance(10**1000)


@pytest.mark.parametrize("name", ["base_delay", "max_delay"])
@pytest.mark.parametrize("huge", [10**400, 10**1000, -(10**400)])
def test_huge_int_delays_are_value_errors(
    tmp_path: Path, clock: ManualClock, name: str, huge: int
) -> None:
    with pytest.raises(ValueError, match=name):
        Outbox(tmp_path / "o.sqlite3", clock=clock, **{name: huge})  # type: ignore[arg-type]


@pytest.mark.parametrize("huge", [10**400, 10**1000, -(10**400)])
def test_huge_int_lease_is_value_error(outbox: Outbox, huge: int) -> None:
    outbox.enqueue("k", 1)
    with pytest.raises(ValueError, match="lease_seconds"):
        outbox.claim(lease_seconds=huge)
    assert outbox.stats()["pending"] == 1


@pytest.mark.parametrize("huge", [10**400, 10**1000, -(10**400), 1e308, MAX_CLOCK * 1.0001])
def test_clock_outside_supported_range_is_value_error(db_path: Path, huge: float) -> None:
    with Outbox(db_path, clock=lambda: huge) as box:
        with pytest.raises(ValueError, match="clock"):
            box.enqueue("k", 1)
        with pytest.raises(ValueError, match="clock"):
            box.claim(lease_seconds=10)
        with pytest.raises(ValueError, match="clock"):
            box.recover_expired()
        with pytest.raises(ValueError, match="clock"):
            box.ack("r")
        with pytest.raises(ValueError, match="clock"):
            box.nack("r")
        assert box.stats() == {"pending": 0, "claimed": 0, "done": 0, "dead": 0}


def test_clock_at_supported_upper_bound_still_works(db_path: Path) -> None:
    with Outbox(db_path, clock=lambda: MAX_CLOCK) as box:
        box.enqueue("k", 1)
        claim = box.claim(lease_seconds=10)
        assert claim is not None and claim.lease_expires_at > MAX_CLOCK
        box.ack(claim.receipt)


@pytest.mark.parametrize(
    ("now", "lease"),
    [(1.7e9, 1e-9), (1000.0, 5e-324), (MAX_CLOCK, 1e-9), (1e12, 1e-5)],
)
def test_lease_that_would_not_outlive_now_is_rejected_without_state_change(
    db_path: Path, now: float, lease: float
) -> None:
    clock = ManualClock(now)
    with Outbox(db_path, clock=clock) as box:
        box.enqueue("k", 1)
        with pytest.raises(ValueError, match="lease"):
            box.claim(lease_seconds=lease)
        item = box.get("k")
        assert item is not None and item.status == "pending" and item.attempts == 0
        good = box.claim(lease_seconds=10)
        assert good is not None and good.lease_expires_at > clock()
        box.ack(good.receipt)  # a rejected lease never produces a born-stale receipt


def test_invalid_claim_rolls_back_expired_lease_recovery(db_path: Path) -> None:
    clock = ManualClock(1.7e9)
    with Outbox(db_path, clock=clock) as box:
        box.enqueue("k", 1)
        assert box.claim(lease_seconds=10) is not None
        clock.advance(20)  # the lease is now expired but not yet recovered
        with pytest.raises(ValueError, match="lease"):
            box.claim(lease_seconds=1e-9)
        item = box.get("k")
        assert item is not None and item.status == "claimed" and item.attempts == 1


class LockProbeClock:
    """A clock that records whether the connection already holds the write lock when read."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start
        self.box: Outbox | None = None
        self.in_txn: list[bool] = []

    def __call__(self) -> float:
        assert self.box is not None
        self.in_txn.append(self.box.in_transaction)
        return self.now


@pytest.mark.parametrize("operation", ["enqueue", "claim", "recover", "ack", "nack"])
def test_clock_is_read_after_write_lock_is_held(db_path: Path, operation: str) -> None:
    probe = LockProbeClock()
    with Outbox(db_path, clock=probe) as box:
        probe.box = box
        box.enqueue("seed", 1)
        claim = box.claim(lease_seconds=30)
        assert claim is not None
        probe.in_txn.clear()
        match operation:
            case "enqueue":
                box.enqueue("other", 2)
            case "claim":
                box.claim(lease_seconds=30)
            case "recover":
                box.recover_expired()
            case "ack":
                box.ack(claim.receipt)
            case "nack":
                box.nack(claim.receipt)
            case _:
                raise AssertionError(operation)
        assert probe.in_txn, "the clock was never read"
        assert all(probe.in_txn), f"{operation} read the clock before taking the write lock"
