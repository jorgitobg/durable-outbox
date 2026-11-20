from __future__ import annotations

import math
from pathlib import Path

import pytest

from durable_outbox import Outbox
from durable_outbox.testing import ManualClock

BAD_NUMBERS = [-1.0, math.nan, math.inf, -math.inf]


@pytest.mark.parametrize("bad", [*BAD_NUMBERS, 0.0])
def test_lease_must_be_finite_positive(outbox: Outbox, bad: float) -> None:
    outbox.enqueue("k", 1)
    with pytest.raises(ValueError, match="lease_seconds"):
        outbox.claim(lease_seconds=bad)
    assert outbox.stats()["pending"] == 1


def test_lease_has_upper_bound(outbox: Outbox) -> None:
    with pytest.raises(ValueError, match="lease_seconds"):
        outbox.claim(lease_seconds=10 * 365 * 86400)


@pytest.mark.parametrize("bad", ["1", None, True])
def test_lease_rejects_non_numbers(outbox: Outbox, bad: object) -> None:
    with pytest.raises(ValueError, match="lease_seconds"):
        outbox.claim(lease_seconds=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", BAD_NUMBERS)
def test_delays_must_be_finite_nonnegative(db_path: Path, clock: ManualClock, bad: float) -> None:
    with pytest.raises(ValueError, match="base_delay"):
        Outbox(db_path, clock=clock, base_delay=bad)
    with pytest.raises(ValueError, match="max_delay"):
        Outbox(db_path, clock=clock, max_delay=bad)


def test_max_delay_must_not_be_below_base_delay(db_path: Path, clock: ManualClock) -> None:
    with pytest.raises(ValueError, match="max_delay"):
        Outbox(db_path, clock=clock, base_delay=10.0, max_delay=5.0)


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "3"])
def test_max_attempts_must_be_positive_int(db_path: Path, clock: ManualClock, bad: object) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        Outbox(db_path, clock=clock, max_attempts=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", BAD_NUMBERS)
def test_clock_values_must_be_finite_nonnegative(db_path: Path, bad: float) -> None:
    with Outbox(db_path, clock=lambda: bad) as box:
        with pytest.raises(ValueError, match="clock"):
            box.enqueue("k", 1)
        with pytest.raises(ValueError, match="clock"):
            box.claim(lease_seconds=5)


def test_manual_clock_rejects_bad_values() -> None:
    with pytest.raises(ValueError):
        ManualClock(-1)
    with pytest.raises(ValueError):
        ManualClock(math.nan)
    clock = ManualClock(0)
    for bad in BAD_NUMBERS:
        with pytest.raises(ValueError):
            clock.advance(bad)


@pytest.mark.parametrize("bad", ["", 5, None])
def test_key_must_be_nonempty_string(outbox: Outbox, bad: object) -> None:
    with pytest.raises(ValueError, match="key"):
        outbox.enqueue(bad, 1)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [math.nan, math.inf, {1, 2}, object()])
def test_payload_must_be_strict_json(outbox: Outbox, bad: object) -> None:
    with pytest.raises(ValueError, match="payload"):
        outbox.enqueue("k", bad)
    assert outbox.stats()["pending"] == 0


def test_receipt_must_be_nonempty_string(outbox: Outbox) -> None:
    with pytest.raises(ValueError, match="receipt"):
        outbox.ack("")
    with pytest.raises(ValueError, match="receipt"):
        outbox.nack(None)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [-1, 0, True, 1.5, "5"])
def test_busy_timeout_must_be_positive_int_ms_or_rejected(
    db_path: Path, clock: ManualClock, bad: object
) -> None:
    with pytest.raises(ValueError, match="busy_timeout_ms"):
        Outbox(db_path, clock=clock, busy_timeout_ms=bad)  # type: ignore[arg-type]
