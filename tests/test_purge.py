from __future__ import annotations

import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

from durable_outbox import Outbox
from durable_outbox.cli import main
from durable_outbox.testing import ManualClock

DAY = 86_400.0


def _settle(box: Outbox, key: str, *, ack: bool) -> None:
    box.enqueue(key, {"k": key})
    claim = box.claim(lease_seconds=60)
    assert claim is not None and claim.key == key
    if ack:
        box.ack(claim.receipt)
    else:
        assert box.nack(claim.receipt, error="boom").dead


@pytest.fixture
def mixed(db_path: Path, clock: ManualClock) -> Outbox:
    """One row per status (done, dead, claimed, pending), all created at t=1000."""
    box = Outbox(db_path, clock=clock, max_attempts=1)
    _settle(box, "done-1", ack=True)
    _settle(box, "dead-1", ack=False)
    box.enqueue("claimed-1", 2)
    assert box.claim(lease_seconds=6 * DAY) is not None  # lease outlives the test clock jumps
    box.enqueue("pending-1", 1)
    assert box.stats() == {"pending": 1, "claimed": 1, "done": 1, "dead": 1}
    return box


def test_dry_run_is_default_and_deletes_nothing(mixed: Outbox, clock: ManualClock) -> None:
    clock.advance(3 * DAY)
    before = mixed.stats()
    assert mixed.purge(older_than_seconds=DAY, statuses=("done", "dead")) == 2
    assert mixed.stats() == before


def test_purge_deletes_only_selected_terminal_rows(mixed: Outbox, clock: ManualClock) -> None:
    clock.advance(3 * DAY)
    assert mixed.purge(older_than_seconds=DAY, statuses=("done",), dry_run=False) == 1
    assert mixed.get("done-1") is None
    assert mixed.get("dead-1") is not None
    assert mixed.get("pending-1") is not None


def test_pending_and_claimed_are_never_deleted(mixed: Outbox, clock: ManualClock) -> None:
    clock.advance(3 * DAY)
    stats = mixed.stats()
    mixed.purge(older_than_seconds=0, statuses=("done", "dead"), dry_run=False)
    after = mixed.stats()
    assert after["pending"] == stats["pending"] and after["claimed"] == stats["claimed"]
    assert after["done"] == 0 and after["dead"] == 0


@pytest.mark.parametrize("bad", [("pending",), ("claimed",), ("done", "pending"), ("nope",), ()])
def test_non_terminal_or_unknown_statuses_rejected(outbox: Outbox, bad: tuple[str, ...]) -> None:
    outbox.enqueue("p", 1)
    with pytest.raises(ValueError, match="statuses"):
        outbox.purge(older_than_seconds=0, statuses=bad, dry_run=False)
    assert outbox.get("p") is not None


@pytest.mark.parametrize("bad", [-1, float("nan"), float("inf"), True, "1"])
def test_invalid_age_rejected(outbox: Outbox, bad: object) -> None:
    with pytest.raises(ValueError, match="older_than_seconds"):
        outbox.purge(older_than_seconds=bad, statuses=("done",))  # type: ignore[arg-type]


def test_cutoff_boundary_is_strict(outbox: Outbox, clock: ManualClock) -> None:
    _settle(outbox, "k", ack=True)  # created_at == 1000
    clock.advance(DAY)  # age is exactly one day
    assert outbox.purge(older_than_seconds=DAY, statuses=("done",), dry_run=False) == 0
    clock.advance(0.5)
    assert outbox.purge(older_than_seconds=DAY, statuses=("done",), dry_run=False) == 1


def test_purged_key_can_be_enqueued_again(outbox: Outbox, clock: ManualClock) -> None:
    _settle(outbox, "k", ack=True)
    assert outbox.enqueue("k", {"k": "k"}).created is False  # dedupes while the row exists
    clock.advance(2 * DAY)
    outbox.purge(older_than_seconds=DAY, statuses=("done",), dry_run=False)
    again = outbox.enqueue("k", {"k": "k"})
    assert again.created is True
    item = outbox.get("k")
    assert item is not None and item.status == "pending" and item.attempts == 0


def test_second_connection_sees_purge_and_database_reopens(
    db_path: Path, clock: ManualClock
) -> None:
    with Outbox(db_path, clock=clock) as a, Outbox(db_path, clock=clock) as b:
        _settle(a, "k", ack=True)
        clock.advance(2 * DAY)
        assert b.purge(older_than_seconds=DAY, statuses=("done",), dry_run=False) == 1
        assert a.get("k") is None
    with Outbox(db_path, clock=clock) as reopened:  # format validation unchanged
        assert reopened.stats()["done"] == 0


def test_purge_does_not_leave_a_transaction_open(outbox: Outbox) -> None:
    outbox.purge(older_than_seconds=0, statuses=("done",), dry_run=False)
    assert not outbox.in_transaction


def _cli(*argv: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


def test_cli_purge_is_dry_run_without_execute(db_path: Path, clock: ManualClock) -> None:
    with Outbox(db_path, clock=clock) as box:
        _settle(box, "k", ack=True)
    code, out, _ = _cli(
        "purge", "--db", str(db_path), "--older-than-seconds", "0", "--status", "done"
    )
    assert code == 0 and json.loads(out) == {"dry_run": True, "matched": 1}
    code, out, _ = _cli(
        "purge", "--db", str(db_path), "--older-than-seconds", "0", "--status", "done", "--execute"
    )
    assert code == 0 and json.loads(out) == {"dry_run": False, "matched": 1}
    with Outbox(db_path) as box:
        assert box.stats()["done"] == 0


def test_cli_purge_requires_explicit_arguments(db_path: Path) -> None:
    with pytest.raises(SystemExit):
        _cli("purge", "--db", str(db_path), "--status", "done")
    with pytest.raises(SystemExit):
        _cli("purge", "--db", str(db_path), "--older-than-seconds", "1")


def test_cli_purge_rejects_pending_and_missing_database(tmp_path: Path, db_path: Path) -> None:
    with Outbox(db_path):
        pass
    code, _, err = _cli(
        "purge", "--db", str(db_path), "--older-than-seconds", "0", "--status", "pending"
    )
    assert code == 2 and "statuses" in err
    missing = tmp_path / "nope.sqlite3"
    code, _, err = _cli(
        "purge", "--db", str(missing), "--older-than-seconds", "0", "--status", "done"
    )
    assert code == 2 and "does not exist" in err and not missing.exists()
