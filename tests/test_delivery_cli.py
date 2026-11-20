from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, cast

from durable_outbox import Outbox
from durable_outbox.cli import main
from durable_outbox.delivery import deliver_next
from durable_outbox.testing import FakeTransport, ManualClock


def test_deliver_next_empty_returns_none(outbox: Outbox) -> None:
    assert deliver_next(outbox, FakeTransport(), lease_seconds=30) is None


def test_deliver_success_acks_after_send(outbox: Outbox) -> None:
    outbox.enqueue("k", {"v": 1})
    transport = FakeTransport()
    result = deliver_next(outbox, transport, lease_seconds=30)
    assert result is not None and result.outcome == "delivered"
    assert transport.sent == [("k", {"v": 1})]
    item = outbox.get("k")
    assert item is not None and item.status == "done"


def test_deliver_failure_nacks_with_error(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    transport = FakeTransport(fail_times=1)
    result = deliver_next(outbox, transport, lease_seconds=30)
    assert result is not None and result.outcome == "retry"
    item = outbox.get("k")
    assert item is not None and item.status == "pending"
    assert item.last_error is not None and "fake transport failure" in item.last_error


def test_send_happens_outside_any_transaction(outbox: Outbox) -> None:
    outbox.enqueue("k", 1)
    seen: list[bool] = []

    def send(_payload: object, *, key: str) -> None:
        seen.append(outbox.in_transaction)
        assert key == "k"

    deliver_next(outbox, send, lease_seconds=30)
    assert seen == [False]


def test_lease_expiring_during_send_reports_duplicate_risk(
    outbox: Outbox, clock: ManualClock
) -> None:
    outbox.enqueue("k", 1)

    def slow_send(_payload: object, *, key: str) -> None:
        assert key == "k"
        clock.advance(31)  # the remote call "took" longer than the lease

    result = deliver_next(outbox, slow_send, lease_seconds=30)
    assert result is not None and result.outcome == "sent_but_ack_failed"
    item = outbox.get("k")
    assert item is not None and item.status == "claimed"  # will be redelivered: at-least-once


def test_cli_round_trip(db_path: Path) -> None:
    def run(*argv: str) -> dict[str, Any]:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = main([*argv, "--db", str(db_path)])
        assert code == 0
        return cast("dict[str, Any]", json.loads(buf.getvalue()))

    created = run("enqueue", "--key", "k", "--payload", '{"a": 1}')
    assert isinstance(created, dict) and created["created"] is True
    again = run("enqueue", "--key", "k", "--payload", '{"a": 1}')
    assert isinstance(again, dict) and again["created"] is False
    claim = run("claim", "--lease-seconds", "60")
    assert isinstance(claim, dict) and claim["payload"] == {"a": 1}
    assert run("ack", "--receipt", str(claim["receipt"])) == {"acked": True}
    stats = run("stats")
    assert isinstance(stats, dict) and stats["done"] == 1


def test_receipts_are_safe_to_pass_on_a_command_line(outbox: Outbox) -> None:
    """A receipt starting with '-' would be parsed as an option by `ack --receipt <token>`."""
    for i in range(300):
        outbox.enqueue(f"k{i}", {"v": i})
        claim = outbox.claim(lease_seconds=30)
        assert claim is not None
        assert re.fullmatch(r"[0-9a-f]{48}", claim.receipt)
        outbox.ack(claim.receipt)


def test_cli_reports_errors_with_nonzero_exit(db_path: Path) -> None:
    out = io.StringIO()
    err = io.StringIO()

    with redirect_stdout(out), redirect_stderr(err):
        assert main(["enqueue", "--key", "k", "--payload", "{", "--db", str(db_path)]) == 2
        assert main(["ack", "--receipt", "nope", "--db", str(db_path)]) == 1
        assert main(["enqueue", "--key", "k", "--payload", "1", "--db", str(db_path)]) == 0
        assert main(["enqueue", "--key", "k", "--payload", "2", "--db", str(db_path)]) == 1
    assert "stale" in err.getvalue().lower() or "receipt" in err.getvalue().lower()
