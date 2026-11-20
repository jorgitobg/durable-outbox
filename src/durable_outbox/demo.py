"""Synthetic, deterministic demo: fixed data, manual clock, fake transport, no sleeps/network.

Run with ``python -m durable_outbox.demo``. The database lives in a throwaway temporary
directory and is deleted afterwards.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from .delivery import deliver_next
from .errors import IdempotencyConflictError, StaleReceiptError
from .outbox import Outbox
from .testing import FakeTransport, ManualClock


def run_demo() -> list[str]:
    out: list[str] = ["SYNTHETIC DEMO (fake transport, manual clock, no network, no sleeping)"]
    clock = ManualClock(1_000.0)
    with tempfile.TemporaryDirectory(prefix="durable-outbox-demo-") as tmp:
        db = Path(tmp) / "demo.sqlite3"
        with Outbox(db, clock=clock, max_attempts=3, base_delay=2.0, max_delay=30.0) as box:
            # 1. idempotent enqueue
            first = box.enqueue("invoice-001", {"to": "customer-a", "amount": 10})
            again = box.enqueue("invoice-001", {"amount": 10, "to": "customer-a"})
            out.append(
                f"1. duplicate enqueue reused item: id={first.item_id} "
                f"created={first.created} then created={again.created}"
            )
            try:
                box.enqueue("invoice-001", {"to": "customer-a", "amount": 99})
            except IdempotencyConflictError:
                out.append("   conflicting payload rejected")
            box.enqueue("invoice-002", {"to": "customer-b", "amount": 20})
            box.enqueue("invoice-003", {"to": "customer-c", "amount": 30})

            # 2. retry with backoff, then success
            transport = FakeTransport(fail_times=1)
            res = deliver_next(box, transport, lease_seconds=30)
            assert res is not None
            failed = box.get(res.key)
            assert failed is not None
            out.append(f"2. {res.key}: {res.outcome} (attempts={failed.attempts}, backoff 2s)")
            for _ in range(2):
                res = deliver_next(box, transport, lease_seconds=30)
                assert res is not None
                out.append(f"   next ready item {res.key}: {res.outcome}")
            idle = deliver_next(box, transport, lease_seconds=30)
            out.append(f"   nothing else ready yet: {idle}")
            clock.advance(2)
            res = deliver_next(box, transport, lease_seconds=30)
            assert res is not None
            out.append(f"   after +2s of manual clock, {res.key}: {res.outcome}")

            # 3. crashed worker + stale receipt
            box.enqueue("invoice-004", {"to": "customer-d", "amount": 40})
            lost = box.claim(lease_seconds=10)
            assert lost is not None
            out.append(f"3. worker claims {lost.key} then 'crashes' (attempt {lost.attempts})")
            clock.advance(10)
            winner = box.claim(lease_seconds=10)
            assert winner is not None
            out.append(
                f"   after lease expiry {winner.key} is re-claimed (attempt {winner.attempts})"
            )
            try:
                box.ack(lost.receipt)
            except StaleReceiptError:
                out.append("   stale receipt rejected; item still owned by the new claim")
            box.ack(winner.receipt)
            out.append("   new claim acked")

            # 4. send succeeded but ack lost => at-least-once duplicate
            box.enqueue("invoice-005", {"to": "customer-e", "amount": 50})
            remote = FakeTransport()

            def send_then_lease_expires(payload: object, *, key: str) -> None:
                remote(payload, key=key)
                clock.advance(31)  # simulates a slow send / crash before ack

            res = deliver_next(box, send_then_lease_expires, lease_seconds=30)
            assert res is not None
            out.append(f"4. {res.key}: {res.outcome}; remote has received it once")
            res = deliver_next(box, remote, lease_seconds=30)
            assert res is not None
            out.append(
                f"   redelivered -> remote received {len(remote.sent)} copies: "
                "duplicate delivery; consumers must be idempotent"
            )

            # 5. dead-letter after max attempts
            box.enqueue("invoice-006", {"to": "customer-f", "amount": 60})
            broken = FakeTransport(fail_times=99)
            outcomes: list[str] = []
            for _ in range(3):
                clock.advance(100)
                res = deliver_next(box, broken, lease_seconds=30)
                assert res is not None
                outcomes.append(res.outcome)
            out.append(f"5. invoice-006 outcomes {outcomes}: dead-lettered after max_attempts=3")
            out.append(f"   dead letters: {[d.key for d in box.dead_letters()]}")
            out.append(f"final stats: {box.stats()}")
    return out


def main() -> None:
    print("\n".join(run_demo()))


if __name__ == "__main__":
    main()
