from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
from pathlib import Path

from durable_outbox import Outbox
from durable_outbox.testing import ManualClock


def test_claims_follow_ready_order_then_insertion(db_path: Path, clock: ManualClock) -> None:
    with Outbox(db_path, clock=clock, base_delay=10.0) as box:
        box.enqueue("a", 1)
        box.enqueue("b", 2)
        box.enqueue("c", 3)
        first = box.claim(lease_seconds=100)
        assert first is not None and first.key == "a"
        box.nack(first.receipt)  # a becomes ready again at +10
        clock.advance(5)
        box.enqueue("d", 4)  # ready at +5, before a's retry
        order: list[str] = []
        for _ in range(2):
            c = box.claim(lease_seconds=100)
            assert c is not None
            order.append(c.key)
        assert order == ["b", "c"]
        clock.advance(10)
        rest: list[str] = []
        while (c := box.claim(lease_seconds=100)) is not None:
            rest.append(c.key)
        assert rest == ["d", "a"]


def test_two_connections_never_double_claim_sequential(db_path: Path, clock: ManualClock) -> None:
    with Outbox(db_path, clock=clock) as one, Outbox(db_path, clock=clock) as two:
        one.enqueue("k", 1)
        a = one.claim(lease_seconds=30)
        b = two.claim(lease_seconds=30)
        assert a is not None and b is None


def test_threads_with_own_connections_claim_each_row_once(
    db_path: Path, clock: ManualClock
) -> None:
    total = 40
    with Outbox(db_path, clock=clock) as seed:
        for i in range(total):
            seed.enqueue(f"k{i}", i)
    claimed: list[list[int]] = [[] for _ in range(4)]
    errors: list[BaseException] = []

    def worker(slot: int) -> None:
        try:
            with Outbox(db_path, clock=clock) as box:
                while (c := box.claim(lease_seconds=1000)) is not None:
                    claimed[slot].append(c.item_id)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    everything = [i for slot in claimed for i in slot]
    assert len(everything) == total
    assert len(set(everything)) == total


_CHILD = textwrap.dedent(
    """
    import sys
    from durable_outbox import Outbox
    path = sys.argv[1]
    with Outbox(path, clock=lambda: 1000.0) as box:
        while (c := box.claim(lease_seconds=1000)) is not None:
            print(c.item_id)
    """
)


def test_separate_processes_never_double_claim(db_path: Path, clock: ManualClock) -> None:
    total = 60
    with Outbox(db_path, clock=clock) as seed:
        for i in range(total):
            seed.enqueue(f"k{i}", i)
    src = str(Path(__file__).resolve().parent.parent / "src")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-c",
                f"import sys; sys.path.insert(0, {src!r})\n" + _CHILD,
                str(db_path),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        for _ in range(3)
    ]
    ids: list[int] = []
    for p in procs:
        out, _ = p.communicate(timeout=120)
        assert p.returncode == 0
        ids.extend(int(x) for x in out.split())
    assert len(ids) == total
    assert len(set(ids)) == total


def test_claim_holds_write_lock_from_select_to_update(db_path: Path, clock: ManualClock) -> None:
    """Deterministic interleaving: a rival claims while the first claim is mid-flight.

    A SQLite trace hook fires when the first connection starts its UPDATE, i.e. after it has
    already chosen a row. A correct claim holds the write lock there, so the rival cannot
    claim (it times out); a non-atomic claim would let the rival win and then be overwritten.
    """
    import sqlite3

    with (
        Outbox(db_path, clock=clock) as first,
        Outbox(db_path, clock=clock, busy_timeout_ms=50) as rival,
    ):
        first.enqueue("k", 1)
        rival_results: list[object] = []

        def on_statement(sql: str) -> None:
            if sql.startswith("UPDATE outbox SET status = 'claimed'") and not rival_results:
                try:
                    rival_results.append(rival.claim(lease_seconds=30))
                except sqlite3.OperationalError as exc:
                    rival_results.append(exc)

        first._conn.set_trace_callback(on_statement)  # pyright: ignore[reportPrivateUsage]
        mine = first.claim(lease_seconds=30)
        first._conn.set_trace_callback(None)  # pyright: ignore[reportPrivateUsage]
        assert mine is not None
        assert len(rival_results) == 1
        assert rival_results[0] is None or isinstance(rival_results[0], sqlite3.OperationalError)
        assert first.stats()["claimed"] == 1
