"""Foreign-format safety, persisted worker settings, and read-only inspection."""

from __future__ import annotations

import hashlib
import io
import sqlite3
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, cast

import pytest

from durable_outbox import Outbox
from durable_outbox.cli import main
from durable_outbox.testing import ManualClock


def test_constructor_closes_connection_after_initialization_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_connect = sqlite3.connect
    opened: list[sqlite3.Connection] = []

    class FailingConnection(sqlite3.Connection):
        def execute(self, sql: str, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
            if sql.startswith("PRAGMA busy_timeout"):
                raise sqlite3.OperationalError("simulated setup failure")
            return super().execute(sql, *args, **kwargs)

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        # Staging, read-only probe and the main connection are all opened; each must close.
        conn = cast(
            "sqlite3.Connection",
            real_connect(*args, factory=FailingConnection, **kwargs),  # type: ignore[call-overload]
        )
        opened.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", connect)
    with pytest.raises(sqlite3.OperationalError, match="simulated setup failure"):
        Outbox(tmp_path / "new.sqlite3")
    assert opened
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")


def test_foreign_database_remains_byte_identical_on_rejection(tmp_path: Path) -> None:
    for sql in ["CREATE TABLE t (x)", "CREATE TABLE outbox (a)"]:
        db = tmp_path / ("foreign" if sql.startswith("CREATE TABLE t ") else "old")
        with sqlite3.connect(db) as conn:
            conn.execute(sql)
        before = hashlib.sha256(db.read_bytes()).digest()
        with pytest.raises(ValueError, match=r"format|schema|version"):
            Outbox(db)
        assert hashlib.sha256(db.read_bytes()).digest() == before
        assert not Path(f"{db}-wal").exists()
        assert not Path(f"{db}-shm").exists()


def test_version_zero_old_prototype_not_silently_migrated(tmp_path: Path) -> None:
    db = tmp_path / "prototype.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE outbox (item_id INTEGER PRIMARY KEY, key TEXT)")
        conn.execute("PRAGMA user_version = 0")
    before = db.read_bytes()
    with pytest.raises(ValueError, match=r"version|format|schema"):
        Outbox(db)
    assert db.read_bytes() == before


def test_version_one_impostor_is_not_treated_as_our_schema(tmp_path: Path) -> None:
    db = tmp_path / "impostor.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE outbox (item_id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, "
            "payload TEXT, status TEXT, attempts INTEGER, available_at REAL, "
            "created_at REAL, receipt TEXT, lease_expires_at REAL, last_error TEXT)"
        )
        conn.execute("CREATE INDEX outbox_ready ON outbox(status, available_at, item_id)")
        conn.execute(
            "CREATE TABLE outbox_config (singleton INTEGER PRIMARY KEY, "
            "max_attempts INTEGER, base_delay REAL, max_delay REAL)"
        )
        conn.execute("INSERT INTO outbox_config VALUES (1, 5, 1.0, 300.0)")
        conn.execute("PRAGMA user_version = 1")
    before = db.read_bytes()
    with pytest.raises(ValueError, match="schema"):
        Outbox(db)
    assert db.read_bytes() == before


def test_read_only_inspection_never_creates_missing_db(tmp_path: Path) -> None:
    db = tmp_path / "typo.sqlite3"
    for command in ["stats", "dead"]:
        err = io.StringIO()
        with redirect_stderr(err), redirect_stdout(io.StringIO()):
            assert main([command, "--db", str(db)]) != 0
        assert not db.exists()
        with pytest.raises((ValueError, sqlite3.Error, FileNotFoundError)):
            Outbox(db, create=False).stats()
        assert not db.exists()


def test_read_only_inspection_of_existing_file_is_non_mutating(tmp_path: Path) -> None:
    db = tmp_path / "o.sqlite3"
    with Outbox(db) as box:
        box.enqueue("k", 1)
    before = hashlib.sha256(db.read_bytes()).digest()
    with Outbox(db, create=False) as box:
        assert box.stats()["pending"] == 1
        assert box.dead_letters() == []
        with pytest.raises(sqlite3.OperationalError):
            box.enqueue("new", 2)
    for command in ["stats", "dead"]:
        with redirect_stdout(io.StringIO()):
            assert main([command, "--db", str(db)]) == 0
    assert hashlib.sha256(db.read_bytes()).digest() == before


def test_config_inherits_after_reopen_and_explicit_conflicts_rejected(tmp_path: Path) -> None:
    db = tmp_path / "o.sqlite3"
    clock = ManualClock(1000)
    with Outbox(db, clock=clock, max_attempts=2, base_delay=7, max_delay=50) as box:
        box.enqueue("k", 1)
        first = box.claim(lease_seconds=1)
        assert first is not None
    with Outbox(db, clock=clock) as box:
        assert box.max_attempts == 2
        clock.advance(2)
        second = box.claim(lease_seconds=1)
        assert second is not None and second.attempts == 2
        box.nack(second.receipt)
        assert box.get("k") is not None and box.get("k").status == "dead"  # type: ignore[union-attr]
    before = hashlib.sha256(db.read_bytes()).digest()
    for kwargs in [{"max_attempts": 3}, {"base_delay": 1}, {"max_delay": 51}]:
        with pytest.raises(ValueError, match=r"config|setting|mismatch"):
            Outbox(db, clock=clock, **kwargs)  # type: ignore[arg-type]
    assert hashlib.sha256(db.read_bytes()).digest() == before


def test_cli_uses_persisted_max_attempts_and_rejects_conflict(tmp_path: Path) -> None:
    db = tmp_path / "o.sqlite3"
    with Outbox(db, max_attempts=2) as box:
        box.enqueue("k", 1)
    with redirect_stdout(io.StringIO()):
        assert main(["claim", "--db", str(db), "--lease-seconds", "30"]) == 0
    with redirect_stderr(io.StringIO()):
        assert main(["claim", "--db", str(db), "--lease-seconds", "30", "--max-attempts", "3"]) != 0
    with Outbox(db) as box:
        assert box.max_attempts == 2
        assert box.get("k") is not None and box.get("k").attempts == 1  # type: ignore[union-attr]


def test_cli_bad_huge_max_attempts_returns_error_not_traceback(tmp_path: Path) -> None:
    db = tmp_path / "o.sqlite3"
    with redirect_stderr(io.StringIO()):
        argv = ["claim", "--db", str(db), "--lease-seconds", "10", "--max-attempts", str(10**100)]
        assert main(argv) == 2
    assert not db.exists()
