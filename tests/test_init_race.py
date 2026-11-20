"""First creation publishes only a complete database, even with competing creators."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, cast

import pytest

from durable_outbox import Outbox


def test_concurrent_first_creators_both_open_complete_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "first.sqlite3"
    entered = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []
    real_connect = sqlite3.connect
    first: threading.Thread

    class PausedConnection(sqlite3.Connection):
        def execute(self, sql: str, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
            if threading.current_thread() is first and sql.lstrip().startswith(
                "CREATE TABLE IF NOT EXISTS outbox ("
            ):
                entered.set()
                if not release.wait(10):
                    raise AssertionError("first creator was not released")
            return super().execute(sql, *args, **kwargs)

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if kwargs.get("uri"):
            return real_connect(*args, **kwargs)
        return cast("sqlite3.Connection", real_connect(*args, factory=PausedConnection, **kwargs))

    monkeypatch.setattr(sqlite3, "connect", connect)

    def create_first() -> None:
        try:
            with Outbox(path, max_attempts=3) as box:
                assert box.max_attempts == 3
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=create_first)
    first.start()
    try:
        assert entered.wait(10), "first creator never reached schema initialization"
        with Outbox(path, max_attempts=3) as second:
            assert second.max_attempts == 3
    finally:
        release.set()
        first.join(timeout=10)
    assert not first.is_alive()
    assert not errors
    with Outbox(path, create=False) as reopened:
        assert reopened.max_attempts == 3
