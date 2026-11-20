"""Atomic first-time publication: no overwrite, no leftovers, no partial database."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from durable_outbox import Outbox


def _leftovers(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if p.name.endswith(".tmp"))


def test_staging_file_is_removed_after_creation(tmp_path: Path) -> None:
    with Outbox(tmp_path / "o.sqlite3"):
        pass
    assert _leftovers(tmp_path) == []


def test_existing_foreign_file_is_rejected_and_never_modified(tmp_path: Path) -> None:
    foreign = tmp_path / "foreign.sqlite3"
    conn = sqlite3.connect(foreign)
    conn.execute("CREATE TABLE t (a)")
    conn.commit()
    conn.close()
    before = hashlib.sha256(foreign.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="unsupported outbox database"):
        Outbox(foreign)
    assert hashlib.sha256(foreign.read_bytes()).hexdigest() == before
    assert _leftovers(tmp_path) == []


def test_empty_existing_file_is_not_initialised_in_place(tmp_path: Path) -> None:
    empty = tmp_path / "empty.sqlite3"
    empty.write_bytes(b"")
    with pytest.raises(ValueError, match="unsupported outbox database"):
        Outbox(empty)
    assert empty.read_bytes() == b""


def test_mismatching_config_on_second_creator_is_a_clean_rejection(tmp_path: Path) -> None:
    path = tmp_path / "o.sqlite3"
    with Outbox(path, max_attempts=3):
        pass
    with pytest.raises(ValueError, match="config mismatch"):
        Outbox(path, max_attempts=4)
    assert _leftovers(tmp_path) == []


def test_failed_publication_cleans_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    def boom(src: object, dst: object) -> None:
        raise PermissionError("link not permitted")

    monkeypatch.setattr(os, "link", boom)
    with pytest.raises(PermissionError):
        Outbox(tmp_path / "o.sqlite3")
    assert _leftovers(tmp_path) == []
    assert not (tmp_path / "o.sqlite3").exists()
