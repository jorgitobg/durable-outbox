from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from durable_outbox import Outbox
from durable_outbox.testing import ManualClock


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(1_000.0)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "outbox.sqlite3"


@pytest.fixture
def outbox(db_path: Path, clock: ManualClock) -> Iterator[Outbox]:
    box = Outbox(db_path, clock=clock)
    try:
        yield box
    finally:
        box.close()
