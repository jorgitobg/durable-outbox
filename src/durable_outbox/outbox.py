"""SQLite-backed durable outbox."""

from __future__ import annotations

import contextlib
import json
import math
import os
import secrets
import sqlite3
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Literal
from urllib.parse import quote

from .errors import IdempotencyConflictError, StaleReceiptError

Status = Literal["pending", "claimed", "done", "dead"]
Clock = Callable[[], float]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    item_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    key          TEXT NOT NULL UNIQUE,
    payload      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    available_at REAL NOT NULL,
    created_at   REAL NOT NULL,
    receipt      TEXT UNIQUE,
    lease_expires_at REAL,
    last_error   TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox (status, available_at, item_id);
CREATE TABLE IF NOT EXISTS outbox_config (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    max_attempts INTEGER NOT NULL,
    base_delay REAL NOT NULL,
    max_delay REAL NOT NULL
);
"""


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    item_id: int
    created: bool


@dataclass(frozen=True, slots=True)
class Item:
    item_id: int
    key: str
    payload: Any
    status: Status
    attempts: int
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class NackOutcome:
    dead: bool
    retry_at: float | None


@dataclass(frozen=True, slots=True)
class Claim:
    item_id: int
    key: str
    payload: Any
    attempts: int
    receipt: str
    lease_expires_at: float


MAX_LEASE_SECONDS = 7 * 86400.0
MAX_DELAY_SECONDS = 365 * 86400.0
MAX_ATTEMPTS = 2**31 - 1
MAX_CLOCK_SECONDS = 1e12
_VERSION = 1
_PURGEABLE = frozenset({"done", "dead"})
_COLUMNS = (
    "item_id",
    "key",
    "payload",
    "status",
    "attempts",
    "available_at",
    "created_at",
    "receipt",
    "lease_expires_at",
    "last_error",
)


def _validate_format(conn: sqlite3.Connection) -> tuple[int, float, float]:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version != _VERSION:
        raise ValueError("unsupported outbox database format/version; no automatic migration")
    definitions = {
        str(name): sql for name, sql in conn.execute("SELECT name, sql FROM sqlite_master")
    }
    expected = {
        "sqlite_sequence": "CREATE TABLE sqlite_sequence(name,seq)",
        "sqlite_autoindex_outbox_1": None,
        "sqlite_autoindex_outbox_2": None,
    }
    for statement in _SCHEMA.split(";"):
        statement = statement.strip()
        if statement:
            name = statement.split("IF NOT EXISTS ", 1)[1].split()[0].split("(", 1)[0]
            expected[name] = statement.replace("IF NOT EXISTS ", "")
    if definitions != expected:
        raise ValueError("unsupported outbox database schema; no automatic migration")
    columns = tuple(str(row[1]) for row in conn.execute("PRAGMA table_info(outbox)"))
    config_columns = tuple(str(row[1]) for row in conn.execute("PRAGMA table_info(outbox_config)"))
    indexes = {str(row[1]) for row in conn.execute("PRAGMA index_list(outbox)")}
    if (
        columns != _COLUMNS
        or config_columns != ("singleton", "max_attempts", "base_delay", "max_delay")
        or "outbox_ready" not in indexes
    ):
        raise ValueError("unsupported outbox database schema; no automatic migration")
    rows = conn.execute(
        "SELECT max_attempts, base_delay, max_delay FROM outbox_config WHERE singleton = 1"
    ).fetchall()
    if len(rows) != 1 or conn.execute("SELECT COUNT(*) FROM outbox_config").fetchone()[0] != 1:
        raise ValueError("unsupported outbox database config format")
    attempts, base, maximum = rows[0]
    if not isinstance(attempts, int) or not 1 <= attempts <= MAX_ATTEMPTS:
        raise ValueError("unsupported outbox database config: max_attempts")
    base = _finite_nonneg("stored base_delay", base)
    maximum = _finite_nonneg("stored max_delay", maximum)
    if maximum < base or maximum > MAX_DELAY_SECONDS:
        raise ValueError("unsupported outbox database config: max_delay")
    return attempts, base, maximum


def _finite_nonneg(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{name} must be a finite number >= 0, got {value!r}")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be a finite number >= 0") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be a finite number >= 0, got {value!r}")
    return number


def _publish_new_database(target: Path, config: tuple[int, float, float]) -> None:
    """Create a complete database next to ``target`` and publish it without overwriting.

    The staging file is fully initialised, committed and closed before ``os.link`` makes it
    visible, so no other opener can ever see a half-initialised database. If another creator
    wins the race (or the path appeared meanwhile), ``FileExistsError`` is swallowed and the
    caller validates whatever is at ``target`` as an ordinary existing file: a foreign file is
    rejected, never initialised. The staging name is always removed.
    """
    staging = target.with_name(f".{target.name}.init-{secrets.token_hex(8)}.tmp")
    try:
        conn = sqlite3.connect(str(staging), isolation_level=None)
        try:
            conn.execute("BEGIN IMMEDIATE")
            for statement in _SCHEMA.split(";"):
                if statement.strip():
                    conn.execute(statement)
            conn.execute("INSERT INTO outbox_config VALUES (1, ?, ?, ?)", config)
            conn.execute(f"PRAGMA user_version = {_VERSION}")
            conn.execute("COMMIT")
        finally:
            conn.close()
        with contextlib.suppress(FileExistsError):
            os.link(staging, target)
    finally:
        staging.unlink(missing_ok=True)


def _canonical(payload: Any) -> str:
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError(f"payload must be strict JSON-serializable: {exc}") from exc


def _nonempty_str(name: str, value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


class Outbox:
    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock = time.time,
        max_attempts: int | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
        busy_timeout_ms: int = 30_000,
        create: bool = True,
    ) -> None:
        if max_attempts is not None and (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)  # pyright: ignore[reportUnnecessaryIsInstance]
            or not 1 <= max_attempts <= MAX_ATTEMPTS
        ):
            raise ValueError(f"max_attempts must be an int in [1, {MAX_ATTEMPTS}]")
        if (
            isinstance(busy_timeout_ms, bool)
            or not isinstance(busy_timeout_ms, int)  # pyright: ignore[reportUnnecessaryIsInstance]
            or not 1 <= busy_timeout_ms <= 3_600_000
        ):
            raise ValueError(
                f"busy_timeout_ms must be an int in [1, 3600000], got {busy_timeout_ms!r}"
            )
        base = _finite_nonneg("base_delay", base_delay) if base_delay is not None else None
        maximum = _finite_nonneg("max_delay", max_delay) if max_delay is not None else None
        if base is not None and base > MAX_DELAY_SECONDS:
            raise ValueError(f"base_delay must be <= {MAX_DELAY_SECONDS}")
        if maximum is not None and (
            maximum > MAX_DELAY_SECONDS or (base is not None and maximum < base)
        ):
            raise ValueError(f"max_delay must be between base_delay and {MAX_DELAY_SECONDS}")
        self._clock = clock
        location = str(path)
        memory = location == ":memory:"
        existing = not memory and Path(location).exists()
        if create and not memory and not existing:
            # Publish a complete database atomically; never initialise a file in place.
            a = 5 if max_attempts is None else max_attempts
            b = 1.0 if base is None else base
            m = 300.0 if maximum is None else maximum
            if m < b:
                raise ValueError("max_delay must be >= base_delay")
            _publish_new_database(Path(location), (a, b, m))
            existing = True
        # Check the file before any mutable PRAGMA or write-capable connection.
        if existing:
            probe = sqlite3.connect(
                f"file:{quote(str(Path(location).resolve()))}?mode=ro", uri=True
            )
            try:
                checked = _validate_format(probe)
                if (
                    (max_attempts is not None and max_attempts != checked[0])
                    or (base is not None and base != checked[1])
                    or (maximum is not None and maximum != checked[2])
                ):
                    raise ValueError("outbox config mismatch: explicit setting differs from stored")
            finally:
                probe.close()
        elif not create:
            raise ValueError("outbox database does not exist (create=False)")
        self._conn = sqlite3.connect(
            location if create else f"file:{quote(str(Path(location).resolve()))}?mode=ro",
            uri=not create,
            isolation_level=None,
            timeout=busy_timeout_ms / 1000,
        )
        try:
            if create:
                self._conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
                # Recheck under the write lock to cover races after the read-only probe.
                self._conn.execute("BEGIN IMMEDIATE")
                version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
                tables = {
                    str(row[0])
                    for row in self._conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if version == 0 and not tables:
                    a = 5 if max_attempts is None else max_attempts
                    b = 1.0 if base is None else base
                    m = 300.0 if maximum is None else maximum
                    if m < b:
                        raise ValueError("max_delay must be >= base_delay")
                    for statement in _SCHEMA.split(";"):
                        if statement.strip():
                            self._conn.execute(statement)
                    self._conn.execute("INSERT INTO outbox_config VALUES (1, ?, ?, ?)", (a, b, m))
                    self._conn.execute(f"PRAGMA user_version = {_VERSION}")
                    stored = (a, b, m)
                else:
                    stored = _validate_format(self._conn)
                if (
                    (max_attempts is not None and max_attempts != stored[0])
                    or (base is not None and base != stored[1])
                    or (maximum is not None and maximum != stored[2])
                ):
                    raise ValueError("outbox config mismatch: explicit setting differs from stored")
                self._conn.execute("COMMIT")
                if not memory:
                    self._conn.execute("PRAGMA journal_mode = WAL")
                self._conn.execute("PRAGMA synchronous = FULL")
            else:
                stored = _validate_format(self._conn)
                if (
                    (max_attempts is not None and max_attempts != stored[0])
                    or (base is not None and base != stored[1])
                    or (maximum is not None and maximum != stored[2])
                ):
                    raise ValueError("outbox config mismatch: explicit setting differs from stored")
            self._max_attempts, self._base_delay, self._max_delay = stored
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            self._conn.close()
            raise

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    def _now(self) -> float:
        value = self._clock()
        try:
            number = _finite_nonneg("clock value", value)
            if number > MAX_CLOCK_SECONDS:
                raise ValueError("clock exceeds supported upper bound")
            return number
        except ValueError as exc:
            raise ValueError(f"clock returned an invalid time: {value!r}") from exc

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Outbox:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def enqueue(self, key: str, payload: Any) -> EnqueueResult:
        _nonempty_str("key", key)
        text = _canonical(payload)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            row = self._conn.execute(
                "SELECT item_id, payload FROM outbox WHERE key = ?", (key,)
            ).fetchone()
            if row is not None:
                if row[1] != text:
                    raise IdempotencyConflictError(f"key {key!r} exists with a different payload")
                result = EnqueueResult(int(row[0]), False)
            else:
                cur = self._conn.execute(
                    "INSERT INTO outbox (key, payload, available_at, created_at) VALUES (?,?,?,?)",
                    (key, text, now, now),
                )
                result = EnqueueResult(int(cur.lastrowid or 0), True)
            self._conn.execute("COMMIT")
            return result
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def claim(self, *, lease_seconds: float) -> Claim | None:
        lease = _finite_nonneg("lease_seconds", lease_seconds)
        if lease <= 0 or lease > MAX_LEASE_SECONDS:
            raise ValueError(f"lease_seconds must be in (0, {MAX_LEASE_SECONDS}], got {lease!r}")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            expires = now + lease
            if not math.isfinite(expires) or expires <= now:
                raise ValueError("lease_seconds does not produce a future lease expiry")
            self._reopen_expired(now)
            self._conn.execute(
                "UPDATE outbox SET status = 'dead', last_error = 'attempt limit reached' "
                "WHERE status = 'pending' AND attempts >= ?",
                (self._max_attempts,),
            )
            row = self._conn.execute(
                "SELECT item_id, key, payload, attempts FROM outbox "
                "WHERE status = 'pending' AND available_at <= ? "
                "ORDER BY available_at, item_id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            receipt = secrets.token_hex(24)
            attempts = int(row[3]) + 1
            self._conn.execute(
                "UPDATE outbox SET status = 'claimed', attempts = ?, receipt = ?, "
                "lease_expires_at = ? WHERE item_id = ?",
                (attempts, receipt, expires, row[0]),
            )
            self._conn.execute("COMMIT")
            return Claim(int(row[0]), str(row[1]), json.loads(row[2]), attempts, receipt, expires)
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def _reopen_expired(self, now: float) -> int:
        """Return abandoned (lease-expired) claims to the queue. Caller holds the write lock."""
        dead = self._conn.execute(
            "UPDATE outbox SET status = 'dead', receipt = NULL, lease_expires_at = NULL, "
            "last_error = 'lease expired' "
            "WHERE status = 'claimed' AND lease_expires_at <= ? AND attempts >= ?",
            (now, self._max_attempts),
        )
        cur = self._conn.execute(
            "UPDATE outbox SET status = 'pending', available_at = lease_expires_at, "
            "receipt = NULL, lease_expires_at = NULL, last_error = 'lease expired' "
            "WHERE status = 'claimed' AND lease_expires_at <= ?",
            (now,),
        )
        return dead.rowcount + cur.rowcount

    def recover_expired(self) -> int:
        """Eagerly reopen abandoned claims; claim() does this implicitly. Returns the count."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            n = self._reopen_expired(now)
            self._conn.execute("COMMIT")
            return n
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def ack(self, receipt: str) -> None:
        _nonempty_str("receipt", receipt)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            cur = self._conn.execute(
                "UPDATE outbox SET status = 'done', receipt = NULL, lease_expires_at = NULL "
                "WHERE receipt = ? AND status = 'claimed' AND lease_expires_at > ?",
                (receipt, now),
            )
            self._conn.execute("COMMIT")
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise
        if cur.rowcount != 1:
            raise StaleReceiptError("receipt is unknown, settled, superseded, or expired")

    def _backoff(self, attempts: int) -> float:
        delay = self._base_delay
        for _ in range(attempts - 1):
            if delay >= self._max_delay or delay == 0:
                break
            delay *= 2
        return min(delay, self._max_delay)

    def nack(self, receipt: str, *, error: str | None = None) -> NackOutcome:
        _nonempty_str("receipt", receipt)
        if error is not None and not isinstance(error, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError("error must be a string or None")
        if error is not None:
            error = error.encode("utf-8", errors="replace").decode("utf-8")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            row = self._conn.execute(
                "SELECT item_id, attempts FROM outbox "
                "WHERE receipt = ? AND status = 'claimed' AND lease_expires_at > ?",
                (receipt, now),
            ).fetchone()
            if row is None:
                raise StaleReceiptError("receipt is unknown, settled, superseded, or expired")
            if int(row[1]) >= self._max_attempts:
                self._conn.execute(
                    "UPDATE outbox SET status = 'dead', receipt = NULL, lease_expires_at = NULL, "
                    "last_error = ? WHERE item_id = ?",
                    (error, row[0]),
                )
                outcome = NackOutcome(True, None)
            else:
                retry_at = now + self._backoff(int(row[1]))
                self._conn.execute(
                    "UPDATE outbox SET status = 'pending', available_at = ?, receipt = NULL, "
                    "lease_expires_at = NULL, last_error = ? WHERE item_id = ?",
                    (retry_at, error, row[0]),
                )
                outcome = NackOutcome(False, retry_at)
            self._conn.execute("COMMIT")
            return outcome
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def dead_letters(self) -> list[Item]:
        rows = self._conn.execute(
            "SELECT item_id, key, payload, status, attempts, last_error FROM outbox "
            "WHERE status = 'dead' ORDER BY item_id"
        ).fetchall()
        return [Item(int(r[0]), str(r[1]), json.loads(r[2]), r[3], int(r[4]), r[5]) for r in rows]

    def get(self, key: str) -> Item | None:
        row = self._conn.execute(
            "SELECT item_id, key, payload, status, attempts, last_error FROM outbox WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return Item(int(row[0]), str(row[1]), json.loads(row[2]), row[3], int(row[4]), row[5])

    def purge(
        self,
        *,
        older_than_seconds: float,
        statuses: Iterable[str],
        dry_run: bool = True,
    ) -> int:
        """Delete (or, by default, only count) ``done``/``dead`` rows created long ago.

        A row qualifies when ``created_at < now - older_than_seconds`` and its status is in
        ``statuses``. Only ``done`` and ``dead`` may be selected; ``pending`` and ``claimed``
        rows are never touched. Age is the item's age (``created_at``), not time since it
        finished. A purged key is forgotten, so enqueueing it again creates a new row.
        Returns the number of matching rows (deleted unless ``dry_run``).
        """
        age = _finite_nonneg("older_than_seconds", older_than_seconds)
        if isinstance(statuses, str):
            raise ValueError("statuses must be an iterable of 'done'/'dead', not a string")
        chosen = tuple(sorted(set(statuses)))
        if not chosen or not set(chosen) <= _PURGEABLE:
            raise ValueError(f"statuses must be a non-empty subset of {sorted(_PURGEABLE)}")
        marks = ",".join("?" * len(chosen))
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cutoff = self._now() - age
            where = f"status IN ({marks}) AND created_at < ?"
            params = (*chosen, cutoff)
            count = int(
                self._conn.execute(f"SELECT COUNT(*) FROM outbox WHERE {where}", params).fetchone()[
                    0
                ]
            )
            if not dry_run and count:
                self._conn.execute(f"DELETE FROM outbox WHERE {where}", params)
            self._conn.execute("COMMIT")
            return count
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def stats(self) -> dict[str, int]:
        counts = {"pending": 0, "claimed": 0, "done": 0, "dead": 0}
        for status, n in self._conn.execute("SELECT status, COUNT(*) FROM outbox GROUP BY status"):
            counts[str(status)] = int(n)
        return counts
