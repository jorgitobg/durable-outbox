# Semantics

This document is the contract. If the code and this file disagree, that is a bug.

## Data model

One SQLite table, one row per idempotency key. Status is one of:

| status    | meaning                                                         |
|-----------|-----------------------------------------------------------------|
| `pending` | ready once `available_at <= now`                                |
| `claimed` | leased to a worker until `lease_expires_at` (exclusive)         |
| `done`    | acknowledged; row is kept so later duplicate enqueues dedupe (until an explicit purge) |
| `dead`    | `max_attempts` exhausted; kept for inspection, never reclaimed  |

`attempts` counts claims (each successful `claim()` increments it).

## Enqueue and idempotency

* `enqueue(key, payload)` stores the payload as canonical JSON (sorted keys, compact,
  `NaN`/`Infinity` rejected). Non-JSON-serializable payloads raise `ValueError`.
* Same key + same canonical payload: no new row, `created=False`, same `item_id`.
  This holds in every state, including `done` and `dead`.
* Same key + different payload: `IdempotencyConflictError`; the original row is untouched.
* The write is committed (`synchronous=FULL`, WAL) before `enqueue` returns.

## Claim, lease, receipt

* `claim(lease_seconds=...)` runs inside one `BEGIN IMMEDIATE` transaction: it first
  reopens/dead-letters expired leases, then selects the ready item with the smallest
  `(available_at, item_id)` and marks it claimed. The write lock is held from select to
  update, so two connections (threads or processes) cannot claim the same row.
  It reads the clock after taking the write lock, not before waiting for it.
* The lease is finite and bounded: `0 < lease_seconds <= 7 days`. A lease ends exactly at
  `lease_expires_at` (`now >= expires` is expired). If floating-point addition does not
  produce a strictly future, finite expiry, `claim` raises `ValueError` and rolls back
  all changes (including recovery of expired claims).
* Every claim gets a fresh random opaque `receipt` (`secrets.token_hex`, 192 bits). Treat it as an
  opaque string; do not parse it. It is hex on purpose: a URL-safe alphabet can start with `-`,
  and a command-line parser then reads `ack --receipt <token>` as a missing argument.
* Ordering is *ready order*: by the time an item became (or becomes again) available, then by
  insertion. A retried item re-enters at its backoff deadline; an expired lease re-enters at
  the old lease deadline. Ordering is best effort under concurrency and retries, not FIFO.

## Ack / nack

* `ack(receipt)` / `nack(receipt)` succeed only if that exact receipt is the item's *current*
  receipt, the item is `claimed`, and the lease has not expired. Otherwise `StaleReceiptError`.
* A receipt from an earlier claim can therefore never settle a newer claim of the same item,
  even if the item has been re-claimed by the same process.
* `ack` -> `done`.
* `nack` -> `pending` at `now + delay`, or `dead` if `attempts >= max_attempts`.
  `delay = min(base_delay * 2**(attempts-1), max_delay)`. No jitter (deterministic by design).
* A lease that expires without ack/nack (crash, hang) is recovered lazily by the next
  `claim()` (or eagerly by `recover_expired()`). If the item had already used `max_attempts`
  claims it goes to `dead` with `last_error = 'lease expired'`, so a poison item that crashes
  its worker cannot loop forever. Abandoned leases are retried immediately (no backoff).
* Recovery needs no special restart step: state lives in the file, so a restarted process just
  calls `claim()` again.

## Time and numeric validation

* Time comes only from the injected `clock` (default `time.time`). Every reading must be a
  finite number in `[0, 1e12]` epoch seconds or `ValueError` is raised; this upper bound
  leaves room for bounded lease and retry calculations. `durable_outbox.testing.ManualClock` never
  sleeps and only moves when `advance()` is called.
* `lease_seconds`, `base_delay`, `max_delay` must be finite, non-negative numbers (bool and
  non-numbers rejected); `max_delay >= base_delay`; delays are capped at 365 days.
  `lease_seconds` must also be positive and <= 604800 seconds (7 days).
  `max_attempts` is an integer in `[1, 2147483647]`; `busy_timeout_ms` is an integer in
  `[1, 3600000]`. Huge integer inputs that cannot be converted to finite floats raise
  `ValueError`, not `OverflowError`.
* The clock is trusted. A wall clock that jumps backwards can delay readiness or extend a
  lease in effect; use a monotonic-ish source if that matters to you.

## Delivery helper and what is *not* guaranteed

`delivery.deliver_next` does claim -> `transport(payload, key=key)` -> ack/nack. The transport is called
**outside** any database transaction. The library never opens a network connection; tests and
demo use `FakeTransport` (whose `sent` entries are `(key, payload)` tuples). `nack` replaces
unpaired Unicode surrogates in error strings; `deliver_next` also handles exceptions whose
`__str__` raises. Error messages are diagnostic only, not stable API data.

**This is at-least-once delivery, not exactly-once.** The remote side effect and the local ack
are two separate steps:

* send succeeds, process dies before ack -> lease expires -> item is sent again;
* send takes longer than the lease -> ack is rejected (`sent_but_ack_failed`) -> sent again.

Consumers must be idempotent. Use the item `key` as the idempotency key on the remote side.
Choose `lease_seconds` larger than your worst-case send time.

## Database format and retry configuration

`Outbox(path)` creates a new database with format `PRAGMA user_version = 1`, an `outbox`
table/index and an `outbox_config` table. On first creation it persists `max_attempts=5`,
`base_delay=1.0` and `max_delay=300.0` unless explicitly set. Existing databases inherit
all three settings by default; any explicitly conflicting value raises `ValueError` before
changing data. All worker connections and CLI commands therefore use the same retry policy.
No direct claim is made after the stored attempt limit; exhausted pending/expired claims are
dead-lettered instead. Settings are immutable through this API; there is no migration or
config-change command.

A missing database is created atomically: a complete database is built and committed in a
hidden staging file (`.<name>.init-<random>.tmp`) in the same directory, closed, and then
published with `os.link`, which never overwrites. Competing first-time creators therefore
never see a half-initialised file; the loser of the race validates the winner's file like
any existing database (same policy: both succeed; different explicit policy: a clean
`config mismatch` error). On normal return or a caught failure, the staging name is
removed. Forced process termination can leave an inert `.<name>.init-*.tmp` file behind;
no opener treats it as the published database. After confirming that no creator is still
using it, an operator may remove that abandoned staging file. This needs a
local filesystem that supports hard links and write permission in the directory; a path that
exists but is not an outbox database (including an empty file) is rejected, never
initialised in place. WAL mode is switched on only after the published file validates.

Existing files are inspected read-only before any write-capable initialization. Foreign files,
unknown versions and version-zero prototype databases are rejected, never silently migrated
or overwritten. Back up and manually export/re-enqueue old data into a **new** database if
needed; never delete the original to bypass validation. `Outbox(path, create=False)` opens
an existing supported database read-only for `get`, `stats` and `dead_letters`; no missing
file is created. CLI `stats` and `dead` use this mode. This is format validation, not a
promise to parse arbitrary SQLite files or to auto-repair corrupted databases.

## Storage constraints

* SQLite file on a **local filesystem** only. WAL mode (and SQLite's locking generally) is not
  reliable over NFS/SMB or other network filesystems; do not put the database there. WAL also
  requires all connections to be on the same host (shared memory file).
* WAL creates `-wal` and `-shm` sidecar files; back up/copy using SQLite's backup API, not a
  bare file copy of a live database.
* Multi-process safe on one host through SQLite's own locks; writers serialize. If a lock is
  held longer than `busy_timeout_ms`, `sqlite3.OperationalError` ("database is locked")
  propagates; it is not retried further.
* One `Outbox` object owns one connection and is for one thread at a time. Give each thread or
  process its own `Outbox` on the same path.
* The database is **plaintext**. Payloads, keys and error strings are stored unencrypted with
  whatever file permissions your umask gives. No encryption, redaction or secret management is
  provided; do not enqueue secrets you would not write to disk in the clear.
* Rows are never purged automatically. `purge(older_than_seconds=..., statuses=..., dry_run=True)`
  deletes `done`/`dead` rows whose `created_at < now - older_than_seconds`, inside one
  `BEGIN IMMEDIATE` transaction; `dry_run` (the default) only counts. `pending`/`claimed` rows
  cannot be selected. The cutoff is item age, not completion time, and a purged key is forgotten:
  enqueueing it again creates a new pending row. Database format is unchanged. Rationale:
  [adr/0001-purge-terminal-rows.md](adr/0001-purge-terminal-rows.md).
