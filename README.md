# durable-outbox

A small, dependency-free (stdlib only, Python >= 3.12) **durable job outbox on SQLite**:
idempotent enqueue, leased claims with opaque receipts, retry with bounded exponential backoff,
dead-lettering, and recovery of abandoned leases. Library plus a tiny JSON CLI.

It is deliberately not a framework: one core class, no scheduler, no network code, no threads.

## What it does

* `enqueue(key, payload)`: durable before return; same key + same payload reuses the item,
  same key + different payload raises `IdempotencyConflictError`.
* `claim(lease_seconds=...)`: atomically takes the next ready item for a finite, bounded lease
  and returns a fresh opaque `receipt`.
* `ack(receipt)` / `nack(receipt)`: bound to the *current* claim; a stale receipt can never
  settle a newer claim (`StaleReceiptError`).
* `nack` retries after `min(base_delay * 2**(attempts-1), max_delay)`, then dead-letters after
  `max_attempts`.
* A worker that dies leaves a lease that expires and is picked up again, including after a
  process restart.
* Time is injected, so tests and the demo are deterministic and never sleep.

```python
from durable_outbox import Outbox

with Outbox("outbox.sqlite3", max_attempts=5, base_delay=1.0, max_delay=60.0) as box:
    box.enqueue("order-42", {"to": "customer", "amount": 10})
    claim = box.claim(lease_seconds=30)
    if claim is not None:
        try:
            send_somewhere(claim.payload, key=claim.key)  # your code; NOT inside a DB transaction
        except Exception as exc:
            box.nack(claim.receipt, error=str(exc))
        else:
            box.ack(claim.receipt)
```

## Limits (read these)

* **At-least-once, not exactly-once.** If the remote send succeeds but the local ack is lost
  (crash, or lease expired during a slow send) the item is sent again. Remote consumers must be
  idempotent; use the item key as the idempotency key.
* **Local filesystem only.** SQLite/WAL is not safe on NFS/SMB; all workers must be on the same
  host. Multiple threads/processes are supported, each with its own `Outbox` connection.
* **Plaintext database.** No encryption, redaction or secret management. Don't store secrets.
* Ordering is by readiness time then insertion; it is not strict FIFO once retries happen.
* Writers serialize on SQLite's write lock; no throughput numbers are claimed or measured.
* No purge/retention of `done`/`dead` rows, no priorities, no jitter, no metrics, no async API.
* The CLI uses the real clock and is a thin convenience; it is not a daemon.
* Retry settings are stored once per database; reopening and CLI commands inherit them. An
  explicit conflicting setting is rejected. `stats`/`dead` are read-only and never create a file.
* GitHub Actions exercises lint, strict typing, tests, the synthetic demo and package builds on
  Python 3.12 and 3.13; see [.github/workflows/ci.yml](.github/workflows/ci.yml).

Full contract: [docs/semantics.md](docs/semantics.md).

## Reproduce locally

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check .
pyright
pytest
python -m durable_outbox.demo
python -m build            # wheel + sdist in dist/
python scripts/wheel_smoke.py dist/*.whl   # install the wheel in a fresh venv, run CLI + demo
```

The demo is synthetic: fixed data, a manual clock, a fake transport, a throwaway temp database,
no sleeps, no network.

CLI (real clock):

```bash
durable-outbox enqueue --db outbox.sqlite3 --key k1 --payload '{"a": 1}'
durable-outbox claim   --db outbox.sqlite3 --lease-seconds 30     # prints a receipt
durable-outbox ack     --db outbox.sqlite3 --receipt <receipt>
durable-outbox stats   --db outbox.sqlite3
```

## Testing approach

Built test-first in vertical slices: persistence/dedupe, claim/reopen, lease expiry and stale
receipts, nack/backoff/max attempts, ordering and multi-connection safety (threads, separate
processes, and a deterministic interleaving test), validation, delivery helper/CLI, demo.

## License

Apache-2.0, see [LICENSE](LICENSE).
