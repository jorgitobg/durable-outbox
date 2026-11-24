# ADR 0001: explicit, opt-in purge of terminal rows

Status: accepted.

## Context

`done` and `dead` rows are kept forever: `done` rows make later duplicate enqueues dedupe, `dead`
rows are kept for inspection. A long-lived database therefore only grows. The schema has no
completion timestamp (only `created_at` and `available_at`), and `_validate_format` rejects any
schema drift, so adding a column would be a format change with no migration path.

## Options

1. Add a `settled_at` column and purge by completion time. Rejected: changes the database
   format (version bump, existing files would be rejected), which this change should not force.
2. Purge by `created_at` with an explicit cutoff, no schema change. Chosen.

## Decision

`Outbox.purge(older_than_seconds, statuses, dry_run=True)` and CLI `purge`:

* Only `done` and `dead` can be selected. `pending` and `claimed` are rejected with `ValueError`
  and never touched, whatever the arguments.
* `older_than_seconds` and `statuses` are required; there is no default that deletes anything,
  and `dry_run` defaults to `True`. The CLI needs `--execute` to delete.
* A row qualifies when `created_at < now - older_than_seconds` (strict) and its status is selected.
  The cutoff uses the injected clock (the real clock in the CLI).
* One `BEGIN IMMEDIATE` transaction counts and deletes, so concurrent workers on their own
  connections either see a row before the purge or not at all.
* Returns the number of matching rows (dry run) or deleted rows.
* Purge is never automatic or background. It never creates a missing database.

## Consequences

* `created_at` is the age of the *item*, not of its completion: an item enqueued long ago and
  finished a minute ago qualifies. Pick a cutoff comfortably above your longest expected
  lifetime (lease, retries, backoff).
* A purged key is forgotten: enqueueing it again creates a new `pending` row and the work is
  done again. Purge `done` rows only older than any window in which a duplicate enqueue is
  still possible.
* `item_id` values are never reused (`AUTOINCREMENT`).
