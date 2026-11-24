"""Small JSON-in/JSON-out command line interface. Uses the real wall clock."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .errors import OutboxError
from .outbox import Outbox


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="durable-outbox", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name)
        p.add_argument("--db", required=True, help="path to the SQLite file (local disk only)")
        return p

    p = add("enqueue")
    p.add_argument("--key", required=True)
    p.add_argument("--payload", required=True, help="JSON document")
    p = add("claim")
    p.add_argument("--lease-seconds", type=float, required=True)
    p.add_argument("--max-attempts", type=int)
    p = add("ack")
    p.add_argument("--receipt", required=True)
    p = add("nack")
    p.add_argument("--receipt", required=True)
    p.add_argument("--error")
    p.add_argument("--max-attempts", type=int)
    add("stats")
    add("dead")
    p = add("recover")
    p.add_argument("--max-attempts", type=int)
    p = add("purge")
    p.add_argument("--older-than-seconds", type=float, required=True)
    p.add_argument("--status", action="append", required=True, help="done or dead; repeat for both")
    p.add_argument("--execute", action="store_true", help="really delete (default: dry run)")
    return parser


def _run(args: argparse.Namespace) -> Any:
    max_attempts: int | None = getattr(args, "max_attempts", None)
    if args.command == "purge" and not Path(args.db).exists():
        raise ValueError(f"outbox database does not exist: {args.db}")
    with Outbox(
        args.db, max_attempts=max_attempts, create=args.command not in {"stats", "dead"}
    ) as box:
        match args.command:
            case "enqueue":
                return dataclasses.asdict(box.enqueue(args.key, json.loads(args.payload)))
            case "claim":
                claim = box.claim(lease_seconds=args.lease_seconds)
                return None if claim is None else dataclasses.asdict(claim)
            case "ack":
                box.ack(args.receipt)
                return {"acked": True}
            case "nack":
                return dataclasses.asdict(box.nack(args.receipt, error=args.error))
            case "stats":
                return box.stats()
            case "dead":
                return [dataclasses.asdict(i) for i in box.dead_letters()]
            case "recover":
                return {"recovered": box.recover_expired()}
            case "purge":
                matched = box.purge(
                    older_than_seconds=args.older_than_seconds,
                    statuses=args.status,
                    dry_run=not args.execute,
                )
                return {"dry_run": not args.execute, "matched": matched}
            case _:
                raise AssertionError(args.command)  # pragma: no cover


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = _run(args)
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON payload: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except OutboxError as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    except sqlite3.Error as exc:
        print(f"error: database: {exc}", file=sys.stderr)
        return 3
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
