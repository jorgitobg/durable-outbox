"""Deterministic helpers for tests and demos (no sleeping, no network)."""

from __future__ import annotations

import math
from typing import Any


def _check(name: str, value: float) -> float:
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be finite and >= 0") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be finite and >= 0, got {value!r}")
    return number


class ManualClock:
    """A clock that only moves when told to. Callable returning epoch-like seconds."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = _check("start", start)

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += _check("seconds", seconds)


class FakeTransport:
    """In-memory stand-in for a remote system. Never touches the network."""

    def __init__(self, fail_times: int = 0) -> None:
        self.sent: list[tuple[str, Any]] = []
        self._remaining_failures = fail_times

    def __call__(self, payload: Any, *, key: str) -> None:
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise RuntimeError("fake transport failure")
        self.sent.append((key, payload))
