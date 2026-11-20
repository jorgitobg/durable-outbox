from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from durable_outbox.demo import run_demo


def test_demo_is_deterministic_and_covers_key_scenarios() -> None:
    first = run_demo()
    second = run_demo()
    assert first == second
    text = "\n".join(first)
    assert "SYNTHETIC DEMO" in text
    assert "duplicate enqueue reused item" in text
    assert "conflicting payload rejected" in text
    assert "stale receipt rejected" in text
    assert "dead-lettered" in text
    assert "duplicate delivery" in text  # at-least-once is shown honestly


def test_demo_module_runs_as_script() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "durable_outbox.demo"],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent.parent / "src")},
    )
    assert "SYNTHETIC DEMO" in proc.stdout


def test_demo_final_state_is_exact() -> None:
    lines = run_demo()
    assert lines[-1] == "final stats: {'pending': 0, 'claimed': 0, 'done': 5, 'dead': 1}"
    assert lines[-2] == "   dead letters: ['invoice-006']"
