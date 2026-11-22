"""The wheel smoke script must catch a broken wheel even when the source tree is visible."""

from __future__ import annotations

import base64
import hashlib
import os
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "wheel_smoke.py"


def _record_line(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def _make_wheel(path: Path) -> None:
    """A minimal wheel that ships the package without ``cli``/``demo`` (deliberately broken)."""
    dist = "durable_outbox-0.0.0.dist-info"
    files = {
        "durable_outbox/__init__.py": b"",
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: durable-outbox\nVersion: 0.0.0\n",
        f"{dist}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        f"{dist}/entry_points.txt": (
            b"[console_scripts]\ndurable-outbox = durable_outbox.cli:main\n"
        ),
    }
    record = [_record_line(n, d) for n, d in files.items()] + [f"{dist}/RECORD,,"]
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
        zf.writestr(f"{dist}/RECORD", "\n".join(record) + "\n")


def test_broken_wheel_fails_even_with_source_on_pythonpath(tmp_path: Path) -> None:
    wheel = tmp_path / "durable_outbox-0.0.0-py3-none-any.whl"
    _make_wheel(wheel)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), str(wheel)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode != 0, proc.stdout + proc.stderr
    assert "wheel smoke FAILED" in proc.stderr
    assert "durable_outbox.demo" in proc.stderr


def test_missing_wheel_argument_is_rejected() -> None:
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "/nonexistent/x.whl"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
