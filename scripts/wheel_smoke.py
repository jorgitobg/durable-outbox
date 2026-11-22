"""Install a built wheel into a fresh venv and exercise it from outside the checkout.

Usage: python scripts/wheel_smoke.py dist/durable_outbox-*.whl

The check runs with PYTHONPATH removed and a temporary working directory, so the source tree
cannot mask a wheel that is missing modules or entry points. All data is synthetic and lives in
a throwaway directory. No network access is needed beyond what pip uses for the venv itself.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def _run(cmd: list[str], *, cwd: Path, env: dict[str, str]) -> str:
    proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}\n{proc.stderr}"
        )
    return proc.stdout


def smoke(wheel: Path) -> None:
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME"}}
    env["PYTHONNOUSERSITE"] = "1"
    with tempfile.TemporaryDirectory(prefix="outbox-wheel-smoke-") as tmp:
        work = Path(tmp)
        env_dir = work / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(env_dir)
        bindir = env_dir / ("Scripts" if os.name == "nt" else "bin")
        py = str(bindir / "python")
        cli = str(bindir / "durable-outbox")
        run_dir = work / "run"
        run_dir.mkdir()
        _run([py, "-m", "pip", "install", "--quiet", str(wheel)], cwd=run_dir, env=env)

        where = _run(
            [py, "-c", "import durable_outbox as m; print(m.__file__)"], cwd=run_dir, env=env
        )
        if "site-packages" not in where:
            raise RuntimeError(f"package not imported from the installed wheel: {where.strip()}")

        demo = _run([py, "-m", "durable_outbox.demo"], cwd=run_dir, env=env)
        if "SYNTHETIC DEMO" not in demo:
            raise RuntimeError(f"unexpected demo output:\n{demo}")

        db = str(run_dir / "smoke.sqlite3")
        enq = json.loads(
            _run(
                [cli, "enqueue", "--db", db, "--key", "k1", "--payload", '{"a": 1}'],
                cwd=run_dir,
                env=env,
            )
        )
        if enq.get("created") is not True:
            raise RuntimeError(f"enqueue did not create an item: {enq}")
        claim = json.loads(
            _run([cli, "claim", "--db", db, "--lease-seconds", "30"], cwd=run_dir, env=env)
        )
        if not claim or "receipt" not in claim:
            raise RuntimeError(f"claim returned no receipt: {claim}")
        ack = json.loads(
            _run([cli, "ack", "--db", db, "--receipt", claim["receipt"]], cwd=run_dir, env=env)
        )
        if ack != {"acked": True}:
            raise RuntimeError(f"unexpected ack result: {ack}")
        stats = json.loads(_run([cli, "stats", "--db", db], cwd=run_dir, env=env))
        if stats.get("done") != 1:
            raise RuntimeError(f"expected one done item, got {stats}")
    print(f"wheel smoke OK: {wheel.name}")


def main(argv: list[str]) -> int:
    if len(argv) != 1 or not Path(argv[0]).is_file():
        print("usage: wheel_smoke.py <path-to-wheel>", file=sys.stderr)
        return 2
    try:
        smoke(Path(argv[0]).resolve())
    except RuntimeError as exc:
        print(f"wheel smoke FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
