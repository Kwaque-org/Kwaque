"""The benchmark harness lists its cases and runs one case selected by name."""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

CASE = "build_conventions.empty"
REACTOR_ARGUMENTS = (
    f"--reactor-backend={os.environ.get('KWAQUE_REACTOR_BACKEND', 'epoll')}",
    "--smp=1",
    "--memory=128M",
    "--overprovisioned",
    "--blocked-reactor-notify-ms=2000000",
)


class BenchmarkListTest(unittest.TestCase):
    def test_lists_and_runs_one_named_case(self) -> None:
        binary = Path(sys.argv[1])
        listed = subprocess.run(
            [binary, "--list", *REACTOR_ARGUMENTS],
            capture_output=True,
            check=True,
            text=True,
            timeout=60.0,
        )
        self.assertIn(f"\t{CASE}\n", listed.stdout)

        ran = subprocess.run(
            [
                binary,
                "-t",
                CASE,
                "--duration=0",
                "--iterations=1",
                "--runs=1",
                "--no-perf-counters",
                *REACTOR_ARGUMENTS,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=60.0,
        )
        self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
        self.assertIn(CASE, ran.stdout)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
