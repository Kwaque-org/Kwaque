"""Run a store natively, kill it, and check what it left against its record."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

# The test macros export the reactor backend selected for this build.
REACTOR_BACKEND = os.environ.get("KWAQUE_REACTOR_BACKEND", "epoll")
HOLDS = "kwaque-durability-verdict holds"
LOST = "kwaque-durability-verdict lost"


def runfile(path: str) -> Path:
    if root := os.environ.get("RUNFILES_DIR"):
        candidate = Path(root) / "_main" / path
        if candidate.exists():
            return candidate
    return Path(path).resolve()


def marks_of(record: Path) -> int:
    if not record.exists():
        return 0
    return sum(
        1 for line in record.read_text().splitlines() if line.startswith("mark ")
    )


class EngineDurabilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.binary = runfile("src/broker/engine_durability_probe")

    def setUp(self) -> None:
        # A store needs a file system that takes Direct I/O.
        self.directory = tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR"))
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "store"
        self.record = Path(self.directory.name) / "record"

    def arguments(self, *probe: str) -> list[str]:
        return [
            str(self.binary),
            *probe,
            f"--root={self.root}",
            f"--record={self.record}",
            f"--reactor-backend={REACTOR_BACKEND}",
            "--max-networking-io-control-blocks=1000",
            "--smp=1",
            "--memory=256MiB",
            "--overprovisioned",
        ]

    def probe(self, *probe: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.arguments(*probe),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=600,
            check=False,
        )

    def ran(self, history: str) -> None:
        done = self.probe("--mode=run", f"--history={history}")
        self.assertEqual(done.returncode, 0, done.stdout)
        self.assertIn(HOLDS, done.stdout)
        self.assertIn("kwaque-durability-profile ", done.stdout)

    def test_a_store_is_checked_from_its_record(self) -> None:
        self.ran("short")
        marks = marks_of(self.record)
        # Two creations, four requests, a seal and the stop.
        self.assertEqual(marks, 8)
        whole = self.probe("--mode=check")
        self.assertEqual(whole.returncode, 0, whole.stdout)
        self.assertIn(HOLDS, whole.stdout)
        # The answers after the seal, the fifth acknowledgement, withheld.
        part = self.probe("--mode=check", "--answered=5", f"--asked={marks}")
        self.assertEqual(part.returncode, 0, part.stdout)
        self.assertIn(HOLDS, part.stdout)
        none = self.probe("--mode=check", "--answered=9", "--asked=9")
        self.assertEqual(none.returncode, 3, none.stdout)

    def test_a_request_the_store_never_took_is_reported_lost(self) -> None:
        self.ran("short")
        offered = sum(
            1
            for line in self.record.read_text().splitlines()
            if line.startswith("offer ")
        )
        self.assertEqual(offered, 4)
        # A fifth request, reported durable far past where the second
        # segment's file ends.
        with self.record.open("a") as record:
            record.write("offer 2 199 a5\n")
            record.write(f"accepted {offered}\n")
            record.write(f"durable {offered} 1099511627776 a5a5a5a5\n")
        lost = self.probe("--mode=check")
        self.assertEqual(lost.returncode, 2, lost.stdout)
        self.assertIn(LOST, lost.stdout)

    def test_a_whole_life_is_checked_from_its_record(self) -> None:
        self.ran("life")
        whole = self.probe("--mode=check")
        self.assertEqual(whole.returncode, 0, whole.stdout)
        self.assertIn(HOLDS, whole.stdout)

    def test_a_killed_store_is_checked_from_its_record(self) -> None:
        for wanted in (3, 40, 120):
            with self.subTest(killed_at=wanted):
                self.root = Path(self.directory.name) / f"store-{wanted}"
                self.record = Path(self.directory.name) / f"record-{wanted}"
                with subprocess.Popen(
                    self.arguments("--mode=run", "--history=long"),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ) as running:
                    deadline = time.monotonic() + 600
                    while marks_of(self.record) < wanted and running.poll() is None:
                        self.assertLess(time.monotonic(), deadline, "the store hangs")
                        time.sleep(0.002)
                    finished = running.poll() is not None
                    if not finished:
                        running.send_signal(signal.SIGKILL)
                    running.wait()
                if finished:
                    self.assertEqual(running.returncode, 0)
                self.assertGreaterEqual(marks_of(self.record), 1)
                checked = self.probe("--mode=check")
                self.assertEqual(checked.returncode, 0, checked.stdout)
                self.assertIn(HOLDS, checked.stdout)


if __name__ == "__main__":
    unittest.main()
