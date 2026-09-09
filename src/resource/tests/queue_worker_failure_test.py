"""Check terminal worker failures in isolated native reactor processes."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path


class QueueWorkerFailureTest(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(os.environ["RUNFILES_DIR"])
        workspace = os.environ.get("TEST_WORKSPACE", "_main")
        self.probe = (
            root / workspace / "src/resource/tests/queue_worker_failure_probe"
        ).resolve(strict=True)
        temporary = tempfile.TemporaryDirectory(dir=os.environ["TEST_TMPDIR"])
        self.addCleanup(temporary.cleanup)
        self.environment = {**os.environ, "HOME": temporary.name}

    def run_scenario(self, scenario: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(self.probe),
                scenario,
                "--reactor-backend=epoll",
                "--memory=192MiB",
                "--smp=1",
                "--overprovisioned",
            ],
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )

    def test_unexpected_failures_are_terminal_before_close(self) -> None:
        cases = {
            "scope-unexpected-exit": "KQ-TASK-UNEXPECTED-EXIT",
            "scope-notifier-throw": "KQ-TASK-FAILURE-NOTIFIER",
            "scope-unrequested-abort": "KQ-TASK-UNHANDLED-FAILURE",
            "scope-finite-failure-first": "KQ-TASK-UNHANDLED-FAILURE",
            "queue-unknown-ready": "KQ-QUEUE-WORKER-FAILED",
            "queue-unknown-suspended": "KQ-QUEUE-WORKER-FAILED",
            "queue-exhausted-reports": "KQ-QUEUE-WORKER-FAILED",
            "queue-disabled-reports": "KQ-QUEUE-WORKER-FAILED",
            "queue-reporter-throw": "KQ-QUEUE-FAILURE-REPORTER",
            "queue-classifier-throw": "KQ-QUEUE-FAILURE-CLASSIFIER",
        }
        for scenario, identity in cases.items():
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                diagnostic = result.stdout + result.stderr
                self.assertEqual(result.returncode, -signal.SIGABRT, diagnostic)
                self.assertIn(f"id={identity} ", diagnostic)
                self.assertNotIn("ERROR: AddressSanitizer", diagnostic)
                self.assertNotIn("runtime error:", diagnostic)

    def test_optional_failures_preserve_bounded_reporting_and_cleanup(self) -> None:
        result = self.run_scenario("queue-optional-limited")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
