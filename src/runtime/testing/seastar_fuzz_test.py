"""Exercise native bridge startup, execution and shutdown in isolated processes."""

from __future__ import annotations

import os
import resource
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def disable_core_dumps() -> None:
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


class SeastarFuzzBridgeTest(unittest.TestCase):
    def run_canary(self, mode: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(
            prefix="fuzz-bridge-", dir=os.environ["TEST_TMPDIR"]
        ) as directory:
            environment = dict(os.environ)
            environment["TMPDIR"] = directory
            environment["LLVM_PROFILE_FILE"] = str(
                Path(directory) / "canary-%p.profraw"
            )
            return subprocess.run(
                [str(CANARY), mode],
                cwd=directory,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
                check=False,
                preexec_fn=disable_core_dumps,
            )

    def test_normal_inputs_join_and_finalize(self) -> None:
        result = self.run_canary("normal")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stderr.count("CANARY input completed"), 2)

    def test_body_exception_propagates_and_next_input_runs(self) -> None:
        result = self.run_canary("body_exception")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stderr.count("CANARY input completed"), 1)

    def assert_skipped_input_fails(self, mode: str) -> None:
        result = self.run_canary(mode)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("reactor did not execute the input", result.stderr)
        self.assertNotIn("CANARY input completed", result.stderr)
        self.assertNotIn("CANARY incorrectly accepted", result.stderr)

    def test_native_startup_failure_is_not_a_successful_input(self) -> None:
        self.assert_skipped_input_fails("startup_failure")

    def test_native_skipped_callback_is_not_a_successful_input(self) -> None:
        self.assert_skipped_input_fails("skipped_callback")

    def test_native_finalization_error_overrides_successful_main(self) -> None:
        result = self.run_canary("finalize_failure")
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn("CANARY input completed", result.stderr)
        self.assertIn("CANARY main returned success", result.stderr)
        self.assertIn("fuzz reactor shutdown failed: 3", result.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("expected the native bridge canary path")
    CANARY = Path(sys.argv[1]).resolve(strict=True)
    unittest.main(argv=[sys.argv[0]])
