"""Exercise owner destruction guards in isolated native reactor processes."""

from __future__ import annotations

import os
import signal
import subprocess
import unittest
from pathlib import Path


class LifecycleInvariantTest(unittest.TestCase):
    def test_destruction_fails_at_the_intended_boundary(self) -> None:
        probe = (
            Path(os.environ["RUNFILES_DIR"])
            / os.environ.get("TEST_WORKSPACE", "_main")
            / "src/runtime/tests/lifecycle_invariant_probe"
        ).resolve(strict=True)
        cases = {
            "scope-foreign": "KQ-WRONG-SHARD-ACCESS",
            "service-foreign": "KQ-WRONG-SHARD-ACCESS",
            "registry-foreign": "KQ-WRONG-SHARD-ACCESS",
            "manager-foreign": "KQ-WRONG-SHARD-ACCESS",
            "service-starting": "KQ-SHARDED-OWNER-STOPPED",
        }
        for scenario, identity in cases.items():
            with self.subTest(scenario=scenario):
                result = subprocess.run(
                    [
                        str(probe),
                        scenario,
                        "--reactor-backend=epoll",
                        "--memory=256MiB",
                        "--smp=2",
                        "--overprovisioned",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
                output = result.stdout + result.stderr
                self.assertEqual(result.returncode, -signal.SIGABRT, output)
                self.assertIn(f"id={identity} ", output)
                self.assertNotIn("ERROR: AddressSanitizer", output)
                self.assertNotIn("runtime error:", output)


if __name__ == "__main__":
    unittest.main()
