"""Verify allocator and invariant policies in bounded native subprocesses."""

from __future__ import annotations

import os
import resource
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path


def runfile(path: str) -> Path:
    if root := os.environ.get("RUNFILES_DIR"):
        candidate = Path(root) / "_main" / path
        if candidate.exists():
            return candidate
    return Path(path).resolve()


class FailurePolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        cls.binary = runfile("src/broker/failure_policy_probe")

    def run_probe(
        self, scenario: str, *options: str, shards: int = 1
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    self.binary,
                    f"--scenario={scenario}",
                    "--reactor-backend=epoll",
                    f"--smp={shards}",
                    f"--memory={64 * shards}MiB",
                    "--overprovisioned",
                    *options,
                ],
                env={**os.environ, "HOME": directory},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=60,
                check=False,
            )
        return result

    def capabilities(self) -> dict[str, str]:
        result = self.run_probe("effective")
        self.assertEqual(result.returncode, 0, result.stdout)
        lines = [
            line for line in result.stdout.splitlines() if line.startswith("allocator=")
        ]
        self.assertEqual(len(lines), 1, result.stdout)
        fields = dict(field.split("=", 1) for field in lines[0].split())
        self.assertIn(fields["allocator"], ("native", "system"))
        self.assertEqual(
            fields["abort"], "true" if fields["allocator"] == "native" else "false"
        )
        self.assertIn(fields["injection"], ("true", "false"))
        return fields

    def test_effective_default_and_presence_option(self) -> None:
        capabilities = self.capabilities()
        explicit = self.run_probe("effective", "--abort-on-seastar-bad-alloc")
        if capabilities["allocator"] == "native":
            self.assertEqual(explicit.returncode, 0, explicit.stdout)
            self.assertIn("abort=true", explicit.stdout)
        else:
            self.assertEqual(explicit.returncode, 2, explicit.stdout)
            self.assertIn("requires the native Seastar allocator", explicit.stdout)
        contradiction = self.run_probe(
            "effective", "--abort-on-seastar-bad-alloc=false"
        )
        self.assertEqual(contradiction.returncode, 2, contradiction.stdout)
        self.assertNotIn("allocator=", contradiction.stdout)

    def test_real_native_heap_exhaustion_aborts(self) -> None:
        native = self.capabilities()["allocator"] == "native"
        result = self.run_probe("oom")
        self.assertEqual(
            result.returncode, -signal.SIGABRT if native else 77, result.stdout
        )
        if native:
            self.assertIn("Failed to allocate", result.stdout)

    def test_injected_failure_remains_an_exception(self) -> None:
        injection = self.capabilities()["injection"] == "true"
        result = self.run_probe("injection")
        self.assertEqual(result.returncode, 0 if injection else 77, result.stdout)

    def test_configured_admission_limits_remain_recoverable(self) -> None:
        result = self.run_probe("admission")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_fatal_diagnostic_survives_armed_allocation_injection(self) -> None:
        result = self.run_probe("invariant")
        self.assertEqual(result.returncode, -signal.SIGABRT, result.stdout)
        self.assertIn("id=KQ-FAILURE-POLICY-PROBE", result.stdout)
        self.assertIn("fatal diagnostic needs no allocation", result.stdout)

    def test_owner_shard_violation_remains_fatal(self) -> None:
        result = self.run_probe("wrong-shard", shards=2)
        self.assertEqual(result.returncode, -signal.SIGABRT, result.stdout)
        self.assertIn("id=KQ-WRONG-SHARD-ACCESS", result.stdout)


if __name__ == "__main__":
    unittest.main()
