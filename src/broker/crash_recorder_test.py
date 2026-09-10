"""Exercise prepared fatal recording and native signal ownership in subprocesses."""

from __future__ import annotations

import os
import resource
import signal
import struct
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


def crc32c(data: bytes) -> int:
    value = 0xFFFFFFFF
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value ^ 0xFFFFFFFF


class CrashRecorderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        cls.binary = runfile("src/broker/crash_recorder_probe")

    def run_probe(self, scenario: str, shards: int = 1):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    self.binary,
                    f"--scenario={scenario}",
                    f"--directory={directory}",
                    "--reactor-backend=epoll",
                    f"--smp={shards}",
                    f"--memory={96 * shards}MiB",
                    "--overprovisioned",
                ],
                env={**os.environ, "HOME": directory},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=60,
                check=False,
            )
            reports = [path.read_bytes() for path in Path(directory).glob("crash_reports/*.crash")]
        return result, reports

    def check_report(self, encoded: bytes, kind: int, signo: int) -> None:
        self.assertGreaterEqual(len(encoded), 48)
        self.assertLessEqual(len(encoded), 9216)
        self.assertEqual(encoded[:8], b"KQCRSH2\0")
        timestamp, actual_signal, shard, actual_kind, message, stack, version, architecture = struct.unpack_from("<QIIIIIII", encoded, 8)
        self.assertGreater(timestamp, 0)
        self.assertEqual(actual_signal, signo)
        self.assertEqual(actual_kind, kind)
        self.assertLess(shard, 2)
        self.assertLessEqual(message, 4096)
        self.assertLessEqual(stack, 4096)
        self.assertLessEqual(version, 128)
        self.assertGreater(architecture, 0)
        self.assertLessEqual(architecture, 16)
        self.assertEqual(len(encoded), 48 + message + stack + version + architecture)
        self.assertEqual(encoded[-4 - architecture:-4].decode(), os.uname().machine)
        self.assertEqual(struct.unpack_from("<I", encoded, len(encoded) - 4)[0], crc32c(encoded[:-4]))
        self.assertNotIn(b"secret-token-never-record", encoded)

    def test_signal_profiles_and_native_termination(self) -> None:
        for scenario, signo, kind in (("abrt", signal.SIGABRT, 2), ("ill", signal.SIGILL, 3), ("segv", signal.SIGSEGV, 4)):
            with self.subTest(signal=scenario):
                result, reports = self.run_probe(scenario)
                if scenario == "segv" and "asan=true" in result.stdout:
                    # The sanitizer retains SIGSEGV ownership and may terminate
                    # through SIGABRT, which the recorder still observes.
                    self.assertIn(result.returncode, (-signal.SIGABRT, -signal.SIGSEGV), result.stdout)
                    completed = [report for report in reports if report]
                    if completed:
                        self.assertEqual(len(completed), 1)
                        self.check_report(completed[0], 2, signal.SIGABRT)
                    self.assertIn("AddressSanitizer", result.stdout)
                else:
                    self.assertEqual(result.returncode, -signo, result.stdout)
                    self.assertEqual(len(reports), 1, result.stdout)
                    self.check_report(reports[0], kind, signo)

    def test_prior_handler_runs_after_recording(self) -> None:
        result, reports = self.run_probe("prior")
        self.assertEqual(result.returncode, 73, result.stdout)
        self.assertIn("prior fatal handler", result.stdout)
        self.assertEqual(len(reports), 1)
        self.check_report(reports[0], 3, signal.SIGILL)

    def test_simultaneous_shard_signals_keep_one_complete_report(self) -> None:
        result, reports = self.run_probe("concurrent", shards=2)
        self.assertIn(result.returncode, (-signal.SIGABRT, -signal.SIGILL), result.stdout)
        self.assertEqual(len(reports), 1, result.stdout)
        signo = struct.unpack_from("<I", reports[0], 16)[0]
        self.check_report(reports[0], 2 if signo == signal.SIGABRT else 3, signo)
        self.assertEqual(result.stdout.count("Recorded crash report"), 1)

    def test_preparation_failure_and_clean_stop_leave_no_report(self) -> None:
        result, reports = self.run_probe("preinit")
        self.assertEqual(result.returncode, -signal.SIGABRT, result.stdout)
        self.assertEqual(reports, [])
        result, reports = self.run_probe("clean")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(reports, [])

    def test_prepared_fatal_write_with_allocation_injection(self) -> None:
        result, reports = self.run_probe("allocation")
        if result.returncode == 77:
            self.assertIn("injection=false", result.stdout)
            self.assertNotEqual(
                os.environ.get("KWAQUE_EXPECT_TEST_INJECTION"), "true"
            )
            self.assertEqual(reports, [])
            self.skipTest("allocation injection is disabled in this verified profile")
        self.assertEqual(result.returncode, -signal.SIGABRT, result.stdout)
        self.assertEqual(len(reports), 1, result.stdout)
        self.check_report(reports[0], 2, signal.SIGABRT)

    def test_startup_failure_report_is_secret_safe(self) -> None:
        result, reports = self.run_probe("startup")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual(len(reports), 1, result.stdout)
        self.check_report(reports[0], 1, 0)
        self.assertIn(b"Failure during startup", reports[0])

    def test_invariant_and_real_native_oom_record_before_termination(self) -> None:
        for scenario in ("invariant", "oom"):
            with self.subTest(scenario=scenario):
                result, reports = self.run_probe(scenario)
                if scenario == "oom" and "allocator=system" in result.stdout:
                    self.assertEqual(result.returncode, 77, result.stdout)
                    self.assertEqual(reports, [])
                    self.skipTest(
                        "real native heap exhaustion is unavailable with the system allocator"
                    )
                else:
                    self.assertEqual(result.returncode, -signal.SIGABRT, result.stdout)
                    self.assertEqual(len(reports), 1, result.stdout)
                    self.check_report(reports[0], 2, signal.SIGABRT)


if __name__ == "__main__":
    unittest.main()
