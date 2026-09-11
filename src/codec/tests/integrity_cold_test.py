"""Qualify isolated integrity initialization and retain scoped native observations."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import resource
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ISOLATED_FAILURE_OBSERVED = 86
MEMORY_FIELDS = {
    "mallocs",
    "frees",
    "requested_bytes",
    "page_bytes_before",
    "page_bytes_after",
    "large_allocation_warnings",
    "foreign_allocations",
    "fallback_allocations",
}


def parse_measurement(output: str) -> dict[str, str | int | bool]:
    lines = [line for line in output.splitlines() if line.startswith("measurement ")]
    if len(lines) != 1:
        raise ValueError("expected one cold measurement")
    tokens = [token.split("=", 1) for token in lines[0].split()[1:]]
    if any(len(token) != 2 for token in tokens):
        raise ValueError("invalid cold measurement field")
    fields = dict(tokens)
    if len(fields) != len(tokens):
        raise ValueError("duplicate cold measurement field")
    observed = fields.get("memory_observed")
    if observed not in {"true", "false"}:
        raise ValueError("missing cold allocator observation status")
    expected = {"backend", "bytes", "nanoseconds", "memory_observed"}
    if observed == "true":
        expected |= MEMORY_FIELDS
    if set(fields) != expected:
        raise ValueError("unexpected or missing cold measurement fields")
    if fields["backend"] not in {
        "google_configured",
        "abseil_selected",
        "openssl_sha256",
    }:
        raise ValueError("unknown measured backend")
    result: dict[str, str | int | bool] = {
        "backend": fields["backend"],
        "memory_observed": observed == "true",
    }
    for key in expected - {"backend", "memory_observed"}:
        if not fields[key].isascii() or not fields[key].isdecimal():
            raise ValueError("cold measurement counters must be nonnegative integers")
        result[key] = int(fields[key])
    return result


class MeasurementParserTest(unittest.TestCase):
    def test_manifest_exists_before_the_first_probe_and_stays_incomplete_on_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            probe = IntegrityColdTest(
                "test_first_use_measurements_use_separate_fresh_processes"
            )
            probe.binary = directory / "fixture.bin"
            probe.binary.write_bytes(b"measurement fixture")
            with (
                mock.patch.dict(os.environ, {"TEST_UNDECLARED_OUTPUTS_DIR": root}),
                mock.patch.object(
                    probe,
                    "capabilities",
                    return_value={"allocator": "native", "abort": "false"},
                ),
                mock.patch.object(
                    probe, "actual_probe", side_effect=OSError("fixture failure")
                ),
                self.assertRaises(OSError),
            ):
                probe.test_first_use_measurements_use_separate_fresh_processes()
            recorded = json.loads(
                (directory / "integrity-cold-measurements.json").read_text()
            )
            self.assertFalse(recorded["complete"])
            self.assertEqual(recorded["profile"]["abort"], "true")
            self.assertEqual(recorded["measurements"], [])
            self.assertEqual(recorded["expected_measurements"], 9)
            self.assertEqual(
                recorded["binary_sha256"],
                hashlib.sha256(b"measurement fixture").hexdigest(),
            )

    def test_fresh_probe_records_and_logs_are_retained(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            probe = IntegrityColdTest(
                "test_first_use_measurements_use_separate_fresh_processes"
            )
            probe.binary = directory / "fixture.bin"
            probe.binary.write_bytes(b"measurement fixture")
            invoked = []

            def sample(
                scenario: str, capabilities: dict[str, str]
            ) -> subprocess.CompletedProcess[str]:
                recorded = json.loads(
                    (directory / "integrity-cold-measurements.json").read_text()
                )
                self.assertFalse(recorded["complete"])
                self.assertEqual(len(recorded["measurements"]), len(invoked))
                self.assertEqual(capabilities, {"allocator": "system"})
                invoked.append(scenario)
                if scenario == "measure-sha":
                    backend, length = "openssl_sha256", "3"
                else:
                    _, name, length = scenario.split("-")
                    backend = {
                        "google": "google_configured",
                        "abseil": "abseil_selected",
                    }[name]
                return subprocess.CompletedProcess(
                    [],
                    0,
                    stdout=f"measurement backend={backend} bytes={length} nanoseconds=12 memory_observed=false\n",
                )

            with (
                mock.patch.dict(os.environ, {"TEST_UNDECLARED_OUTPUTS_DIR": root}),
                mock.patch.object(
                    probe, "capabilities", return_value={"allocator": "system"}
                ),
                mock.patch.object(probe, "actual_probe", side_effect=sample),
            ):
                probe.test_first_use_measurements_use_separate_fresh_processes()
            recorded = json.loads(
                (directory / "integrity-cold-measurements.json").read_text()
            )
            self.assertTrue(recorded["complete"])
            self.assertEqual(len(set(invoked)), 9)
            self.assertEqual(len(recorded["measurements"]), 9)
            for scenario in invoked:
                self.assertTrue((directory / f"{scenario}.log").is_file())

    def test_system_allocator_does_not_report_unobserved_memory_as_zero(self) -> None:
        result = parse_measurement(
            "measurement backend=openssl_sha256 bytes=3 nanoseconds=42 memory_observed=false"
        )
        self.assertFalse(result["memory_observed"])
        self.assertNotIn("requested_bytes", result)

    def test_native_observation_requires_the_complete_counter_set(self) -> None:
        line = "measurement backend=abseil_selected bytes=65 nanoseconds=42 memory_observed=true"
        complete = line + " " + " ".join(f"{key}=0" for key in sorted(MEMORY_FIELDS))
        self.assertTrue(parse_measurement(complete)["memory_observed"])
        for key in MEMORY_FIELDS:
            with self.subTest(key=key), self.assertRaises(ValueError):
                parse_measurement(complete.replace(f" {key}=0", ""))

    def test_malformed_or_duplicate_observations_reject(self) -> None:
        line = "measurement backend=google_configured bytes=32 nanoseconds=10 memory_observed=false"
        for invalid in (
            "",
            line + "\n" + line,
            line + " bytes=32",
            line + " extra=0",
            line.replace("nanoseconds=10", "nanoseconds=-1"),
            line.replace("nanoseconds=10", "nanoseconds=nan"),
            line.replace("memory_observed=false", "memory_observed=unknown"),
            line.replace("google_configured", "unknown"),
        ):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                parse_measurement(invalid)


def runfile(path: str) -> Path:
    if root := os.environ.get("RUNFILES_DIR"):
        candidate = Path(root) / "_main" / path
        if candidate.exists():
            return candidate
    return Path(path).resolve()


class IntegrityColdTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        cls.binary = runfile("src/codec/tests/integrity_cold_probe")

    def run_probe(
        self, scenario: str, *options: str
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            return subprocess.run(
                [
                    self.binary,
                    f"--scenario={scenario}",
                    "--reactor-backend=epoll",
                    "--smp=1",
                    "--memory=64MiB",
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

    def profile(self, result: subprocess.CompletedProcess[str]) -> dict[str, str]:
        lines = [
            line for line in result.stdout.splitlines() if line.startswith("allocator=")
        ]
        self.assertEqual(len(lines), 1, result.stdout)
        fields = dict(field.split("=", 1) for field in lines[0].split())
        self.assertIn(fields["allocator"], ("native", "system"))
        for field in ("abort", "injection", "optimized", "sanitized", "asan", "ubsan"):
            self.assertIn(fields[field], ("true", "false"))
        if fields["allocator"] == "system":
            self.assertEqual(fields["injection"], "false")
        return fields

    def capabilities(self) -> dict[str, str]:
        result = self.run_probe("capabilities")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("phase=capabilities status=ok", result.stdout)
        fields = self.profile(result)
        expectations = {
            "allocator": "KWAQUE_EXPECT_TEST_ALLOCATOR",
            "injection": "KWAQUE_EXPECT_TEST_INJECTION",
            "optimized": "KWAQUE_EXPECT_TEST_OPTIMIZED",
            "sanitized": "KWAQUE_EXPECT_TEST_SANITIZED",
        }
        supplied = [name for name in expectations.values() if name in os.environ]
        self.assertIn(
            len(supplied),
            (0, len(expectations)),
            "specify all four validation profile expectations",
        )
        for field, name in expectations.items():
            if name in os.environ:
                self.assertEqual(fields[field], os.environ[name], name)
        if os.environ.get("KWAQUE_EXPECT_TEST_SANITIZED") == "true":
            self.assertEqual(fields["asan"], "true", "ASan instrumentation is required")
            self.assertEqual(
                fields["ubsan"], "true", "UBSan instrumentation is required"
            )
        return fields

    def actual_probe(
        self, scenario: str, capabilities: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        native = capabilities["allocator"] == "native"
        options = ("--abort-on-seastar-bad-alloc",) if native else ()
        result = self.run_probe(scenario, *options)
        profile = self.profile(result)
        self.assertEqual(profile["abort"], "true" if native else "false", result.stdout)
        for field in (
            "allocator",
            "injection",
            "optimized",
            "sanitized",
            "asan",
            "ubsan",
        ):
            self.assertEqual(profile[field], capabilities[field], result.stdout)
        return result

    def check_cold_crc(self, length: int) -> None:
        result = self.actual_probe(f"crc-{length}", self.capabilities())
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn(f"phase=cold-crc bytes={length} status=ok", result.stdout)

    def test_capabilities(self) -> None:
        self.capabilities()

    def test_cold_crc_32_bytes(self) -> None:
        self.check_cold_crc(32)

    def test_cold_crc_48_bytes(self) -> None:
        self.check_cold_crc(48)

    def test_cold_crc_65_bytes(self) -> None:
        self.check_cold_crc(65)

    def test_cold_crc_4096_bytes(self) -> None:
        self.check_cold_crc(4096)

    def test_cold_sha_abc(self) -> None:
        result = self.actual_probe("sha-abc", self.capabilities())
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("phase=cold-sha input=abc status=ok", result.stdout)

    def test_first_use_measurements_use_separate_fresh_processes(self) -> None:
        capabilities = self.capabilities()
        cases = [
            (f"measure-{name}-{length}", backend, length)
            for name, backend in (
                ("google", "google_configured"),
                ("abseil", "abseil_selected"),
            )
            for length in (32, 48, 65, 4096)
        ] + [("measure-sha", "openssl_sha256", 3)]
        records = []
        output_root = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
        directory = Path(output_root) if output_root else None
        if directory is not None:
            directory.mkdir(parents=True, exist_ok=True)
        with self.binary.open("rb") as binary:
            digest = hashlib.file_digest(binary, "sha256").hexdigest()
        # The capabilities subprocess has no OOM override. actual_probe checks
        # this effective policy separately in every measured subprocess.
        measurement_profile = {
            **capabilities,
            "abort": "true" if capabilities["allocator"] == "native" else "false",
        }

        def write_measurements() -> None:
            if directory is not None:
                (directory / "integrity-cold-measurements.json").write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "binary_sha256": digest,
                            "profile": measurement_profile,
                            "architecture": platform.machine(),
                            "expected_measurements": len(cases),
                            "complete": len(records) == len(cases),
                            "scope": "one native operation per fresh process; clock warmed, target engine not warmed",
                            "memory_scope": "reactor-local requested-byte totals and page occupancy; excludes alien threads and is not peak live capacity",
                            "measurements": records,
                        },
                        indent=2,
                    )
                    + "\n"
                )

        write_measurements()
        for scenario, backend, length in cases:
            with self.subTest(scenario=scenario):
                result = self.actual_probe(scenario, capabilities)
                if directory is not None:
                    (directory / f"{scenario}.log").write_text(result.stdout)
                self.assertEqual(result.returncode, 0, result.stdout)
                measured = parse_measurement(result.stdout)
                self.assertEqual(measured["backend"], backend)
                self.assertEqual(measured["bytes"], length)
                self.assertEqual(
                    measured["memory_observed"], capabilities["allocator"] == "native"
                )
                if measured["memory_observed"]:
                    for key in (
                        "large_allocation_warnings",
                        "foreign_allocations",
                        "fallback_allocations",
                    ):
                        self.assertEqual(measured[key], 0, key)
                records.append({"scenario": scenario, **measured})
                write_measurements()

    def test_warm_crc_update_does_not_allocate(self) -> None:
        capabilities = self.capabilities()
        result = self.actual_probe("crc-warm", capabilities)
        injection = capabilities["injection"] == "true"
        self.assertEqual(result.returncode, 0 if injection else 77, result.stdout)
        if not injection:
            self.assertIn(
                "phase=warm-crc status=skipped injection=false", result.stdout
            )
            self.skipTest("allocation injection is disabled in this verified profile")
        self.assertIn(
            "phase=warm-crc bytes=4096 allocation_free=true status=ok", result.stdout
        )

    def test_cold_crc_failures_are_observed_in_isolation(self) -> None:
        capabilities = self.capabilities()
        injection = capabilities["injection"] == "true"
        for ordinal in (0, 1):
            with self.subTest(ordinal=ordinal):
                result = self.actual_probe(f"crc-inject-{ordinal}", capabilities)
                self.assertEqual(
                    result.returncode,
                    ISOLATED_FAILURE_OBSERVED if injection else 77,
                    result.stdout,
                )
                if not injection:
                    self.assertIn(
                        f"phase=cold-crc-injection ordinal={ordinal} "
                        "status=skipped injection=false",
                        result.stdout,
                    )
                else:
                    self.assertIn(
                        f"phase=cold-crc-injection ordinal={ordinal} "
                        "caught_bad_alloc=true injected=true seed_unchanged=true "
                        "abort=true exit=isolated status=ok",
                        result.stdout,
                    )
        if not injection:
            self.skipTest("allocation injection is disabled in this verified profile")


if __name__ == "__main__":
    unittest.main()
