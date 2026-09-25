"""Shared observation parser and isolated native qualification driver."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import unittest
from pathlib import Path

# Qualify cold engine initialization without loading host configuration or
# configured providers. Configuration-file I/O can allocate inside libc,
# outside the crypto callbacks and executable linker wrappers.
PROBE_ENVIRONMENT = {"OPENSSL_CONF": ""}

SCENARIOS = (
    "observer-control",
    "crc-cold-32",
    "crc-cold-4096",
    "crc-warm-4096",
    "google-cold-4096",
    "sha-cold",
    "sha-warm",
    "sha-churn",
    "sha-owner",
    "encode-small",
    "encode-max",
    "encode-abort",
    "decode-small",
    "decode-max",
    "decode-invalid",
    "decode-abort",
    "decode-pressure",
    "staging-fragments",
    "collection-8192",
    "collection-pressure",
)
COUNTERS = {
    "mallocs",
    "frees",
    "native_mallocs",
    "native_frees",
    "peak_upper_bound",
    "live_upper_bound",
    "critical_peak_upper_bound",
    "largest_allocation",
}


def fields(line: str) -> dict[str, str]:
    tokens = [token.split("=", 1) for token in line.split()[1:]]
    if any(len(token) != 2 for token in tokens):
        raise ValueError("invalid observation field")
    result = dict(tokens)
    if len(result) != len(tokens):
        raise ValueError("duplicate observation field")
    return result


def observations(output: str, native: bool) -> list[dict[str, str | int | bool]]:
    result = []
    for line in output.splitlines():
        if not line.startswith("observation "):
            continue
        raw = fields(line)
        expected = {
            "scenario",
            "bytes",
            "retained_bound",
            "execution_reservation",
            "observed",
        }
        if native:
            expected |= COUNTERS | {"complete"}
        if set(raw) != expected or raw["observed"] != str(native).lower():
            raise ValueError("observation does not match the allocator profile")
        sample: dict[str, str | int | bool] = {
            "scenario": raw["scenario"],
            "observed": native,
        }
        if native:
            if raw["complete"] != "true":
                raise ValueError("native allocator coverage is incomplete")
            sample["complete"] = True
        for name in expected - {"scenario", "observed", "complete"}:
            if not raw[name].isascii() or not raw[name].isdecimal():
                raise ValueError("allocation measurements must be nonnegative integers")
            sample[name] = int(raw[name])
        if native:
            if (
                sample["mallocs"] != sample["native_mallocs"]
                or sample["frees"] > sample["native_frees"]
            ):
                raise ValueError("wrapped events disagree with native counters")
            if (
                sample["live_upper_bound"] > sample["peak_upper_bound"]
                or sample["critical_peak_upper_bound"] > sample["peak_upper_bound"]
                or sample["largest_allocation"] > sample["peak_upper_bound"]
                or sample["peak_upper_bound"]
                > sample["mallocs"] * sample["largest_allocation"]
            ):
                raise ValueError("inconsistent live/peak allocation observation")
        result.append(sample)
    return result


def runfile(path: str) -> Path:
    if directory := os.environ.get("RUNFILES_DIR"):
        candidate = Path(directory) / "_main" / path
        if candidate.exists():
            return candidate
    return Path(path).resolve()


class MemoryQualificationTest(unittest.TestCase):
    binary_path = "src/codec/tests/memory_qualification_probe"
    artifact_name = "codec-memory-qualification.json"
    scenarios = SCENARIOS
    scope = (
        "new reactor-local native allocations; initial input and shared engine state "
        "are accounted separately; OpenSSL automatic configuration disabled, "
        "excluding host configuration and configured providers; not RSS or "
        "instrumented timing"
    )

    def probe_path(self, scenario: str) -> str:
        return self.binary_path

    def profile(self, output: str) -> dict[str, str]:
        lines = [line for line in output.splitlines() if line.startswith("allocator=")]
        self.assertEqual(len(lines), 1, output)
        profile = fields("profile " + lines[0])
        self.assertEqual(
            set(profile),
            {
                "allocator",
                "abort",
                "injection",
                "optimized",
                "sanitized",
                "asan",
                "ubsan",
            },
        )
        self.assertIn(profile["allocator"], {"native", "system"})
        for name in set(profile) - {"allocator"}:
            self.assertIn(profile[name], {"true", "false"})
        expected = {
            "allocator": "KWAQUE_EXPECT_TEST_ALLOCATOR",
            "injection": "KWAQUE_EXPECT_TEST_INJECTION",
            "optimized": "KWAQUE_EXPECT_TEST_OPTIMIZED",
            "sanitized": "KWAQUE_EXPECT_TEST_SANITIZED",
        }
        self.assertIn(sum(key in os.environ for key in expected.values()), (0, 4))
        for field, key in expected.items():
            if key in os.environ:
                self.assertEqual(profile[field], os.environ[key], key)
        if profile["allocator"] == "system":
            self.assertEqual(profile["injection"], "false")
        if os.environ.get("KWAQUE_EXPECT_TEST_SANITIZED") == "true":
            self.assertEqual(profile["asan"], "true")
            self.assertEqual(profile["ubsan"], "true")
        return profile

    def execution_bounds(self, samples: dict) -> list[dict]:
        crc_initialization = max(
            samples[name]["peak_upper_bound"]
            for name in ("crc-cold-32", "crc-cold-4096")
        )
        sha_initialization = samples["sha-cold"]["peak_upper_bound"]
        bounds = []
        for name, row in samples.items():
            if name.startswith(("encode-", "decode-")) or name == "staging-fragments":
                initialization = crc_initialization
            elif name in {"sha-warm", "sha-churn", "sha-owner"}:
                initialization = sha_initialization
            elif name.startswith("collection-"):
                initialization = 0
            else:
                continue
            # Deliberately conservative: all new allocations, including
            # separately admitted metadata, plus the full cold engine peak
            # must fit the benchmark's excluded execution allowance.
            bound = initialization + row["peak_upper_bound"]
            self.assertLessEqual(bound, row["execution_reservation"], name)
            bounds.append(
                {
                    "scenario": name,
                    "execution_upper_bound": bound,
                    "reservation": row["execution_reservation"],
                }
            )
        return bounds

    def complete_owner_bounds(
        self, samples: dict, owners: tuple[str, ...], all_new: frozenset[str]
    ) -> list[dict]:
        initialization = samples["sha-cold"]["peak_upper_bound"] + max(
            samples[name]["peak_upper_bound"]
            for name in ("crc-cold-32", "crc-cold-4096")
        )
        bounds = []
        for name in owners:
            row = samples[name]
            operation = row["retained_bound"] + row["peak_upper_bound"] + initialization
            self.assertLessEqual(operation, 64 << 20, name)
            complete_execution = name in all_new
            if complete_execution:
                execution = initialization + row["peak_upper_bound"]
                basis = "all_new_allocations_plus_cold_engines"
            elif row.get("critical_observed", False):
                execution = initialization + row["critical_peak_upper_bound"]
                basis = "critical_allocations_plus_cold_engines"
            else:
                execution = None
                basis = "critical_tracking_unavailable"
            if execution is not None:
                self.assertLessEqual(execution, row["execution_reservation"], name)
            bounds.append(
                {
                    "scenario": name,
                    "operation_upper_bound": operation,
                    "operation_limit": 64 << 20,
                    "execution_upper_bound": execution,
                    "execution_basis": basis,
                    "reservation": row["execution_reservation"],
                }
            )
        return bounds

    def test_native_owner_reservations(self) -> None:
        binary = runfile(self.binary_path)
        output_root = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
        directory = Path(output_root) if output_root else None
        if directory is not None:
            directory.mkdir(parents=True, exist_ok=True)
        with binary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        evidence = {
            "schema_version": 1,
            "binary_sha256": digest,
            "architecture": platform.machine(),
            "complete": False,
            "qualified": False,
            "operation_qualified": False,
            "execution_qualified": False,
            "expected_scenarios": list(self.scenarios),
            "binaries": {},
            "observations": [],
            "invocations": [],
            "environment_overrides": PROBE_ENVIRONMENT,
            "scope": self.scope,
        }

        def save() -> None:
            if directory is not None:
                (directory / self.artifact_name).write_text(
                    json.dumps(evidence, indent=2) + "\n"
                )

        def probe(scenario: str, native: bool = False) -> str:
            path = self.probe_path(scenario)
            selected = runfile(path)
            if path not in evidence["binaries"]:
                with selected.open("rb") as stream:
                    evidence["binaries"][path] = hashlib.file_digest(
                        stream, "sha256"
                    ).hexdigest()
            command = [
                str(selected),
                f"--scenario={scenario}",
                "--reactor-backend=epoll",
                "--smp=1",
                "--memory=256MiB",
                "--overprovisioned",
                "--blocked-reactor-notify-ms=2000000",
                *(["--abort-on-seastar-bad-alloc"] if native else []),
            ]
            evidence["invocations"].append(command)
            save()
            outcome = subprocess.run(
                command,
                env={**os.environ, **PROBE_ENVIRONMENT},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=60,
                check=False,
            )
            if directory is not None:
                (directory / f"{scenario}.log").write_text(outcome.stdout)
            self.assertEqual(outcome.returncode, 0, outcome.stdout)
            return outcome.stdout

        save()
        capability = self.profile(probe("capabilities"))
        native = capability["allocator"] == "native"
        critical_tracking = native and capability["injection"] == "true"
        evidence["critical_tracking"] = critical_tracking
        samples = {}
        for scenario in self.scenarios:
            output = probe(scenario, native)
            self.assertEqual(
                self.profile(output), {**capability, "abort": str(native).lower()}
            )
            compiler = [
                line.removeprefix("compiler=")
                for line in output.splitlines()
                if line.startswith("compiler=")
            ]
            self.assertEqual(len(compiler), 1, output)
            if "compiler" in evidence:
                self.assertEqual(evidence["compiler"], compiler[0])
            evidence["compiler"] = compiler[0]
            evidence["profile"] = self.profile(output)
            measured = observations(output, native)
            expected_names = [scenario]
            if scenario == "observer-control" and native:
                expected_names.extend(
                    [
                        "observer-reallocation",
                        "observer-preexisting-free",
                        "observer-conservative-free",
                    ]
                )
                self.assertIn(
                    "control missed_allocation_detected=true", output.splitlines()
                )
            self.assertEqual(
                [row["scenario"] for row in measured], expected_names, output
            )
            self.assertIn("status=ok", output.splitlines())
            for row in measured:
                row["critical_observed"] = critical_tracking
                samples[row["scenario"]] = row
                evidence["observations"].append(row)
            save()
        execution_qualified = False
        if native:
            critical_peak = samples["observer-control"]["critical_peak_upper_bound"]
            if critical_tracking:
                self.assertGreater(critical_peak, 0)
            else:
                self.assertEqual(critical_peak, 0)
            self.assertEqual(samples["crc-warm-4096"]["mallocs"], 0)
            evidence["execution_bounds"] = self.execution_bounds(samples)
            execution_qualified = all(
                row["execution_upper_bound"] is not None
                for row in evidence["execution_bounds"]
            )
        evidence["complete"] = True
        evidence["operation_qualified"] = native
        evidence["execution_qualified"] = execution_qualified
        evidence["qualified"] = native and execution_qualified
        save()
        if not native:
            self.skipTest(
                "semantic scenarios completed; native allocation observation is unavailable"
            )
        if not execution_qualified:
            self.skipTest(
                "native operation bounds checked; critical-subset execution qualification "
                "requires a native build with allocation-failure injection enabled"
            )
