"""Observe fixed-value allocations in a fresh native process."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import unittest
from pathlib import Path

SAMPLES = (
    "cold-invalid-id",
    "warm-invalid-id",
    "identity",
    "epoch-span",
    "warm-invalid-span",
    "batch-context",
    "warm-invalid-context",
    "coverage-0",
    "coverage-1",
    "coverage-64",
    "limits",
    "observer-control",
)
COUNTERS = {
    "mallocs",
    "requested_bytes",
    "foreign_allocations",
    "fallback_allocations",
}


def runfile(path: str) -> Path:
    if root := os.environ.get("RUNFILES_DIR"):
        candidate = Path(root) / "_main" / path
        if candidate.exists():
            return candidate
    return Path(path).resolve()


def fields(line: str) -> dict[str, str]:
    tokens = [token.split("=", 1) for token in line.split()[1:]]
    if any(len(token) != 2 for token in tokens):
        raise ValueError("invalid observation field")
    result = dict(tokens)
    if len(result) != len(tokens):
        raise ValueError("duplicate observation field")
    return result


class ValueAllocationTest(unittest.TestCase):
    def profile(self, output: str) -> dict[str, str]:
        lines = [line for line in output.splitlines() if line.startswith("profile ")]
        self.assertEqual(len(lines), 1, output)
        result = fields(lines[0])
        self.assertEqual(
            set(result),
            {
                "allocator",
                "injection",
                "optimized",
                "sanitized",
                "asan",
                "ubsan",
                "abort",
            },
        )
        self.assertIn(result["allocator"], {"native", "system"})
        for key in set(result) - {"allocator"}:
            self.assertIn(result[key], {"true", "false"})
        expectations = {
            "allocator": "KWAQUE_EXPECT_TEST_ALLOCATOR",
            "injection": "KWAQUE_EXPECT_TEST_INJECTION",
            "optimized": "KWAQUE_EXPECT_TEST_OPTIMIZED",
            "sanitized": "KWAQUE_EXPECT_TEST_SANITIZED",
        }
        supplied = sum(name in os.environ for name in expectations.values())
        self.assertIn(supplied, (0, len(expectations)))
        for key, name in expectations.items():
            if name in os.environ:
                self.assertEqual(result[key], os.environ[name], name)
        if result["allocator"] == "system":
            self.assertEqual(result["injection"], "false")
        if os.environ.get("KWAQUE_EXPECT_TEST_SANITIZED") == "true":
            self.assertEqual(result["asan"], "true")
            self.assertEqual(result["ubsan"], "true")
        return result

    def test_cold_and_warm_value_allocations(self) -> None:
        binary = runfile("src/model/tests/value_allocation_probe")
        with binary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        output_root = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
        directory = Path(output_root) if output_root else None
        if directory is not None:
            directory.mkdir(parents=True, exist_ok=True)
        evidence = {
            "schema_version": 1,
            "binary_sha256": digest,
            "host": platform.platform(),
            "complete": False,
            "allocation_qualified": False,
            "expected_samples": list(SAMPLES),
        }

        def write_evidence() -> None:
            if directory is not None:
                (directory / "value-allocations.json").write_text(
                    json.dumps(evidence, indent=2) + "\n"
                )

        def run_probe(label: str, *options: str) -> subprocess.CompletedProcess[str]:
            command = [
                str(binary),
                "--reactor-backend=epoll",
                "--smp=1",
                "--memory=64MiB",
                "--overprovisioned",
                *options,
            ]
            evidence[f"{label}_command"] = command
            write_evidence()
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=60,
                check=False,
            )
            if directory is not None:
                (directory / f"{label}.log").write_text(result.stdout)
            self.assertEqual(result.returncode, 0, result.stdout)
            return result

        write_evidence()
        capabilities = self.profile(run_probe("capabilities", "--capabilities").stdout)
        native = capabilities["allocator"] == "native"
        options = ("--abort-on-seastar-bad-alloc",) if native else ()
        # A distinct subprocess: the capabilities run cannot warm this category.
        observed = run_probe("values", *options)
        profile = self.profile(observed.stdout)
        self.assertEqual(
            profile, {**capabilities, "abort": "true" if native else "false"}
        )
        evidence["profile"] = profile
        compilers = [
            line.removeprefix("compiler=")
            for line in observed.stdout.splitlines()
            if line.startswith("compiler=")
        ]
        self.assertEqual(len(compilers), 1, observed.stdout)
        evidence["compiler"] = compilers[0]
        samples = [
            fields(line)
            for line in observed.stdout.splitlines()
            if line.startswith("sample ")
        ]
        evidence["samples"] = samples
        write_evidence()
        self.assertEqual(tuple(sample["name"] for sample in samples), SAMPLES)
        for sample in samples:
            self.assertEqual(sample["memory_observed"], "true" if native else "false")
            self.assertEqual(
                set(sample),
                {"name", "memory_observed"} | (COUNTERS if native else set()),
            )
            if native:
                for name in COUNTERS:
                    self.assertTrue(sample[name].isascii() and sample[name].isdecimal())
                    value = int(sample[name])
                    if sample["name"] == "cold-invalid-id":
                        # Shared error initialization is an observation, not a
                        # stronger allocation-free contract inferred from layout.
                        continue
                    if sample["name"] == "observer-control" and name in {
                        "mallocs",
                        "requested_bytes",
                    }:
                        self.assertGreaterEqual(
                            value, 32 if name == "requested_bytes" else 1
                        )
                    else:
                        self.assertEqual(value, 0, sample["name"])
        self.assertIn("status=ok", observed.stdout.splitlines())
        evidence["complete"] = True
        evidence["allocation_qualified"] = native
        write_evidence()
        if not native:
            self.skipTest(
                "value checks passed; system allocation counters are unavailable"
            )


if __name__ == "__main__":
    unittest.main()
