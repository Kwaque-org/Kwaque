"""Observe complete model/compression operations under the native allocator."""

from __future__ import annotations

import unittest

from src.codec.tests import memory_qualification_driver as shared

ENGINE_SCENARIOS = (
    "observer-control",
    "crc-cold-32",
    "crc-cold-4096",
    "crc-warm-4096",
    "sha-cold",
)
OWNER_SCENARIOS = (
    "record-materialize",
    "record-materialize-max",
    "record-scan",
    "batch-decode-max",
    "batch-decode-headers",
    "batch-build-max",
    "batch-build-headers",
    "batch-fingerprint-max",
    "batch-rewrite-max",
    "batch-rewrite-abort",
    "batch-decode-abort",
    "batch-decode-pressure",
    "batch-lz4-encode",
    "batch-lz4-decode",
    "lz4-compress",
    "lz4-decompress",
    "lz4-compress-abort",
    "lz4-decompress-abort",
    "lz4-decompress-corrupt",
)
# These paths allocate only temporary/returned metadata and execution state;
# all payload backing already exists before observation. Their entire new peak
# can conservatively be charged to the excluded execution allowance too.
ALIAS_ONLY = frozenset(
    (
        "record-materialize",
        "record-materialize-max",
        "record-scan",
        "batch-decode-max",
        "batch-decode-headers",
        "batch-fingerprint-max",
        "batch-decode-abort",
        "batch-decode-pressure",
    )
)


class MemoryQualificationTest(shared.MemoryQualificationTest):
    binary_path = "src/model/tests/memory_qualification_probe"
    artifact_name = "model-memory-qualification.json"
    scenarios = ENGINE_SCENARIOS + OWNER_SCENARIOS
    scope = (
        "reactor-local new-allocation upper bounds plus retained input/cache and "
        "separate cold-engine bounds; execution_basis distinguishes all-new "
        "allocation checks from critical/frame/control-subset checks for paths "
        "that allocate payload/scratch; OpenSSL automatic configuration disabled; "
        "not RSS, arbitrary caller/opaque-owner qualification or instrumented timing"
    )

    def probe_path(self, scenario: str) -> str:
        if scenario in ENGINE_SCENARIOS:
            return shared.MemoryQualificationTest.binary_path
        return self.binary_path

    def execution_bounds(self, samples: dict) -> list[dict]:
        initialization = samples["sha-cold"]["peak_upper_bound"] + max(
            samples[name]["peak_upper_bound"]
            for name in ("crc-cold-32", "crc-cold-4096")
        )
        bounds = []
        for name in OWNER_SCENARIOS:
            row = samples[name]
            # Global peak includes every observed allocation, including newly
            # copied/expanded payload, C-library scratch and coroutine frames.
            operation = row["retained_bound"] + row["peak_upper_bound"] + initialization
            self.assertLessEqual(operation, 64 << 20, name)
            all_new = name in ALIAS_ONLY
            observed_execution = row[
                "peak_upper_bound" if all_new else "critical_peak_upper_bound"
            ]
            execution = observed_execution + initialization
            self.assertLessEqual(execution, row["execution_reservation"], name)
            bounds.append(
                {
                    "scenario": name,
                    "operation_upper_bound": operation,
                    "operation_limit": 64 << 20,
                    "execution_upper_bound": execution,
                    "execution_basis": (
                        "all_new_allocations_plus_cold_engines"
                        if all_new
                        else "critical_allocations_plus_cold_engines"
                    ),
                    "reservation": row["execution_reservation"],
                }
            )
        self.assertGreater(samples["batch-decode-headers"]["mallocs"], 0)
        return bounds


class ReservationBoundsTest(unittest.TestCase):
    def samples(self) -> dict:
        return {
            name: {
                "mallocs": 1,
                "peak_upper_bound": 1024,
                "critical_peak_upper_bound": 256,
                "retained_bound": 1024,
                "execution_reservation": 1 << 20,
            }
            for name in ENGINE_SCENARIOS + OWNER_SCENARIOS
        }

    def test_new_payload_is_in_global_peak_but_not_labeled_as_frame_memory(self):
        samples = self.samples()
        samples["batch-build-max"]["peak_upper_bound"] = 8 << 20
        bounds = MemoryQualificationTest().execution_bounds(samples)
        built = next(row for row in bounds if row["scenario"] == "batch-build-max")
        self.assertGreater(built["operation_upper_bound"], 8 << 20)
        self.assertEqual(
            built["execution_basis"], "critical_allocations_plus_cold_engines"
        )

    def test_global_peak_critical_peak_and_alias_only_peak_are_separate_gates(self):
        for name, field, value in (
            ("batch-build-max", "peak_upper_bound", 64 << 20),
            ("batch-build-max", "critical_peak_upper_bound", 1 << 20),
            ("record-materialize", "peak_upper_bound", 1 << 20),
            ("batch-decode-headers", "mallocs", 0),
        ):
            samples = self.samples()
            samples[name][field] = value
            with self.subTest(name=name, field=field), self.assertRaises(
                AssertionError
            ):
                MemoryQualificationTest().execution_bounds(samples)


if __name__ == "__main__":
    unittest.main()
