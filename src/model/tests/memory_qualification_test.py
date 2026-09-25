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
MODEL_SCENARIOS = (
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
CHECKPOINT_SCENARIOS = (
    tuple(
        f"checkpoint-{owner}-{case}"
        for owner in ("sorted", "unordered")
        for case in (
            "4096",
            "4097",
            "count-limit",
            "object-limit",
            "allocation-limit",
            "metadata-pressure",
            "operation-pressure",
            "work-limit",
            "abort",
            "duplicate",
        )
    )
    + (
        "checkpoint-unordered-partial",
        "checkpoint-unordered-free-chunks",
        "checkpoint-unordered-scratch-limit",
    )
    + (
        "checkpoint-fingerprint-4096",
        "checkpoint-encode-4096",
        "checkpoint-encode-operation-pressure",
        "checkpoint-encode-abort",
    )
    + tuple(
        f"checkpoint-decode-{case}"
        for case in (
            "h32",
            "h41",
            "h4096",
            "4097",
            "count-limit",
            "object-limit",
            "byte-limit",
            "allocation-limit",
            "metadata-pressure",
            "operation-pressure",
            "work-limit",
            "abort",
            "duplicate",
            "corrupt",
            "wrong-topic",
        )
    )
)
OWNER_SCENARIOS = MODEL_SCENARIOS + CHECKPOINT_SCENARIOS

# Selected record paths allocate metadata/aliases; checkpoint paths also own
# bounded serialized bodies and sort/conversion storage. For these owners, the
# entire new peak can conservatively fit the excluded execution allowance.
ALL_NEW_EXECUTION = frozenset(
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
) | frozenset(CHECKPOINT_SCENARIOS)


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
        bounds = self.complete_owner_bounds(samples, OWNER_SCENARIOS, ALL_NEW_EXECUTION)
        self.assertGreater(samples["batch-decode-headers"]["mallocs"], 0)
        return bounds


class ReservationBoundsTest(unittest.TestCase):
    def samples(self) -> dict:
        return {
            name: {
                "mallocs": 1,
                "peak_upper_bound": 1024,
                "critical_peak_upper_bound": 256,
                "critical_observed": True,
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
            ("checkpoint-unordered-partial", "peak_upper_bound", 1 << 20),
            ("checkpoint-encode-4096", "retained_bound", 64 << 20),
        ):
            samples = self.samples()
            samples[name][field] = value
            with self.subTest(name=name, field=field), self.assertRaises(
                AssertionError
            ):
                MemoryQualificationTest().execution_bounds(samples)


if __name__ == "__main__":
    unittest.main()
