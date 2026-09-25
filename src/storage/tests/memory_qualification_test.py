"""Qualify supplied-byte storage owners under the native allocation observer."""

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
GEOMETRIES = tuple(f"h{h}-a{a}" for h in (32, 4096) for a in (512, 65536))
OWNER_SCENARIOS = (
    tuple(
        f"{owner}-decode-{geometry}"
        for owner in ("header", "durable")
        for geometry in GEOMETRIES
    )
    + tuple(
        f"{owner}-encode-h32-a{a}"
        for owner in ("header", "durable")
        for a in (512, 65536)
    )
    + tuple(
        f"child-{operation}-{encoding}"
        for operation in (
            "make",
            "share",
            "validate",
            "reuse",
            "narrow",
            "narrow-reject",
            "narrow-max",
        )
        for encoding in ("none", "lz4")
    )
    + tuple(
        f"{owner}-{operation}-{encoding}-a{a}"
        for owner in ("block", "wal")
        for operation in ("encode", "decode")
        for encoding in ("none", "lz4")
        for a in (512, 65536)
    )
    + tuple(
        f"{owner}-extended-decode-{encoding}-h4096-a{a}"
        for owner in ("block", "wal")
        for encoding in ("none", "lz4")
        for a in (512, 65536)
    )
    + tuple(
        f"{owner}-{part}-decode-{geometry}"
        for owner in ("retry", "index", "manifest")
        for part in ("page", "root")
        for geometry in GEOMETRIES
    )
    + tuple(
        f"{owner}-{part}-encode-h32-a{a}"
        for owner in ("retry", "index", "manifest")
        for part in ("page", "root")
        for a in (512, 65536)
    )
    + tuple(
        f"{owner}-walk-{geometry}"
        for owner in ("retry", "index", "manifest")
        for geometry in GEOMETRIES
    )
    + (
        "block-abort",
        "block-pressure",
        "block-padding",
        "block-nested",
        "extent-empty",
        "extent-reuse",
        "extent-failure",
        "extent-close",
    )
)
# Decoding compressed maximum data creates ordinary expanded payload; the
# critical subset cannot certify its entire execution footprint. All other
# selected operations create only bounded staging/metadata and aliases, so
# their entire new peak is conservatively tested against the reserved MiB.
PAYLOAD_PRODUCING = frozenset(
    name
    for name in OWNER_SCENARIOS
    if (name.startswith(("block-decode-lz4", "wal-decode-lz4")))
    or name in {"child-make-lz4", "child-validate-lz4", "child-narrow-max-lz4"}
)


class MemoryQualificationTest(shared.MemoryQualificationTest):
    binary_path = "src/storage/tests/memory_qualification_probe"
    artifact_name = "storage-memory-qualification.json"
    scenarios = ENGINE_SCENARIOS + OWNER_SCENARIOS
    scope = (
        "supplied-byte storage owners; retained input/fixtures/pins plus all new "
        "native allocations and cold-engine bounds; one decoded page at a time; "
        "continuous extent observation includes SHA state retained between calls; "
        "execution_basis distinguishes all-new peaks from critical subsets on "
        "compressed payload expansion; OpenSSL automatic configuration disabled; "
        "not filesystem/recovery qualification, RSS or instrumented timing"
    )

    def probe_path(self, scenario: str) -> str:
        if scenario in ENGINE_SCENARIOS:
            return shared.MemoryQualificationTest.binary_path
        return self.binary_path

    def execution_bounds(self, samples: dict) -> list[dict]:
        return self.complete_owner_bounds(
            samples, OWNER_SCENARIOS, frozenset(OWNER_SCENARIOS) - PAYLOAD_PRODUCING
        )


class ReservationBoundsTest(unittest.TestCase):
    def samples(self) -> dict:
        return {
            name: {
                "peak_upper_bound": 1024,
                "critical_peak_upper_bound": 256,
                "critical_observed": True,
                "retained_bound": 1024,
                "execution_reservation": 1 << 20,
            }
            for name in ENGINE_SCENARIOS + OWNER_SCENARIOS
        }

    def test_payload_expansion_is_never_subtracted_from_the_operation_peak(self):
        samples = self.samples()
        name = "wal-decode-lz4-a65536"
        samples[name]["peak_upper_bound"] = 8 << 20
        row = next(
            row
            for row in MemoryQualificationTest().execution_bounds(samples)
            if row["scenario"] == name
        )
        self.assertGreater(row["operation_upper_bound"], 8 << 20)
        self.assertEqual(
            row["execution_basis"], "critical_allocations_plus_cold_engines"
        )

    def test_independent_gates_reject_overflow_and_include_cold_initialization(self):
        for name, field, value in (
            ("wal-decode-lz4-a512", "peak_upper_bound", 64 << 20),
            ("wal-decode-lz4-a512", "retained_bound", 64 << 20),
            ("wal-decode-lz4-a512", "critical_peak_upper_bound", 1 << 20),
            ("retry-page-decode-h4096-a65536", "peak_upper_bound", 1 << 20),
            ("extent-reuse", "peak_upper_bound", 1 << 20),
            ("sha-cold", "peak_upper_bound", 1 << 20),
        ):
            with self.subTest(name=name, field=field), self.assertRaises(
                AssertionError
            ):
                samples = self.samples()
                samples[name][field] = value
                MemoryQualificationTest().execution_bounds(samples)

    def test_names_are_unique_and_matrix_covers_all_selected_geometries(self):
        self.assertEqual(len(OWNER_SCENARIOS), len(set(OWNER_SCENARIOS)))
        for owner in ("retry", "index", "manifest"):
            for geometry in GEOMETRIES:
                self.assertIn(f"{owner}-page-decode-{geometry}", OWNER_SCENARIOS)
                self.assertIn(f"{owner}-root-decode-{geometry}", OWNER_SCENARIOS)
                self.assertIn(f"{owner}-walk-{geometry}", OWNER_SCENARIOS)


if __name__ == "__main__":
    unittest.main()
