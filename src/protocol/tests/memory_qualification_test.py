"""Observe complete frame and control owners without a socket or connection."""

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
FRAME_SCENARIOS = (
    tuple(
        f"{operation}-h{header}"
        for operation in ("prefix", "header")
        for header in (48, 57, 4096)
    )
    + tuple(
        f"{operation}-{shape}"
        for operation in ("encode", "decode")
        for shape in ("empty", "tiny", "max", "max-fragmented", "abort", "pressure")
    )
    + (
        "encode-fragment-limit",
        "decode-control",
        "decode-extended",
        "decode-missing-prefix",
        "decode-missing-header",
        "decode-missing-payload",
        "decode-corrupt",
    )
    + tuple(
        f"raw-{kind}-{encoding}-{shape}"
        for kind in ("submitted", "assigned", "sparse")
        for encoding in ("none", "lz4")
        for shape in ("tiny", "max")
    )
    + tuple(
        f"raw-assigned-none-{failure}"
        for failure in (
            "abort",
            "pressure",
            "trailing",
            "corrupt-inner",
            "wrong-kind",
            "wrong-context",
        )
    )
)
CONTROL_ROOTS = ("request", "response", "redirect", "error")
CONTROL_SCENARIOS = (
    tuple(
        f"control-{root}-{operation}-{shape}"
        for root in CONTROL_ROOTS
        for operation in ("decode", "decode_frame", "make", "encode", "encode_frame")
        for shape in ("tiny", "max", "abort", "pressure")
    )
    + tuple(
        f"control-{root}-{operation}-{shape}"
        for root in CONTROL_ROOTS
        for operation in ("decode", "decode_frame")
        for shape in ("wire_max", "malformed", "context", "late_abort")
    )
    + tuple(
        f"control-request-{operation}-{shape}"
        for operation in ("decode", "decode_frame")
        for shape in (
            "distributed",
            "churn",
            *(f"repeated_{count}" for count in (1, 2, 3, 15, 16, 17, 31, 32)),
        )
    )
    + tuple(
        f"control-error-{operation}-unknowns"
        for operation in ("decode", "decode_frame")
    )
    + tuple(
        f"control-{root}-decode-{shape}"
        for root in CONTROL_ROOTS
        for shape in ("exact", "short")
    )
    + tuple(
        f"control-{root}-{operation}-build_limit"
        for root in ("request", "response")
        for operation in ("decode", "decode_frame", "make", "encode", "encode_frame")
    )
    + tuple(
        f"control-{root}-{operation}-late_abort"
        for root in CONTROL_ROOTS
        for operation in ("encode", "encode_frame")
    )
)
OWNER_SCENARIOS = FRAME_SCENARIOS + CONTROL_SCENARIOS
# Compressed model decoding allocates expanded records. Its complete new peak
# still belongs to the operation bound; the critical subset is labeled as such.
PAYLOAD_PRODUCING = frozenset(
    name for name in OWNER_SCENARIOS if name.startswith("raw-") and "-lz4-" in name
)


class MemoryQualificationTest(shared.MemoryQualificationTest):
    binary_path = "src/protocol/tests/memory_qualification_probe"
    artifact_name = "frame-memory-qualification.json"
    scenarios = ENGINE_SCENARIOS + OWNER_SCENARIOS
    scope = (
        "supplied-byte frame owners; retained input and setup plus complete new "
        "native peaks and cold engines; exact prefix zero-allocation checks; "
        "critical subset distinguished for compressed raw payload expansion; "
        "controls include first native parsing, conversion, immutable publication "
        "and joined temporary cleanup, with returned owners live at observation "
        "end; complete control aggregate also includes supplied owners and cold "
        "engines under 1 MiB; only exact/short/late_abort controls warm a baseline; "
        "OpenSSL automatic configuration disabled; not socket/connection, RSS "
        "or arbitrary opaque caller qualification and not instrumented timing"
    )

    def probe_path(self, scenario: str) -> str:
        return (
            shared.MemoryQualificationTest.binary_path
            if scenario in ENGINE_SCENARIOS
            else self.binary_path
        )

    def execution_bounds(self, samples: dict) -> list[dict]:
        bounds = self.complete_owner_bounds(
            samples, OWNER_SCENARIOS, frozenset(OWNER_SCENARIOS) - PAYLOAD_PRODUCING
        )
        for name in OWNER_SCENARIOS:
            if name.startswith("prefix-"):
                self.assertEqual(samples[name]["mallocs"], 0, name)
                self.assertEqual(samples[name]["peak_upper_bound"], 0, name)
        for row in bounds:
            if row["scenario"] in CONTROL_SCENARIOS:
                # This deliberately includes initial fixture/donor storage and
                # all new allocations, not only the observer's critical subset.
                self.assertLessEqual(row["operation_upper_bound"], 1 << 20, row)
                row["control_aggregate_upper_bound"] = row["operation_upper_bound"]
                row["control_aggregate_limit"] = 1 << 20
        return bounds


class ReservationBoundsTest(unittest.TestCase):
    def samples(self) -> dict:
        samples = {
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
        for name, row in samples.items():
            if name.startswith("prefix-"):
                row.update(mallocs=0, peak_upper_bound=0, critical_peak_upper_bound=0)
        return samples

    def test_expanded_payload_remains_in_global_peak(self):
        samples = self.samples()
        name = "raw-assigned-lz4-max"
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

    def test_independent_bounds_and_allocation_free_prefix_are_enforced(self):
        for name, field, value in (
            ("raw-assigned-lz4-max", "peak_upper_bound", 64 << 20),
            ("raw-assigned-lz4-max", "critical_peak_upper_bound", 1 << 20),
            ("decode-max-fragmented", "peak_upper_bound", 1 << 20),
            ("decode-max-fragmented", "retained_bound", 64 << 20),
            ("prefix-h48", "mallocs", 1),
            ("prefix-h4096", "peak_upper_bound", 1),
            ("sha-cold", "peak_upper_bound", 1 << 20),
        ):
            with self.subTest(name=name, field=field), self.assertRaises(
                AssertionError
            ):
                samples = self.samples()
                samples[name][field] = value
                MemoryQualificationTest().execution_bounds(samples)

    def test_controls_use_complete_peak_and_initial_owner_storage(self):
        self.assertEqual(len(CONTROL_SCENARIOS), len(set(CONTROL_SCENARIOS)))
        samples = self.samples()
        rows = MemoryQualificationTest().execution_bounds(samples)
        controls = [row for row in rows if row["scenario"] in CONTROL_SCENARIOS]
        self.assertEqual(len(controls), len(CONTROL_SCENARIOS))
        for row in controls:
            self.assertEqual(
                row["execution_basis"], "all_new_allocations_plus_cold_engines"
            )
            self.assertEqual(row["control_aggregate_upper_bound"], 4096)
            self.assertEqual(row["control_aggregate_limit"], 1 << 20)
        for name in CONTROL_SCENARIOS:
            for field in ("peak_upper_bound", "retained_bound"):
                with self.subTest(name=name, field=field):
                    changed = self.samples()
                    # Critical allocations remain small: using that subset or
                    # omitting retained owners would incorrectly accept this.
                    changed[name][field] = (1 << 20) - 2048
                    with self.assertRaises(AssertionError):
                        MemoryQualificationTest().execution_bounds(changed)

    def test_every_control_root_keeps_all_owning_operations_and_failures(self):
        for root in CONTROL_ROOTS:
            for operation in (
                "decode",
                "decode_frame",
                "make",
                "encode",
                "encode_frame",
            ):
                for shape in ("tiny", "max", "abort", "pressure"):
                    self.assertIn(
                        f"control-{root}-{operation}-{shape}", CONTROL_SCENARIOS
                    )


if __name__ == "__main__":
    unittest.main()
