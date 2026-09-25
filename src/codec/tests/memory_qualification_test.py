"""Codec memory qualification entry point and observation parser tests."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.codec.tests import memory_qualification_driver as shared


class ObservationParserTest(unittest.TestCase):
    def line(self) -> str:
        return (
            "observation scenario=fixture bytes=32 retained_bound=64 "
            "execution_reservation=1024 observed=true complete=true "
            "mallocs=1 frees=0 native_mallocs=1 native_frees=1 "
            "peak_upper_bound=64 live_upper_bound=64 "
            "critical_peak_upper_bound=32 largest_allocation=64"
        )

    def test_missing_frees_retain_an_explicit_conservative_bound(self) -> None:
        measured = shared.observations(self.line(), True)[0]
        self.assertEqual(measured["native_frees"], 1)
        self.assertEqual(measured["frees"], 0)
        self.assertEqual(measured["live_upper_bound"], 64)

    def test_missing_allocations_or_incomplete_tracking_cannot_qualify(self) -> None:
        for line in (
            self.line().replace("complete=true", "complete=false"),
            self.line().replace("native_mallocs=1", "native_mallocs=2"),
            self.line().replace("frees=0", "frees=2"),
        ):
            with self.subTest(line=line), self.assertRaises(ValueError):
                shared.observations(line, True)

    def test_system_profile_does_not_invent_zero_measurements(self) -> None:
        line = (
            "observation scenario=fixture bytes=32 retained_bound=64 "
            "execution_reservation=1024 observed=false"
        )
        self.assertNotIn("peak_upper_bound", shared.observations(line, False)[0])
        with self.assertRaises(ValueError):
            shared.observations(line + " peak_upper_bound=0", False)

    def test_malformed_and_inconsistent_measurements_reject(self) -> None:
        for line in (
            self.line() + " mallocs=1",
            self.line().replace("mallocs=1", "mallocs=-1"),
            self.line().replace(" live_upper_bound=64", " live_upper_bound=65"),
            self.line().replace(
                "critical_peak_upper_bound=32", "critical_peak_upper_bound=65"
            ),
            self.line().replace(" largest_allocation=64", ""),
            self.line().replace("largest_allocation=64", "largest_allocation=32"),
            self.line().replace("largest_allocation=64", "largest_allocation=65"),
        ):
            with self.subTest(line=line), self.assertRaises(ValueError):
                shared.observations(line, True)


class MemoryQualificationTest(shared.MemoryQualificationTest):
    pass


class CriticalReservationTest(unittest.TestCase):
    def samples(self, tracked: bool) -> dict:
        return {
            name: {
                "retained_bound": 1024,
                "peak_upper_bound": (8 << 20) if name == "payload" else 1024,
                "critical_peak_upper_bound": 256 if tracked else 0,
                "critical_observed": tracked,
                "execution_reservation": 1 << 20,
            }
            for name in (
                "sha-cold",
                "crc-cold-32",
                "crc-cold-4096",
                "payload",
                "metadata",
            )
        }

    def bounds(self, samples: dict) -> list[dict]:
        return shared.MemoryQualificationTest().complete_owner_bounds(
            samples, ("payload", "metadata"), frozenset({"metadata"})
        )

    def test_unavailable_critical_tracking_cannot_certify_an_execution_bound(self):
        payload, metadata = self.bounds(self.samples(False))
        self.assertIsNone(payload["execution_upper_bound"])
        self.assertEqual(payload["execution_basis"], "critical_tracking_unavailable")
        self.assertGreater(payload["operation_upper_bound"], 8 << 20)
        self.assertEqual(metadata["execution_upper_bound"], 3072)

    def test_absent_capability_is_not_assumed_available(self):
        samples = self.samples(False)
        del samples["payload"]["critical_observed"]
        self.assertIsNone(self.bounds(samples)[0]["execution_upper_bound"])

    def test_release_still_checks_total_and_all_new_allocation_bounds(self):
        for name, field, value in (
            ("payload", "peak_upper_bound", 64 << 20),
            ("payload", "retained_bound", 64 << 20),
            ("metadata", "peak_upper_bound", 1 << 20),
            ("sha-cold", "peak_upper_bound", 1 << 20),
        ):
            samples = self.samples(False)
            samples[name][field] = value
            with self.subTest(name=name, field=field), self.assertRaises(
                AssertionError
            ):
                self.bounds(samples)

    def test_available_critical_tracking_preserves_its_separate_gate(self):
        samples = self.samples(True)
        payload, _ = self.bounds(samples)
        self.assertEqual(payload["execution_upper_bound"], 2304)
        self.assertEqual(
            payload["execution_basis"], "critical_allocations_plus_cold_engines"
        )
        samples["payload"]["critical_peak_upper_bound"] = 1 << 20
        with self.assertRaises(AssertionError):
            self.bounds(samples)

    def test_driver_runs_all_native_scenarios_before_reporting_partial_qualification(
        self,
    ):
        class OwnerHarness(shared.MemoryQualificationTest):
            scenarios = (
                "observer-control",
                "crc-cold-32",
                "crc-cold-4096",
                "crc-warm-4096",
                "sha-cold",
                "payload",
                "metadata",
            )

            def execution_bounds(self, samples):
                return self.complete_owner_bounds(
                    samples, ("payload", "metadata"), frozenset({"metadata"})
                )

        for tracked in (False, True):
            with self.subTest(
                tracked=tracked
            ), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "mock-probe"
                binary.write_bytes(b"never executed")
                harness = OwnerHarness()
                harness.binary_path = str(binary)

                def native_output(command, **kwargs):
                    scenario = next(
                        arg.split("=", 1)[1]
                        for arg in command
                        if arg.startswith("--scenario=")
                    )
                    abort = "--abort-on-seastar-bad-alloc" in command
                    output = (
                        f"allocator=native abort={str(abort).lower()} "
                        f"injection={str(tracked).lower()} optimized=true "
                        "sanitized=false asan=false ubsan=false\ncompiler=fixture\n"
                    )
                    if scenario == "capabilities":
                        return SimpleNamespace(returncode=0, stdout=output)
                    names = [scenario]
                    if scenario == "observer-control":
                        names += [
                            "observer-reallocation",
                            "observer-preexisting-free",
                            "observer-conservative-free",
                        ]
                        output += "control missed_allocation_detected=true\n"
                    for name in names:
                        peak = (
                            0
                            if name == "crc-warm-4096"
                            else (8 << 20) if name == "payload" else 1024
                        )
                        largest = min(peak, 128 << 10)
                        allocations = peak // largest if largest else 0
                        critical = min(peak, 256) if tracked else 0
                        output += (
                            f"observation scenario={name} bytes=32 retained_bound=1024 "
                            f"execution_reservation={1 << 20} observed=true complete=true "
                            f"mallocs={allocations} frees=0 native_mallocs={allocations} native_frees=0 "
                            f"peak_upper_bound={peak} live_upper_bound={peak} "
                            f"critical_peak_upper_bound={critical} largest_allocation={largest}\n"
                        )
                    return SimpleNamespace(returncode=0, stdout=output + "status=ok\n")

                with patch.dict(
                    shared.os.environ,
                    {"TEST_UNDECLARED_OUTPUTS_DIR": directory},
                    clear=True,
                ), patch.object(
                    shared.subprocess, "run", side_effect=native_output
                ) as invoked:
                    if tracked:
                        harness.test_native_owner_reservations()
                    else:
                        with self.assertRaisesRegex(
                            unittest.SkipTest, "critical-subset"
                        ):
                            harness.test_native_owner_reservations()
                self.assertEqual(invoked.call_count, 1 + len(harness.scenarios))
                evidence = json.loads(
                    (Path(directory) / harness.artifact_name).read_text()
                )
                self.assertTrue(evidence["complete"])
                self.assertTrue(evidence["operation_qualified"])
                self.assertEqual(evidence["execution_qualified"], tracked)
                self.assertEqual(evidence["qualified"], tracked)
                self.assertTrue(
                    all(
                        row["critical_observed"] == tracked
                        for row in evidence["observations"]
                    )
                )


if __name__ == "__main__":
    unittest.main()
