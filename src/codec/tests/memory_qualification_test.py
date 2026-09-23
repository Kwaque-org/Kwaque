"""Codec memory qualification entry point and observation parser tests."""

import unittest

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


if __name__ == "__main__":
    unittest.main()
