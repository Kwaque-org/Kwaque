import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests.wal_cohort_report import (
    PREFIX,
    compare,
    percentile,
    read_measurements,
    summarize,
    validate,
)


def measurement():
    return {
        "owner": "writer",
        "case": "fixed",
        "size": 0,
        "fragmented": False,
        "child_bytes": 223,
        "child_fragments": 4,
        "delay_ns": 0,
        "timing_only": False,
        "preallocated": True,
        "foreground_probe": False,
        "memory_observed": False,
        "comparison_id": "a" * 64,
        "device": 1,
        "setup_extent": 2097152,
        "target_members": 2,
        "target_bytes": 65536,
        "group_capacity": 8,
        "sha256": "b" * 64,
        "encoded_bytes": 16384,
        "offered": 2,
        "accepted": 2,
        "elapsed_ns": 100,
        "flushes": 1,
        "flush_times": [[30, 60]],
        "native_calls": 2,
        "logical_writes": 1,
        "allocations": 4,
        "tasks": 8,
        "sampled_retained_admission": 4096,
        "batch_wait_ns": 10,
        "write_service_ns": 30,
        "flush_service_ns": 30,
        "foreground_turns": 2,
        "build_profile": {
            "allocator": "native",
            "injection": "false",
            "optimized": "true",
            "asan": "false",
            "ubsan": "false",
            "oom_abort": "true",
        },
        "requests": [
            {
                "accepted": True,
                "timeout": False,
                "queue_ns": 0,
                "admission_ns": 10,
                "latency_ns": 80,
                "terminal": 80,
                "file": 0,
                "end": 16384,
                "covering_end": 24576,
            },
            {
                "accepted": True,
                "timeout": False,
                "queue_ns": 5,
                "admission_ns": 12,
                "latency_ns": 95,
                "terminal": 95,
                "file": 0,
                "end": 24576,
                "covering_end": 24576,
            },
        ],
    }


def aggregate_measurement():
    row = measurement()
    row["observation"] = "aggregate"
    row["affinity_cpus"] = [4]
    for request in row["requests"]:
        request.pop("queue_ns")
        request.update(
            arrival_lateness_ns=None, admission_ns=None, latency_ns=None, terminal=None
        )
    return row


class ReportTest(unittest.TestCase):
    def test_cpu_observation_is_explicit_and_part_of_pair_identity(self):
        row = measurement()
        row["reactor_cpu_ns"] = 15
        validate(row)
        self.assertEqual(summarize([row])["reactor_cpu_ns"]["samples"], [15])
        for invalid in (-1, None, True, 1.5):
            row["reactor_cpu_ns"] = invalid
            with self.subTest(value=invalid), self.assertRaisesRegex(ValueError, "CPU"):
                validate(row)
        row["reactor_cpu_ns"] = 15
        with self.assertRaisesRegex(ValueError, "comparison changed"):
            compare([row], [measurement()])
        with self.assertRaisesRegex(ValueError, "CPU"):
            summarize([row, measurement()])

    def test_write_overlap_is_distinct_from_cumulative_service(self):
        row = measurement()
        # Two overlapping intervals can total more than the workload's wall
        # time. The union and longest individual interval cannot.
        row.update(write_service_ns=120, write_busy_ns=80, write_max_service_ns=70)
        validate(row)
        summary = summarize([row])
        self.assertEqual(summary["write_busy_ns"], 80)
        self.assertEqual(summary["write_max_service_ns"], 70)
        self.assertEqual(summary["write_service_ns"], 120)
        for field, value in (
            ("write_busy_ns", 121),
            ("write_busy_ns", 101),
            ("write_busy_ns", 69),
            ("write_max_service_ns", -1),
            ("write_busy_ns", None),
        ):
            changed = copy.deepcopy(row)
            changed[field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(
                ValueError, "write"
            ):
                validate(changed)
        changed = copy.deepcopy(row)
        changed.pop("write_busy_ns")
        with self.assertRaisesRegex(ValueError, "write"):
            validate(changed)

    def test_timing_only_does_not_fabricate_write_intervals(self):
        row = measurement()
        row.update(timing_only=True, write_busy_ns=None, write_max_service_ns=None)
        validate(row)
        self.assertIsNone(summarize([row])["write_busy_ns"])
        row["write_busy_ns"] = 0
        with self.assertRaisesRegex(ValueError, "write"):
            validate(row)

    def test_flush_intervals_are_ordered_and_accounted(self):
        row = measurement()
        row["flush_service_ns"] = 29
        with self.assertRaisesRegex(ValueError, "flush"):
            validate(row)
        row = measurement()
        row["requests"][0]["covering_end"] = 16384
        row.update(flushes=2, flush_times=[[30, 60], [50, 70]], flush_service_ns=50)
        with self.assertRaisesRegex(ValueError, "flush"):
            validate(row)

    def test_aggregate_records_completed_work_without_inventing_latency(self):
        row = aggregate_measurement()
        validate(row)
        result = summarize([row])
        self.assertEqual((result["completed"], result["latency_samples"]), (2, 0))
        self.assertEqual(
            result["latency_ns"], {"p50": None, "p99": None, "p99_9": None}
        )
        self.assertIsNone(result["arrival_lateness_p99_ns"])
        self.assertIsNone(result["admission_p99_ns"])
        self.assertIsNone(result["notification_p99_ns"])
        self.assertEqual(result["physical_flushes"], 1)
        self.assertEqual(result["flush_service_ns"], 30)

    def test_aggregate_rejects_fabricated_timing_and_incomplete_work(self):
        for field, value in (("terminal", 0), ("latency_ns", 50), ("timeout", True)):
            row = aggregate_measurement()
            row["requests"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(row)
        row = aggregate_measurement()
        row["case"] = "paced"
        with self.assertRaises(ValueError):
            validate(row)

    def test_observation_profiles_cannot_be_combined_or_compared(self):
        left, right = measurement(), aggregate_measurement()
        left["affinity_cpus"] = [4]
        with self.assertRaisesRegex(ValueError, "observation profiles"):
            summarize([left, right])
        with self.assertRaisesRegex(ValueError, "comparison changed"):
            compare([left], [right])

    def test_observed_affinity_is_validated_and_part_of_pair_identity(self):
        for mask in (None, [], [4, 4], [-1], [True], ["4"]):
            row = measurement()
            row["affinity_cpus"] = mask
            with self.subTest(mask=mask), self.assertRaisesRegex(
                ValueError, "affinity"
            ):
                validate(row)
        row = aggregate_measurement()
        changed = copy.deepcopy(row)
        changed["affinity_cpus"] = [5]
        with self.assertRaisesRegex(ValueError, "comparison changed"):
            compare([row], [changed])

    def test_profile_and_completed_line_required(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            row = measurement()
            path.write_text(PREFIX + json.dumps(row) + "\n")
            with self.assertRaisesRegex(ValueError, "build profile"):
                read_measurements(path)
            profile = "kwaque-benchmark-profile-v1 " + " ".join(
                f"{k}={v}" for k, v in row["build_profile"].items()
            )
            path.write_text(profile + "\n" + PREFIX + json.dumps(row) + "\n")
            self.assertEqual(read_measurements(path)[0]["encoded_bytes"], 16384)

    def test_fixed_pairs_reject_changed_contracts(self):
        original = measurement()
        for key, value in [
            ("sha256", "c" * 64),
            ("preallocated", False),
            ("device", 2),
            ("foreground_probe", True),
            ("memory_observed", True),
            ("timing_only", True),
            ("comparison_id", "d" * 64),
            ("case", "burst"),
        ]:
            with self.subTest(key=key):
                changed = copy.deepcopy(original)
                changed[key] = value
                with self.assertRaises(ValueError):
                    compare([original], [changed])
        changed = copy.deepcopy(original)
        changed["requests"][0]["covering_end"] = 16384
        with self.assertRaises(ValueError):
            compare([original], [changed])
        changed = copy.deepcopy(original)
        changed["build_profile"]["optimized"] = "false"
        with self.assertRaises(ValueError):
            compare([original], [changed])

    def test_counts_and_notification_boundary(self):
        row = measurement()
        validate(row)
        row["requests"][0]["terminal"] = 59
        with self.assertRaisesRegex(ValueError, "notification"):
            validate(row)
        row = measurement()
        row["offered"] = 3
        with self.assertRaisesRegex(ValueError, "offered"):
            validate(row)

    def test_omitted_and_timed_out_requests_remain_in_denominator(self):
        row = measurement()
        row["case"] = "burst"
        row["requests"][0]["timeout"] = True
        row["requests"].append(
            {
                "accepted": False,
                "timeout": False,
                "queue_ns": 90,
                "admission_ns": 3,
                "latency_ns": 93,
                "terminal": 93,
                "file": 0,
                "end": 0,
                "covering_end": 0,
            }
        )
        row["offered"] = 3
        validate(row)
        result = summarize([row])
        self.assertEqual(
            (
                result["offered"],
                result["accepted"],
                result["rejected"],
                result["timeouts"],
            ),
            (3, 2, 1, 1),
        )
        self.assertEqual(result["arrival_lateness_p99_ns"], 90)
        self.assertEqual(result["latency_samples"], 1)
        self.assertFalse(result["one_per_thousand_tail_samples_available"])

    def test_nearest_rank_and_paired_ratio(self):
        self.assertEqual(percentile(list(range(1, 1001)), 0.999), 999)
        other = measurement()
        other["elapsed_ns"] = 150
        self.assertEqual(
            compare([measurement()], [other])["median_elapsed_ratio_right_over_left"],
            1.5,
        )

    def test_timing_profile_does_not_invent_native_components(self):
        row = measurement()
        row["timing_only"] = True
        row["flush_times"] = []
        validate(row)
        result = summarize([row])
        self.assertIsNone(result["native_calls"])
        self.assertIsNone(result["notification_p99_ns"])
        self.assertIsNone(result["batch_wait_ns"])

    def test_fixed_pairs_reject_different_valid_group_partitions(self):
        row = measurement()
        row["encoded_bytes"] = 32768
        for request, end in zip(row["requests"], (24576, 40960), strict=True):
            request.update(end=end, covering_end=40960)
        changed = copy.deepcopy(row)
        changed["requests"][0]["end"] = 16384
        validate(row)
        validate(changed)
        with self.assertRaisesRegex(ValueError, "comparison changed"):
            compare([row], [changed])

    def test_group_extents_and_covered_boundaries_are_checked(self):
        for key, value in (("end", 16385), ("end", 8192), ("covering_end", 32768)):
            row = measurement()
            row["requests"][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate(row)
        row = measurement()
        row["encoded_bytes"] += 8192
        with self.assertRaisesRegex(ValueError, "byte count"):
            validate(row)

    def test_arrival_lateness_is_separate_from_service_time(self):
        row = measurement()
        for request in row["requests"]:
            request["arrival_lateness_ns"] = request.pop("queue_ns")
        validate(row)
        result = summarize([row])
        self.assertEqual(
            result["service_from_submit_ns"], {"p50": 80, "p99": 90, "p99_9": 90}
        )
        self.assertEqual(result["arrival_lateness_p99_ns"], 5)
        self.assertEqual(result["elapsed_ns"]["samples"], [100])

    def test_covering_cuts_cannot_move_backwards(self):
        row = measurement()
        row.update(
            offered=3, accepted=3, encoded_bytes=24576, flushes=2, timing_only=True
        )
        third = copy.deepcopy(row["requests"][1])
        third.update(end=32768, covering_end=32768)
        row["requests"].append(third)
        row["requests"][0]["covering_end"] = 32768
        with self.assertRaisesRegex(ValueError, "ordered group boundary"):
            validate(row)

    def test_frozen_cohorts_partition_every_accepted_group(self):
        row = measurement()
        row.update(
            offered=3, accepted=3, encoded_bytes=24576, flushes=2, timing_only=True
        )
        third = copy.deepcopy(row["requests"][1])
        third.update(end=32768, covering_end=32768)
        row["requests"].append(third)
        validate(row)
        # The first cut includes group two. That group cannot instead belong
        # to the next cohort, even though all cuts remain monotone and aligned.
        row["requests"][1]["covering_end"] = 32768
        with self.assertRaisesRegex(ValueError, "cohort membership"):
            validate(row)


if __name__ == "__main__":
    unittest.main()
