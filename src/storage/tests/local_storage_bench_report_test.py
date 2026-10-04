import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import local_storage_bench_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}


def kind(flushes):
    return {
        "calls": len(flushes),
        "bytes": 4096 * len(flushes),
        "flushes": len(flushes),
        "depth": 1,
        "flush_depth": 1,
        "write_service_ns": 10 * len(flushes),
        "write_busy_ns": 10 * len(flushes),
        "flush_service_ns": sum(end - begin for begin, end in flushes),
        "cap_waits": 0,
        "cap_wait_ns": 0,
        "flush_times_complete": True,
        "flush_times": flushes,
    }


def measurement(scope="candidate", arrival="held", timing=False):
    row = {key: 0 for key in report.COUNTERS}
    row.update({key: False for key in report.FLAGS})
    row.update(
        scope=scope,
        case="tiny_group",
        arrival=arrival,
        observation="aggregate",
        comparison_id="",
        topology="shared",
        fdatasync="aio",
        wal_digest="a" * 32,
        segment_digest=["b" * 32],
        requests=2,
        segments=1,
        child_bytes=400,
        elapsed_ns=1000,
        reactor_cpu_ns=800,
        allocations=10,
        tasks=5,
        wal_groups=1,
        wal_flushes=1,
        wal_writes=1,
        footers=1,
        blocks=2,
        complete=True,
        timing_only=timing,
        samples=[],
        affinity_cpus=[4],
        build_profile=dict(PROFILE),
    )
    if not timing:
        row.update(wal=kind([[100, 300]]), data=kind([[200, 400]]), other=kind([]))
    return row


def observed(arrival="burst"):
    row = measurement(arrival=arrival)
    row.update(
        observation="requests",
        samples=[[0, 1, 5, 400], [0, 2, 6, 450]],
    )
    return row


class LocalStorageReportTest(unittest.TestCase):
    def test_reads_effective_profile_and_complete_rows(self):
        row = measurement()
        profile = row.pop("build_profile")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            path.write_text(
                "kwaque-benchmark-profile-v1 "
                + " ".join(f"{key}={value}" for key, value in profile.items())
                + "\n"
                + report.PREFIX
                + json.dumps(row)
                + "\n"
            )
            self.assertEqual(
                report.read_measurements(path)[0]["build_profile"], profile
            )
            path.write_text("benchmark started but failed before completion\n")
            with self.assertRaises(ValueError):
                report.read_measurements(path)

    def test_fixed_cuts_and_barriers_cannot_be_dropped(self):
        report.validate(measurement())
        report.validate(measurement(timing=True))
        for mutate in (
            lambda r: r.update(wal_groups=2),
            lambda r: r.update(footers=2),
            lambda r: r.update(blocks=1),
            lambda r: r.update(wal_flushes=0),
            lambda r: r.update(segment_digest=[]),
            lambda r: r.update(complete=False),
            lambda r: r["wal"].update(flushes=2),
            lambda r: r["data"]["flush_times"].append([500, 600]),
            lambda r: r["data"].update(cap_waits=1),
            lambda r: r.update(topology="separate"),
        ):
            row = measurement()
            mutate(row)
            with self.assertRaises(ValueError):
                report.validate(row)
        serial = measurement(arrival="serial")
        serial.update(wal_groups=2, wal_flushes=2, footers=2)
        serial["wal"] = kind([[0, 100], [200, 300]])
        serial["data"] = kind([[50, 150], [250, 350]])
        report.validate(serial)

    def test_composed_paths_and_timing_only_profiles_are_bounded(self):
        with self.assertRaises(ValueError):
            report.validate(observed() | {"scope": "baseline"})
        timed = measurement(timing=True)
        timed["wal"] = kind([[100, 300]])
        with self.assertRaises(ValueError):
            report.validate(timed)

    def test_request_samples_follow_both_stages(self):
        row = observed()
        report.validate(row)
        for mutate in (
            lambda r: r.update(samples=r["samples"][:1]),
            lambda r: r["samples"][0].__setitem__(3, 4),
            lambda r: r.update(observation="aggregate"),
        ):
            changed = copy.deepcopy(row)
            mutate(changed)
            with self.assertRaises(ValueError):
                report.validate(changed)
        summary = report.summarize([row])
        self.assertEqual(summary["arrival_to_result_ns"]["p50"], 400)
        self.assertEqual(summary["accepted_stage_ns"]["p99"], 4)
        self.assertFalse(summary["arrival_to_result_ns"]["p999_qualified"])

    def test_barrier_components_report_their_overlap(self):
        summary = report.summarize([measurement()])
        native = summary["native"]
        self.assertEqual(native["wal_flush_busy_ns"], 200)
        self.assertEqual(native["data_flush_busy_ns"], 200)
        self.assertEqual(native["overlapped_flush_ns"], 100)
        self.assertEqual(summary["footers_per_request"], 0.5)

    def test_pairs_require_equal_stored_work(self):
        baseline = measurement("baseline")
        candidate = measurement("candidate") | {"elapsed_ns": 1100}
        sequential = measurement("sequential") | {"elapsed_ns": 1500}
        result = report.pairs([baseline, candidate, sequential])
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0]["candidate_to_baseline_elapsed"], 1.1)
        self.assertAlmostEqual(result[0]["sequential_to_baseline_elapsed"], 1.5)
        for mutate in (
            lambda r: r.update(wal_digest="c" * 32),
            lambda r: r.update(segment_digest=["c" * 32]),
            lambda r: r.update(footers=2),
        ):
            changed = copy.deepcopy(candidate)
            mutate(changed)
            with self.assertRaises(ValueError):
                report.pairs([baseline, changed])


if __name__ == "__main__":
    unittest.main()
