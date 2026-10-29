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
        self.assertIsNone(summary["arrival_to_result_ns"]["p999"])

    def test_a_tail_counts_the_barriers_its_requests_took(self):
        # A thousand requests pooled over runs are not a qualified tail when
        # they took fewer independent barriers than the rule asks for.
        rows = [observed() for _ in range(500)]
        summary = report.summarize(rows)
        self.assertEqual(summary["arrival_to_result_ns"]["samples"], 1000)
        self.assertEqual(
            summary["arrival_to_result_ns"]["groups"],
            sum(row["wal_flushes"] for row in rows),
        )
        self.assertFalse(summary["arrival_to_result_ns"]["p999_qualified"])
        for row in rows:
            row["wal_flushes"] = row["wal_groups"] = row["footers"] = 2
        rows += [copy.deepcopy(rows[0]) for _ in range(1000)]
        summary = report.summarize(rows)
        self.assertEqual(summary["arrival_to_result_ns"]["groups"], 3000)
        self.assertTrue(summary["arrival_to_result_ns"]["p999_qualified"])
        self.assertEqual(summary["arrival_to_result_ns"]["p999"], 450)

    def test_a_claimed_barrier_reaches_the_device(self):
        row = measurement()
        row.update(
            wal_barrier="extending",
            data_barrier="extending",
            reactor_flushes=2,
            reactor_writes=3,
            reactor_write_bytes=12288,
            reactor_reads=0,
            reactor_read_bytes=0,
        )
        report.validate(row)
        for mutate in (
            lambda r: r.update(reactor_flushes=0),
            lambda r: r.update(wal_barrier="flushed"),
            lambda r: r.pop("reactor_reads"),
            lambda r: r.update(reactor_writes=-1),
        ):
            changed = copy.deepcopy(row)
            mutate(changed)
            with self.assertRaises((ValueError, KeyError)):
                report.validate(changed)

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

    def test_placed_scopes_keep_their_own_cut(self):
        segment = placed("segment_placed")
        report.validate(segment)
        report.validate(placed("wal_placed"))
        for mutate in (
            lambda r: r.update(wal_groups=1, wal_flushes=1),
            lambda r: r.update(footers=2),
            lambda r: r.update(ack_ns=0),
            lambda r: r.update(ack_ns=r["elapsed_ns"] + 1),
        ):
            row = copy.deepcopy(segment)
            mutate(row)
            with self.assertRaises(ValueError):
                report.validate(row)
        with self.assertRaises(ValueError):
            report.validate(placed("wal_placed") | {"wal_groups": 0, "wal_flushes": 0})

    def test_placement_compares_equal_segment_work(self):
        baseline = placed("baseline")
        segment = placed("segment_placed") | {
            "ack_ns": 500,
            "elapsed_ns": 500,
            "reactor_write_bytes": 4096,
        }
        wal = placed("wal_placed") | {"ack_ns": 250, "elapsed_ns": 900}
        held = placed("wal_held") | {"ack_ns": 200, "elapsed_ns": 800}
        result = report.placement([baseline, segment, wal, held])
        self.assertAlmostEqual(
            result[0]["wal_held"]["ack_ns_per_request_to_baseline"], 0.2
        )
        with self.assertRaises(ValueError):
            report.placement([baseline, held | {"wal_digest": "c" * 32}])
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(
            result[0]["segment_placed"]["ack_ns_per_request_to_baseline"], 0.5
        )
        self.assertAlmostEqual(
            result[0]["segment_placed"]["device_write_bytes_to_baseline"], 0.5
        )
        self.assertAlmostEqual(
            result[0]["wal_placed"]["ack_ns_per_request_to_baseline"], 0.25
        )
        self.assertAlmostEqual(
            result[0]["wal_placed"]["elapsed_ns_per_request_to_baseline"], 0.9
        )
        for scope, mutate in (
            ("segment_placed", lambda r: r.update(segment_digest=["c" * 32])),
            ("segment_placed", lambda r: r.update(child_bytes=401)),
            ("wal_placed", lambda r: r.update(wal_digest="c" * 32)),
        ):
            changed = copy.deepcopy(segment if scope == "segment_placed" else wal)
            mutate(changed)
            with self.assertRaises(ValueError):
                report.placement([baseline, changed])
        unmeasured = copy.deepcopy(baseline)
        del unmeasured["ack_ns"]
        with self.assertRaises(ValueError):
            report.placement([unmeasured, segment])


class SynchronizedWritesTest(unittest.TestCase):
    def test_a_synchronized_barrier_needs_writes_or_a_flush(self):
        report.validate(synchronized())
        report.validate(synchronized("segment_placed"))
        for mutate in (
            lambda r: r.update(wal_synchronized_writes=0),
            lambda r: r["data"].update(synchronized_calls=0),
            lambda r: r["data"].update(synchronized_calls=5),
            lambda r: r.update(wal_barrier="extending", data_barrier="extending"),
        ):
            row = synchronized()
            mutate(row)
            with self.assertRaises(ValueError):
                report.validate(row)

    def test_writes_compares_zero_written_against_growing_files(self):
        growing = placed("baseline")
        zeroed = synchronized() | {"ack_ns": 400, "elapsed_ns": 500}
        result = report.writes([growing, zeroed])
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(
            result[0]["zero_written"]["ack_ns_per_request_to_growing"], 0.4
        )
        self.assertEqual(result[0]["zero_written"]["data_synchronized_writes"], 1)
        changed = copy.deepcopy(zeroed)
        changed["segment_digest"] = ["c" * 32]
        with self.assertRaises(ValueError):
            report.writes([growing, changed])
        # Placement keeps the two kinds of file apart.
        self.assertEqual(report.placement([growing, zeroed]), [])


def synchronized(scope="baseline"):
    row = placed(scope)
    row.update(
        wal_barrier="synchronized",
        data_barrier="synchronized",
        wal_flushes=0,
        wal_synchronized_writes=1,
        reactor_flushes=0,
    )
    row["wal"] = kind([]) | {"calls": 1, "synchronized_calls": 1}
    row["data"] = kind([]) | {"calls": 1, "synchronized_calls": 1}
    if scope == "segment_placed":
        row.update(wal_synchronized_writes=0)
        row["wal"] = kind([])
    return row


def placed(scope):
    row = measurement(scope)
    row.update(
        case="small_one",
        ack_ns=1000,
        wal_barrier="extending",
        data_barrier="extending",
        reactor_flushes=2,
        reactor_writes=2,
        reactor_write_bytes=8192,
        reactor_reads=0,
        reactor_read_bytes=0,
    )
    if scope == "segment_placed":
        row.update(wal_groups=0, wal_flushes=0, wal_writes=0, reactor_flushes=1)
        row["wal"] = kind([])
    return row


if __name__ == "__main__":
    unittest.main()
