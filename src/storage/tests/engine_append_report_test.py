import copy
import json
import random
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import engine_append_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}


def bucket_of(value):
    """The bucket a value counts in, as the histogram finds it."""
    if value >= report.MAXIMUM_NS:
        return report.LAST_BUCKET
    if value <= report.MINIMUM_NS:
        return 0
    top = value.bit_length() - 1
    return ((top - 10) << 9) + ((value >> (top - 9)) & 511)


def histogram(values, interval=0):
    counts = {}
    for value in values:
        counts[bucket_of(value)] = counts.get(bucket_of(value), 0) + 1
    return {
        "minimum_ns": report.MINIMUM_NS,
        "maximum_ns": report.MAXIMUM_NS,
        "precision": report.PRECISION,
        "period_ns": report.PERIOD_NS,
        "total": len(values),
        "buckets": [
            [
                interval,
                report.bucket_lower(index),
                None if index == report.LAST_BUCKET else report.bucket_lower(index + 1),
                count,
            ]
            for index, count in sorted(counts.items())
        ],
    }


def kind(**changes):
    counts = {key: 0 for key in report.KIND_COUNTERS}
    counts.update(changes)
    return counts


def device(store, st_dev, pool, role):
    return {
        "store": format(store, "02x") * 16,
        "st_dev": st_dev,
        "pool": pool,
        "role": role,
    }


LATENCIES = [400_000 + 1_000 * i for i in range(256)]
DELAYS = [10 * i for i in range(256)]


def measurement(case="quiet", timing=False):
    beside = report.CASES[case]
    row = {key: 0 for key in report.COUNTERS}
    row.update(
        case=case,
        beside=beside,
        fdatasync="aio",
        group_target=None,
        wal_barrier="extending",
        data_barrier="extending",
        requests=256,
        completed=256,
        request_bound=256,
        spacing_ns=1_000_000,
        targets=2,
        groups=200,
        wal_capacity_bytes=128 << 20,
        share_bytes=1 << 30,
        elapsed_ns=300_000_000,
        reactor_cpu_ns=200_000_000,
        allocations=5_000,
        tasks=9_000,
        runtime_operations=10,
        reactor_flushes=400,
        reactor_writes=400,
        reactor_write_bytes=400 * 8192,
        segment_placed_portions=200,
        segment_placed_bytes=200 * 8192,
        segment_barriers=200,
        devices=[device(0x33, 10, None, "wal_control"), device(0x44, 10, 0, "data")],
        target_devices=[1, 1],
        same_filesystem=True,
        alignment_bytes=8192,
        segment_bytes=64 << 20,
        synchronous_write_bytes=1 << 20,
        settle_ms=0,
        fresh_segments=False,
        block=None,
        timing_only=timing,
        ndebug=True,
        libcxx_hardening_none=True,
        complete=True,
        latency=histogram(LATENCIES),
        delay=histogram(DELAYS),
        affinity_cpus=[4],
        build_profile=dict(PROFILE),
    )
    if case == "two_data_devices":
        row["devices"].append(device(0x55, 10, 0, "data"))
        row["target_devices"] = [1, 2]
    if case == "exhausted_share":
        row["share_bytes"] = 0
    if case == "fixed_rate":
        row.update(group_target=200, request_bound=7680, spacing_ns=4_000_000)
    if beside != "none":
        row.update(
            beside_at=128,
            beside_begin_ns=128_000_000,
            beside_end_ns=140_000_000,
            overlapping=20,
            overlap_groups=15,
            overlap=histogram(LATENCIES[128:148]),
        )
    if beside in ("seal", "removal"):
        row.update(
            beside_outcome="sealed",
            beside_fill_bytes=33_000_000,
            sealed_file_end=35_000_000,
            sealed_records=256,
        )
    if beside == "removal":
        row.update(
            beside_outcome="gone",
            verdict="authorized",
            gone=True,
            settling=False,
            removed=5,
            foreign=0,
        )
    if beside == "creation":
        row.update(beside_outcome="resource_exhausted", table_size=2)
    if not timing:
        removal = beside == "removal"
        row.update(
            wal=kind(writes=200, write_bytes=200 * 8192, flushes=200),
            data=kind(
                writes=200,
                write_bytes=200 * 16384,
                flushes=200,
                unlinks=1 if removal else 0,
                unlink_ns=50_000 if removal else 0,
                unlink_longest_ns=50_000 if removal else 0,
            ),
            other=kind(),
            namespace={
                key: 1 if removal and key == "renames" else 0
                for key in report.NAMESPACE_COUNTERS
            },
        )
    if case == "rolling":
        row.update(
            wal_barrier="synchronized",
            data_barrier="synchronized",
            reactor_flushes=0,
            fresh_segments=True,
            rolls=8,
            roll_creation_ns=80_000_000,
            roll_creation_longest_ns=15_000_000,
        )
        if not timing:
            row["wal"].update(flushes=0, synchronized_calls=200)
            row["data"].update(flushes=0, synchronized_calls=200)
    return row


class EngineAppendReportTest(unittest.TestCase):
    def assertRefused(self, mutate, case="quiet", timing=False):
        row = measurement(case, timing)
        mutate(row)
        with self.assertRaises((ValueError, KeyError, TypeError)):
            report.validate(row)

    def test_reads_effective_profile_and_complete_rows(self):
        row = measurement()
        profile = row.pop("build_profile")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            line = "allocator=native abort=true injection=false optimized=true "
            line += "sanitized=false asan=false ubsan=false"
            path.write_text(
                line + "\n" + line + "\n" + report.PREFIX + json.dumps(row) + "\n"
            )
            self.assertEqual(
                report.read_measurements(path)[0]["build_profile"], profile
            )
            path.write_text(
                line.replace("optimized=true", "optimized=false") + "\n" + line + "\n"
            )
            with self.assertRaises(ValueError):
                report.read_measurements(path)
            path.write_text("benchmark started but failed before completion\n")
            with self.assertRaises(ValueError):
                report.read_measurements(path)

    def test_every_case_validates_in_both_builds(self):
        for case in report.CASES:
            for timing in (False, True):
                # A rolling stream's files are zero-written, which only the
                # observing build counts.
                if case == "rolling" and timing:
                    continue
                report.validate(measurement(case, timing))
        # Two data devices on two filesystems are two pools.
        row = measurement("two_data_devices")
        row["devices"][2].update(st_dev=11, pool=1)
        row["same_filesystem"] = False
        report.validate(row)
        # A removal that settles instead of going.
        row = measurement("during_removal")
        row.update(gone=False, settling=True, beside_outcome="settling")
        report.validate(row)

    def test_the_field_set_is_exact(self):
        self.assertRefused(lambda r: r.pop("groups"))
        self.assertRefused(lambda r: r.update(extra=1))
        self.assertRefused(lambda r: r.update(beside_at=128))
        self.assertRefused(lambda r: r.update(beside="seal"))
        self.assertRefused(lambda r: r.pop("sealed_file_end"), "during_seal")
        self.assertRefused(lambda r: r.update(table_size=2), "during_seal")
        self.assertRefused(lambda r: r.pop("overlap"), "exhausted_share")
        self.assertRefused(lambda r: r.update(wal=kind()), timing=True)
        self.assertRefused(lambda r: r.pop("namespace"))
        self.assertRefused(lambda r: r.update(case="burst"))

    def test_requests_barriers_and_plan(self):
        self.assertRefused(lambda r: r.update(completed=255))
        self.assertRefused(lambda r: r.update(groups=0))
        self.assertRefused(lambda r: r.update(groups=257))
        self.assertRefused(lambda r: r.update(reactor_flushes=199))
        self.assertRefused(lambda r: r.update(reactor_writes=0))
        self.assertRefused(lambda r: r["wal"].update(flushes=201))
        self.assertRefused(lambda r: r.update(reactor_flushes=0), timing=True)
        self.assertRefused(lambda r: r.update(wal_barrier="preallocated"))
        self.assertRefused(lambda r: r.update(request_bound=200))
        self.assertRefused(lambda r: r.update(request_bound=512))
        self.assertRefused(lambda r: r.update(group_target=200))
        self.assertRefused(lambda r: r.update(group_target=None), "fixed_rate")
        self.assertRefused(lambda r: r.update(group_target=201), "fixed_rate")
        # Bounded by the room before the target.
        row = measurement("fixed_rate")
        row.update(group_target=250, request_bound=256)
        report.validate(row)
        self.assertRefused(lambda r: r.update(share_bytes=0))
        self.assertRefused(lambda r: r.update(share_bytes=1), "exhausted_share")
        self.assertRefused(lambda r: r.update(complete=False))
        self.assertRefused(lambda r: r.update(elapsed_ns=0))
        self.assertRefused(lambda r: r["build_profile"].pop("asan"))
        self.assertRefused(lambda r: r.update(affinity_cpus=[]))

    def test_barriers_follow_the_write_profile(self):
        # Every portion in its segment: the WAL takes no write and no flush.
        row = measurement()
        row["wal"] = kind()
        row["reactor_flushes"] = 200
        report.validate(row)

        def synchronized(row):
            row.update(wal_barrier="synchronized", data_barrier="synchronized")
            row["wal"].update(flushes=0, synchronized_calls=200)
            row["data"].update(flushes=0, synchronized_calls=200)
            row["reactor_flushes"] = 0
            return row

        def silent(row):
            synchronized(row)
            row["wal"]["synchronized_calls"] = row["data"]["synchronized_calls"] = 0

        report.validate(synchronized(measurement()))
        self.assertRefused(silent)
        self.assertRefused(lambda r: r.update(wal_barrier="synchronized"))
        self.assertRefused(
            lambda r: r.update(wal_barrier="synchronized", data_barrier="synchronized"),
            timing=True,
        )
        # A synchronized row and one that grew its files are two summaries.
        summaries = report.summaries([measurement(), synchronized(measurement())])
        self.assertEqual(
            sorted(one["barrier"] for one in summaries), ["extending", "synchronized"]
        )
        held = next(one for one in summaries if one["barrier"] == "synchronized")
        self.assertEqual(held["synchronized_writes_per_group"], 2)

    def test_summary_reads_device_bytes_and_write_rate(self):
        row = measurement()
        row.update(payload_bytes=8192)
        summary = report.summaries([row])[0]
        # 400 writes of 8 KiB for 256 requests of 8 KiB, over 0.3 s.
        self.assertEqual(summary["device_bytes_per_payload_byte"], 400 / 256)
        self.assertAlmostEqual(summary["writes_per_second"], 400 / 0.3)
        self.assertIsNone(
            report.summaries([measurement()])[0]["device_bytes_per_payload_byte"]
        )
        self.assertIsNone(
            report.summaries([measurement(timing=True)])[0][
                "synchronized_writes_per_group"
            ]
        )

    def test_rolling_stream(self):
        report.validate(measurement("rolling"))
        # Only the rolling case rolls, and its segments are new files.
        self.assertRefused(lambda r: r.update(rolls=1, roll_creation_ns=1))
        self.assertRefused(lambda r: r.update(fresh_segments=False), "rolling")
        self.assertRefused(lambda r: None, "rolling", timing=True)
        # Each roll's creation took time, the longest one of them.
        self.assertRefused(lambda r: r.update(roll_creation_ns=0), "rolling")
        self.assertRefused(
            lambda r: r.update(roll_creation_longest_ns=90_000_000), "rolling"
        )
        # A segment holds a whole number of alignment units.
        self.assertRefused(lambda r: r.update(segment_bytes=0))
        self.assertRefused(lambda r: r.update(segment_bytes=(64 << 20) + 4096))

    def test_alignment_fresh_segments_and_block_device(self):
        def synchronized(row):
            row.update(wal_barrier="synchronized", data_barrier="synchronized")
            row["wal"].update(flushes=0, synchronized_calls=200)
            row["data"].update(flushes=0, synchronized_calls=200)
            row["reactor_flushes"] = 0
            return row

        fresh = synchronized(measurement())
        fresh["fresh_segments"] = True
        report.validate(fresh)
        # Fresh segments are made zero-written.
        self.assertRefused(lambda r: r.update(fresh_segments=True))
        self.assertRefused(lambda r: r.update(alignment_bytes=0))
        self.assertRefused(lambda r: r.update(alignment_bytes=2048))
        self.assertRefused(lambda r: r.update(alignment_bytes=12288))
        self.assertRefused(lambda r: r.update(synchronous_write_bytes=0))
        aligned = measurement()
        aligned["alignment_bytes"] = 4096
        report.validate(aligned)
        # Another alignment or fresh segments are summaries of their own.
        summaries = report.summaries(
            [measurement(), aligned, fresh, synchronized(measurement())]
        )
        self.assertEqual(len(summaries), 4)

        block = {
            "device": "nvme0n1",
            "reads": 0,
            "read_bytes": 0,
            "writes": 512,
            "write_bytes": 512 * 16384,
            "flushes": 256,
        }
        counted = measurement()
        counted.update(payload_bytes=8192, block=dict(block))
        report.validate(counted)
        summary = report.summaries([counted])[0]["block"]
        self.assertEqual(summary["device"], ["nvme0n1"])
        # 8 MiB on the device for 256 requests of 8 KiB.
        self.assertEqual(summary["write_bytes_per_payload_byte"], 4)
        self.assertEqual(summary["writes_per_request"], 2)
        self.assertEqual(summary["flushes_per_request"], 1)
        self.assertIsNone(report.summaries([measurement()])[0]["block"])
        # A run that named no device leaves the pooled counts out.
        self.assertIsNone(report.block_summary([counted, measurement()]))
        self.assertRefused(lambda r: r.update(block={**block, "device": ""}))
        self.assertRefused(lambda r: r.update(block={**block, "writes": 0}))
        self.assertRefused(lambda r: r.update(block={**block, "flushes": -1}))
        self.assertRefused(lambda r: r.update(block={**block, "extra": 1}))
        self.assertRefused(lambda r: r.pop("block"))

    def test_placement_follows_threshold_and_hold(self):
        def placed(row):
            row.update(
                placement_bytes=32768,
                hold_bytes=16384,
                wal_placed_portions=150,
                segment_placed_portions=50,
                held_portions=120,
                runs_filled=4,
                runs_wanted=40,
                runs_synced=2,
                sync_rounds=1,
                round_syncs=2,
                held_runs=1,
                held_bytes=4096,
            )
            return row

        report.validate(placed(measurement()))
        self.assertRefused(
            lambda r: r.update(segment_placed_portions=0, segment_barriers=0)
        )
        self.assertRefused(lambda r: r.update(wal_placed_portions=1))
        self.assertRefused(lambda r: placed(r).update(held_portions=151))
        self.assertRefused(lambda r: placed(r).update(hold_bytes=0))
        self.assertRefused(lambda r: placed(r).update(held_bytes=0))
        self.assertRefused(lambda r: placed(r).update(held_runs=4))
        self.assertRefused(lambda r: placed(r).update(round_syncs=0))
        # Rows at two thresholds are two summaries.
        rows = [measurement(), placed(measurement())]
        summaries = report.summaries(rows)
        self.assertEqual(len(summaries), 2)
        self.assertEqual(
            sorted(one["placement_bytes"] for one in summaries), [0, 32768]
        )
        held = next(one for one in summaries if one["placement_bytes"])
        self.assertEqual(held["wal_placed_share"], 0.75)
        self.assertEqual(held["run_closes"]["wanted"], 40)
        self.assertEqual(held["segment_syncs_per_round"], 2)

    def test_histograms_hold_what_the_row_says(self):
        self.assertRefused(lambda r: r["latency"].update(total=255))
        self.assertRefused(lambda r: r["delay"]["buckets"][0].__setitem__(3, 2))
        self.assertRefused(lambda r: r["latency"]["buckets"].reverse())
        self.assertRefused(lambda r: r["latency"]["buckets"][1].__setitem__(1, 1025))
        self.assertRefused(lambda r: r["latency"]["buckets"][1].__setitem__(2, None))
        self.assertRefused(lambda r: r["latency"].update(precision=128))
        self.assertRefused(lambda r: r["latency"]["buckets"][-1].__setitem__(0, 1))
        self.assertRefused(lambda r: r.update(elapsed_ns=500_000))
        self.assertRefused(lambda r: r["delay"]["buckets"][0].__setitem__(3, 0))
        # A later interval inside the region is kept apart.
        row = measurement()
        row.update(elapsed_ns=1_500_000_000)
        row["latency"]["buckets"][-1][0] = 1
        report.validate(row)

    def test_devices_are_the_tables(self):
        self.assertRefused(
            lambda r: r.update(target_devices=[1, 1]), "two_data_devices"
        )
        self.assertRefused(
            lambda r: r.update(same_filesystem=False), "two_data_devices"
        )
        self.assertRefused(lambda r: r["devices"][2].update(pool=1), "two_data_devices")
        self.assertRefused(
            lambda r: r["devices"][2].update(st_dev=11), "two_data_devices"
        )
        self.assertRefused(lambda r: r["devices"].append(device(0x55, 10, 0, "data")))
        self.assertRefused(lambda r: r["devices"].reverse())
        self.assertRefused(lambda r: r.update(target_devices=[0, 1]))
        self.assertRefused(lambda r: r.update(target_devices=[1]))
        self.assertRefused(lambda r: r["devices"][1].update(store="44"))
        self.assertRefused(lambda r: r["devices"][1].update(role="all"))

    def test_what_ran_beside_the_stream(self):
        for case in ("during_seal", "during_removal", "exhausted_share"):
            self.assertRefused(lambda r: r.update(overlapping=257), case)
            self.assertRefused(lambda r: r.update(overlapping=0), case)
            self.assertRefused(lambda r: r.update(overlapping=21), case)
            self.assertRefused(lambda r: r.update(overlap_groups=21), case)
            self.assertRefused(lambda r: r.update(beside_end_ns=400_000_000), case)
            self.assertRefused(lambda r: r.update(beside_begin_ns=150_000_000), case)
            self.assertRefused(lambda r: r.update(beside_at=256), case)
        self.assertRefused(lambda r: r.update(beside_outcome="failed"), "during_seal")
        self.assertRefused(lambda r: r.update(sealed_file_end=1), "during_seal")
        self.assertRefused(lambda r: r.update(sealed_records=0), "during_seal")
        self.assertRefused(lambda r: r.update(verdict="attached"), "during_removal")
        self.assertRefused(lambda r: r.update(settling=True), "during_removal")
        self.assertRefused(
            lambda r: r.update(beside_outcome="settling"), "during_removal"
        )
        self.assertRefused(lambda r: r.update(removed=0), "during_removal")
        self.assertRefused(
            lambda r: r["data"].update(unlinks=0, unlink_ns=0, unlink_longest_ns=0),
            "during_removal",
        )
        self.assertRefused(lambda r: r["namespace"].update(renames=0), "during_removal")
        self.assertRefused(
            lambda r: r.update(beside_outcome="unavailable"), "exhausted_share"
        )
        self.assertRefused(lambda r: r.update(table_size=1), "exhausted_share")
        self.assertRefused(
            lambda r: r["data"].update(unlink_longest_ns=60_000), "during_removal"
        )

    def test_bucket_geometry(self):
        self.assertEqual(report.bucket_lower(0), 1024)
        self.assertEqual(report.bucket_lower(1), 1026)
        self.assertEqual(report.bucket_lower(511), 2046)
        self.assertEqual(report.bucket_lower(512), 2048)
        self.assertEqual(report.bucket_lower(513), 2052)
        self.assertEqual(report.bucket_lower(report.LAST_BUCKET), 1 << 36)
        for index in (0, 1, 511, 512, 513, 7000, report.LAST_BUCKET):
            self.assertEqual(report.bucket_index(report.bucket_lower(index)), index)
        for lower in (1023, 1025, 2049, (1 << 36) + 1, True, "1024"):
            self.assertIsNone(report.bucket_index(lower))
        generator = random.Random(1)
        for _ in range(1000):
            value = generator.randrange(1025, 1 << 36)
            index = bucket_of(value)
            self.assertLessEqual(report.bucket_lower(index), value)
            self.assertLess(value, report.bucket_lower(index + 1))

    def test_summary_gives_bucket_intervals_and_qualifies_tails(self):
        first, second = measurement(), measurement()
        second["latency"] = histogram([2_000_000] * 256)
        second["delay"] = histogram([50] * 256)
        summary = report.summaries([first, second])
        self.assertEqual(len(summary), 1)
        latency = summary[0]["latency_ns"]
        self.assertEqual(latency["values"], 512)
        self.assertEqual(latency["groups"], 400)
        # The 256th of 512 is the first run's largest, 655,000 ns.
        self.assertEqual(latency["p50"], [654_336, 655_360])
        self.assertEqual(latency["p99"], [1_998_848, 2_000_896])
        self.assertIsNone(latency["p999"])
        self.assertFalse(latency["p999_qualified"])
        # The first bucket also holds what is below the range.
        self.assertEqual(summary[0]["delay_ns"]["p50"], [0, 1026])
        self.assertEqual(report.summaries([first])[0]["delay_ns"]["p50"], [1270, 1272])
        first["groups"] = second["groups"] = 1500
        first["reactor_flushes"] = second["reactor_flushes"] = 1500
        qualified = report.summaries([first, second])[0]["latency_ns"]
        self.assertTrue(qualified["p999_qualified"])
        self.assertEqual(qualified["p999"], [1_998_848, 2_000_896])
        seal = report.summaries([measurement("during_seal")])[0]
        self.assertEqual(seal["overlap_ns"]["groups"], 15)
        self.assertEqual(seal["beside_outcomes"], {"sealed": 1})
        self.assertEqual(seal["beside_ns"], 12_000_000)

    def test_compare_sets_one_stream_side_by_side(self):
        rows = [measurement(), measurement("during_seal")]
        result = report.compare(rows, "quiet", "during_seal")
        self.assertEqual(result["runs"], [1, 1])
        self.assertEqual(result["latency_ns"]["p50"][0], result["latency_ns"]["p50"][1])
        self.assertIn("during_seal_overlap_ns", result)
        self.assertNotIn("quiet_overlap_ns", result)
        with self.assertRaises(ValueError):
            report.compare(rows, "quiet", "during_removal")
        other = copy.deepcopy(rows[1])
        other["spacing_ns"] = 2_000_000
        with self.assertRaises(ValueError):
            report.compare([rows[0], other], "quiet", "during_seal")
        with self.assertRaises(ValueError):
            report.compare(rows + [measurement(timing=True)], "quiet", "during_seal")


if __name__ == "__main__":
    unittest.main()
