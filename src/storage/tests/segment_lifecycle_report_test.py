import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import segment_lifecycle_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}
PROFILE_LINE = "kwaque-benchmark-profile-v1 " + " ".join(
    f"{key}={value}" for key, value in PROFILE.items()
)


def kind(**changes):
    values = {key: 0 for key in report.KIND_COUNTS}
    values.update(changes)
    return values


def common(case, **changes):
    values = {
        "case": case,
        "elapsed_ns": 2_000_000,
        "reactor_cpu_ns": 600_000,
        "allocations": 300,
        "tasks": 120,
        "reactor_flushes": 0,
        "reactor_writes": 4,
        "reactor_write_bytes": 32768,
        "reactor_reads": 1,
        "reactor_read_bytes": 4096,
        "wal": kind(),
        "data": kind(),
        "other": kind(),
        "directory_opens": 2,
        "directory_syncs": 0,
        "listings": 0,
        "stats": 1,
        "renames": 0,
        "directory_removes": 0,
        "crc_bulk_calls": 2,
        "crc_bulk_bytes": 16384,
        "digest_calls": 2,
        "digest_bytes": 8192,
        "timing_only": False,
        "ndebug": True,
        "affinity_cpus": [0],
    }
    values.update(changes)
    return values


def seal(case="seal_without_index", indexed=False, **changes):
    """A writer seal: its retry bundle and sealed pointer, and its index
    bundle when it names one, each flushed, renamed and synced; and its
    sealed root flushed."""
    published = report.SEAL_PUBLICATIONS + indexed
    blocks, alignment = 2048, 4096
    if case == "seal_through_table":
        alignment = 8192
    values = common(
        case,
        data=kind(writes=1, write_bytes=8192, flushes=1, opens=0),
        other=kind(writes=published, write_bytes=published * 8192, flushes=published),
        reactor_flushes=2 * published + 1,
        renames=published,
        directory_syncs=published,
        blocks=blocks,
        alignment=alignment,
        indexed=indexed,
        index_pages=2 if indexed else 0,
        index_bundle_bytes=40960 if indexed else 0,
        retry_pages=0,
        retry_bundle_bytes=4096,
        data_file_bytes=blocks * alignment + 3 * alignment,
    )
    values.update(changes)
    return values


def roll(case="roll_ahead", **changes):
    ahead = case == "roll_ahead"
    values = common(
        case,
        wal=kind(writes=1, write_bytes=8192, flushes=1),
        data=kind(writes=1, write_bytes=16384, flushes=1),
        reactor_flushes=2,
        gap_ns=2_000_000,
        baseline_ns=1_900_000,
        first_requests=13,
        first_bytes=262144,
        block_bytes=8192,
        successor_ahead=ahead,
        refused=not ahead,
    )
    if not ahead:
        values.update(
            other=kind(writes=3, write_bytes=24576, flushes=3, opens=3),
            reactor_flushes=11,
            directory_syncs=6,
            renames=3,
        )
    values.update(changes)
    return values


def removal(target=64, **changes):
    values = common(
        f"removal_extents_{target}",
        data=kind(unlinks=1, unlink_ns=40_000, unlink_longest_ns=40_000),
        other=kind(unlinks=4, unlink_ns=30_000, unlink_longest_ns=12_000),
        reactor_flushes=2,
        renames=1,
        directory_syncs=2,
        directory_removes=2,
        listings=2,
        requests=1152,
        extent_target=target,
        holes=target - 1,
        punch_refused=False,
        extents_known=True,
        extents=target + 2,
        fs_block_bytes=4096,
        data_file_bytes=9_502_720,
        data_allocated_bytes=9_502_720 - (target - 1) * 4096,
        removed_files=5,
    )
    values.update(changes)
    return values


def creation(**changes):
    values = common(
        "creation",
        other=kind(writes=3, write_bytes=24576, flushes=3, opens=3),
        data=kind(opens=1, reads=1, read_bytes=8192),
        reactor_flushes=9,
        directory_syncs=6,
        renames=3,
    )
    values.update(changes)
    return values


def every_case():
    return [
        seal(),
        seal("seal_with_index", indexed=True),
        seal("seal_through_table", indexed=True),
        roll("roll_ahead"),
        roll("roll_on_refusal"),
        removal(1, holes=0, extents=1, data_allocated_bytes=9_502_720),
        removal(64),
        removal(1024, extents=1030),
        creation(),
    ]


class SegmentLifecycleReportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def log(self, rows, profile=PROFILE_LINE):
        path = Path(self.directory.name) / "lifecycle.log"
        lines = [profile] if profile else []
        lines += [report.PREFIX + json.dumps(row) for row in rows]
        path.write_text("\n".join(lines) + "\n")
        return path

    def read(self, rows, **kwargs):
        return report.read_measurements(self.log(rows, **kwargs))

    def refused(self, rows, message, **kwargs):
        with self.assertRaisesRegex(ValueError, message):
            self.read(rows, **kwargs)

    def test_every_case_is_read_and_summarized(self):
        rows = self.read(every_case() + [seal(elapsed_ns=1_000_000)])
        summary = report.summarize(rows)
        self.assertEqual(summary["build_profile"], PROFILE)
        self.assertEqual(len(summary["cases"]), 9)
        self.assertEqual(
            summary["index_added"],
            {
                "with_index_ns": 2_000_000,
                "without_index_ns": 1_500_000,
                "difference_ns": 500_000,
            },
        )
        rolled = summary["cases"]["roll_on_refusal"]
        self.assertEqual(rolled["gap_less_baseline_ns"], 100_000)
        removed = summary["cases"]["removal_extents_1024"]
        self.assertEqual(removed["extents"], [1030, 1030])
        self.assertEqual(removed["data_unlink_ns"], 40_000)
        self.assertEqual(summary["cases"]["seal_with_index"]["index_pages"], [2])

    def test_the_old_profile_line_is_read(self):
        old = "allocator=native abort=true injection=false optimized=true " + (
            "sanitized=false asan=false ubsan=false"
        )
        rows = self.read([creation()], profile=old)
        self.assertEqual(rows[0]["build_profile"]["oom_abort"], "true")

    def test_a_row_needs_the_build_profile(self):
        self.refused([creation()], "effective build profile", profile=None)

    def test_conflicting_profiles_are_refused(self):
        path = Path(self.directory.name) / "two.log"
        path.write_text(
            "\n".join(
                [
                    PROFILE_LINE,
                    PROFILE_LINE.replace("optimized=true", "optimized=false"),
                    report.PREFIX + json.dumps(creation()),
                ]
            )
        )
        with self.assertRaisesRegex(ValueError, "conflicting build profile"):
            report.read_measurements(path)

    def test_a_log_needs_a_row(self):
        self.refused([], "no segment lifecycle rows")

    def test_summary_refuses_mixed_profiles(self):
        first = self.read([creation()])
        second = copy.deepcopy(first)
        second[0]["build_profile"]["optimized"] = "false"
        with self.assertRaisesRegex(ValueError, "different build profiles"):
            report.summarize(first + second)

    def test_a_row_names_its_case(self):
        row = creation()
        del row["case"]
        self.refused([row], "row lacks its case")
        self.refused([[creation()]], "row lacks its case")

    def test_unknown_case(self):
        self.refused([common("removal_extents_0")], "unknown lifecycle case")
        self.refused([common("seal")], "unknown lifecycle case")

    def test_every_count_is_needed(self):
        for key in report.COUNTS:
            row = creation()
            del row[key]
            self.refused([row], "row lacks a count")
        row = creation()
        row["tasks"] = -1
        self.refused([row], "row lacks a count")

    def test_a_region_takes_time(self):
        self.refused([creation(elapsed_ns=0)], "took no time")

    def test_every_kind_count_is_needed(self):
        for key in report.KIND_COUNTS:
            row = creation()
            del row["other"][key]
            self.refused([row], "file kind lacks a count")
        row = creation()
        del row["wal"]
        self.refused([row], "file kind lacks a count")

    def test_bytes_need_a_call(self):
        self.refused([creation(wal=kind(write_bytes=1))], "bytes moved by no call")
        self.refused([creation(wal=kind(read_bytes=1))], "bytes moved by no call")

    def test_unlink_times_follow_unlinks(self):
        self.refused([creation(wal=kind(unlink_ns=5))], "unlink time without an unlink")
        row = removal()
        row["other"]["unlink_longest_ns"] = 31_000
        self.refused([row], "longest unlink took more")

    def test_build_flags_and_affinity(self):
        self.refused([creation(timing_only=True)], "timing-only")
        row = creation()
        del row["ndebug"]
        self.refused([row], "build flags")
        for cpus in ([], [0, 0], [-1], "0", [0.5]):
            self.refused([creation(affinity_cpus=cpus)], "CPU affinity")

    def test_a_lifecycle_row_names_no_barrier(self):
        self.refused([creation(barrier="extending")], "names no append barrier")

    def test_the_shard_issues_every_observed_flush_and_sync(self):
        self.refused([creation(reactor_flushes=8)], "fewer flushes")
        self.refused([removal(reactor_flushes=1)], "fewer flushes")
        self.read([creation(reactor_flushes=12)])

    def test_seal_counts_are_needed(self):
        for key in report.SEAL_COUNTS:
            row = seal()
            del row[key]
            self.refused([row], "seal row lacks a count")
        row = seal()
        row["indexed"] = None
        self.refused([row], "seal row lacks a count")

    def test_a_seal_names_an_index_as_its_case_says(self):
        self.refused([seal(indexed=True)], "other than its case says")
        self.refused(
            [seal("seal_through_table", indexed=False)], "other than its case says"
        )

    def test_index_pages_follow_whether_it_was_named(self):
        self.refused([seal(index_pages=1)], "disagree with whether")
        self.refused([seal(index_bundle_bytes=4096)], "disagree with whether")
        self.refused(
            [seal("seal_with_index", indexed=True, index_bundle_bytes=0)],
            "disagree with whether",
        )

    def test_an_indexed_seal_has_more_than_one_page(self):
        self.refused(
            [seal("seal_with_index", indexed=True, blocks=1024, index_pages=1)],
            "blocks' anchors",
        )
        self.refused(
            [seal("seal_through_table", indexed=True, index_pages=3)],
            "blocks' anchors",
        )
        self.read([seal("seal_with_index", indexed=True, blocks=2049, index_pages=3)])

    def test_a_seal_has_its_blocks_and_retry_bundle(self):
        self.refused([seal(retry_bundle_bytes=0)], "retry bundle")
        self.refused([seal(data_file_bytes=4096)], "retry bundle")

    def test_a_seal_flushes_its_root(self):
        self.refused([seal(data=kind(writes=1, write_bytes=8192))], "sealed root")

    def test_a_seal_publishes_what_it_names(self):
        self.refused(
            [seal("seal_with_index", indexed=True, renames=2)], "less than it names"
        )
        self.refused([seal(directory_syncs=1)], "less than it names")

    def test_the_index_is_one_publication(self):
        more = seal(
            "seal_with_index",
            indexed=True,
            renames=4,
            directory_syncs=4,
            reactor_flushes=9,
            other=kind(writes=4, write_bytes=32768, flushes=4),
        )
        self.refused([seal(), more], "one publication more")
        self.read(
            [
                seal(),
                seal("seal_with_index", indexed=True),
                seal("seal_through_table", indexed=True, renames=9),
            ]
        )

    def test_each_writer_seal_publishes_the_same_every_run(self):
        self.refused(
            [seal(), seal(renames=3, directory_syncs=3, reactor_flushes=7)],
            "disagree on their segment",
        )
        self.refused(
            [
                seal(),
                seal(
                    "seal_with_index",
                    indexed=True,
                    blocks=3072,
                    index_pages=3,
                    data_file_bytes=3075 * 4096,
                ),
            ],
            "different sizes",
        )

    def test_roll_counts_are_needed(self):
        for key in (*report.ROLL_COUNTS, "successor_ahead", "refused"):
            row = roll()
            del row[key]
            self.refused([row], "roll row lacks a count")

    def test_a_successor_is_made_as_the_case_says(self):
        self.refused([roll(refused=True)], "made as its case says")
        self.refused(
            [roll("roll_on_refusal", successor_ahead=True)], "as its case says"
        )
        self.refused([roll("roll_on_refusal", refused=False)], "as its case says")

    def test_the_gap_is_the_timed_region(self):
        self.refused([roll(gap_ns=1)], "gap is not its timed region")

    def test_a_roll_has_a_baseline(self):
        self.refused([roll(first_requests=1)], "no baseline")
        self.refused([roll(baseline_ns=0)], "no baseline")

    def test_the_first_segment_holds_what_it_took(self):
        self.refused([roll(first_requests=33)], "more than its bound holds")
        self.refused([roll(block_bytes=0)], "more than its bound holds")

    def test_the_gap_holds_a_durable_request(self):
        self.refused([roll(wal=kind())], "holds no durable request")
        self.refused([roll(data=kind())], "holds no durable request")

    def test_a_successor_made_on_a_refusal_is_created_in_the_gap(self):
        self.refused(
            [roll("roll_on_refusal", directory_syncs=0, reactor_flushes=10)],
            "created in the gap",
        )
        self.refused([roll("roll_on_refusal", renames=0)], "created in the gap")
        # A creation that took other counts than the code reads is reported.
        self.read([roll("roll_on_refusal", directory_syncs=5, reactor_flushes=10)])

    def test_removal_counts_are_needed(self):
        for key in (*report.REMOVAL_COUNTS, "punch_refused", "extents_known"):
            row = removal()
            del row[key]
            self.refused([row], "removal row lacks a count")

    def test_a_removal_targets_its_case(self):
        self.refused([removal(extent_target=65)], "target is not its case's")

    def test_a_removal_is_punched_to_its_target(self):
        self.refused([removal(holes=62)], "not punched to its target")
        self.refused([removal(1, holes=1)], "not punched to its target")

    def test_a_refused_punch_measures_the_file_as_written(self):
        written = removal(
            punch_refused=True,
            holes=0,
            extents_known=False,
            extents=0,
            data_allocated_bytes=9_502_720,
        )
        self.read([written])
        self.read([removal(punch_refused=True, holes=0, extents=1)])
        self.refused([removal(punch_refused=True)], "refused")
        self.refused([removal(1, punch_refused=True, holes=0, extents=1)], "refused")

    def test_counted_extents_follow_the_holes(self):
        self.refused([removal(extents=63)], "fewer extents counted")
        self.refused([removal(extents=0)], "mapped file has no extent")
        self.refused(
            [removal(extents_known=False)], "extents counted where none were mapped"
        )
        self.read([removal(extents_known=False, extents=0)])

    def test_a_removal_file_has_a_size_and_block(self):
        self.refused([removal(fs_block_bytes=0)], "no size or block")
        self.refused([removal(data_file_bytes=0)], "no size or block")

    def test_a_removal_unlinks_its_data_file_once(self):
        row = removal(removed_files=6)
        row["data"].update(unlinks=2)
        self.refused([row], "data file once")
        row = removal()
        row["data"].update(unlink_ns=0, unlink_longest_ns=0)
        self.refused([row], "data file once")

    def test_a_removal_unlinks_the_files_it_removed(self):
        self.refused([removal(removed_files=4)], "files it removed")
        row = removal(removed_files=2)
        row["other"].update(unlinks=1)
        self.refused([row], "files it removed")

    def test_a_removal_marks_and_takes_away_its_directory(self):
        self.refused([removal(renames=0)], "mark and take away")
        self.refused([removal(directory_syncs=1, reactor_flushes=1)], "take away")
        self.refused([removal(directory_removes=1)], "take away")

    def test_a_removal_flushes_no_data_file(self):
        row = removal(reactor_flushes=3)
        row["data"].update(flushes=1)
        self.refused([row], "flushed a data file")

    def test_a_creation_publishes_into_a_synced_directory(self):
        self.refused([creation(directory_syncs=0)], "published nothing")
        self.refused([creation(renames=0)], "published nothing")
        self.refused([creation(other=kind())], "published nothing")
        # Counts other than the code reads are what the run saw, beside
        # what the summary says the code reads.
        for row in (
            creation(directory_syncs=5),
            creation(directory_syncs=7, reactor_flushes=10),
            creation(renames=4),
        ):
            self.read([row])
        summary = report.summarize(self.read([creation(directory_syncs=5)]))
        counted = summary["cases"]["creation"]
        self.assertEqual(counted["directory_syncs"], 5)
        self.assertEqual(
            counted["read_from_code"],
            {"directory_syncs": 6, "flushes": 3, "renames": 3},
        )

    def test_a_creation_removes_nothing(self):
        self.refused([creation(directory_removes=1)], "removed something")
        self.refused(
            [creation(wal=kind(unlinks=1, unlink_ns=10, unlink_longest_ns=10))],
            "removed something",
        )


if __name__ == "__main__":
    unittest.main()
