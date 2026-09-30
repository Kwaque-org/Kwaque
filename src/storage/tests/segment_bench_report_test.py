import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import segment_bench_report as report


def measurement(timing=False):
    row = {key: 0 for key in report.COUNTERS}
    row.update({key: False for key in report.FLAGS})
    row.update(
        scope="preencoded_barrier",
        case="tiny",
        encoding="none",
        groups=2,
        blocks=1,
        batches=2,
        window=2,
        segments=1,
        alignment=8192,
        data_start=8192,
        encoded_bytes=32768,
        child_bytes=400,
        child_fragments=8,
        encoded_fragments=14,
        page_bytes=65536,
        elapsed_ns=100,
        reactor_cpu_ns=80,
        allocations=10,
        tasks=5,
        barriers=1,
        submitted_groups=2,
        fixture_retained_bound=1000,
        sampled_admission_peak=2000,
        admission_after=1000,
        digest="a" * 32,
        segment_digest=["a" * 32],
        header_digest=["c" * 32],
        encoded_bytes_by_segment=[32768],
        cuts=[[24576, 40960], [40960, 40960]],
        affinity_cpus=[4],
        native_geometry=[4096, 512, 512, 512, 131072, 131072],
        comparison_id="b" * 64,
        complete=True,
        disk=True,
        timing_only=timing,
        ndebug=True,
        libcxx_hardening_none=True,
        build_profile=dict(
            allocator="native",
            injection="false",
            optimized="true",
            asan="false",
            ubsan="false",
            oom_abort="true",
        ),
        flush_times=[] if timing else [[40, 50]],
    )
    if not timing:
        row.update({key: 0 for key in report.DIAGNOSTICS})
        row.update(
            native_calls=2,
            native_bytes=32768,
            native_depth=2,
            file_flushes=1,
            native_write_min=16384,
            native_write_max=16384,
            write_service_ns=30,
            write_busy_ns=20,
            write_max_service_ns=15,
            flush_service_ns=10,
            staging_copy_upper_bound=32768,
            crc_inline_observed=False,
        )
    return row


class SegmentReportTest(unittest.TestCase):
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

    def test_timing_and_instrumented_profiles_remain_distinct(self):
        report.validate(measurement())
        timed = measurement(True)
        report.validate(timed)
        with self.assertRaises(ValueError):
            report.compare([timed], [measurement()])
        timed["native_calls"] = 0
        with self.assertRaises(ValueError):
            report.validate(timed)

    def test_barriers_and_covering_cuts_cannot_be_dropped(self):
        for mutate in (
            lambda r: r.update(barriers=0),
            lambda r: r.update(submitted_groups=1),
            lambda r: r["cuts"][0].__setitem__(1, 24576),
            lambda r: r["cuts"][1].__setitem__(0, 40959),
            lambda r: r.update(encoded_bytes=16384),
            lambda r: r.update(complete=False),
        ):
            row = measurement()
            mutate(row)
            with self.subTest(row=row), self.assertRaises(ValueError):
                report.validate(row)

    def test_intervals_are_bounded_but_overlap_is_not_summed_as_latency(self):
        row = measurement()
        row["write_service_ns"] = 150
        report.validate(row)
        for change in (
            {"write_busy_ns": 101},
            {"write_max_service_ns": 21},
            {"flush_times": [[40, 110]]},
            {"flush_service_ns": 9},
        ):
            altered = {**row, **change}
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.validate(altered)

    def test_synchronized_writer_barriers_may_skip_flushes_only(self):
        row = measurement()
        row.update(
            scope="writer_append_barrier",
            file_flushes=0,
            flush_times=[],
            flush_service_ns=0,
        )
        report.validate(row)
        row = measurement()
        row.update(
            scope="writer_append_barrier",
            file_flushes=2,
            flush_times=[[40, 45], [45, 50]],
            flush_service_ns=10,
        )
        with self.assertRaises(ValueError):
            report.validate(row)
        row = measurement()
        row.update(file_flushes=0, flush_times=[], flush_service_ns=0)
        with self.assertRaises(ValueError):
            report.validate(row)

    def test_valid_retry_io_keeps_its_additional_issued_bytes(self):
        row = measurement()
        row.update(native_bytes=36864, staging_copy_upper_bound=36864, native_calls=3)
        report.validate(row)
        self.assertEqual(
            report.summarize([row])["native_write_amplification_over_child_wire"], 92.16
        )

    def test_scope_identity_and_run_context_cannot_be_mixed(self):
        row = measurement(True)
        for change in (
            {"scope": "writer_append_barrier"},
            {"affinity_cpus": [5]},
            {"device": 7},
            {"comparison_id": "c" * 64},
            {"digest": "c" * 32, "segment_digest": ["c" * 32]},
            {"native_geometry": [4096, 4096, 4096, 4096, 131072, 131072]},
        ):
            other = {**row, **change}
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.compare([row], [other])
        row["comparison_id"] = ""
        with self.assertRaises(ValueError):
            report.compare([row], [row])

    def test_keeps_outliers_and_does_not_claim_tail_qualification(self):
        rows = [measurement(True) for _ in range(3)]
        rows[-1]["elapsed_ns"] = 10000
        summary = report.summarize(rows)
        self.assertEqual(summary["elapsed_ns"]["samples"], [100, 100, 10000])
        self.assertEqual(summary["elapsed_ns"]["median"], 100)
        self.assertFalse(summary["tail_latency_qualification"])

    def test_allocation_completeness_and_critical_capability(self):
        row = measurement()
        row.update(
            memory_observed=True,
            memory_complete=True,
            memory_peak=2048,
            largest_allocation=1024,
            critical_observed=False,
            critical_peak=None,
        )
        report.validate(row)
        for change in (
            {"memory_complete": False},
            {"critical_peak": 0},
            {"largest_allocation": 262144},
            {"memory_peak": 1023},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.validate({**row, **change})
        row["build_profile"]["injection"] = "true"
        row.update(critical_observed=True, critical_peak=512)
        report.validate(row)

    def test_typed_evidence_requires_single_hash_and_no_decompression(self):
        row = measurement()
        row.update(
            scope="typed_evidence",
            disk=False,
            submitted_groups=0,
            barriers=0,
            native_geometry=[0] * 6,
            native_bytes=0,
            native_calls=0,
            native_depth=0,
            native_write_min=0,
            native_write_max=0,
            file_flushes=0,
            flush_times=[],
            flush_service_ns=0,
            write_service_ns=0,
            write_busy_ns=0,
            write_max_service_ns=0,
            staging_copy_upper_bound=0,
            digest_update_bytes=32768,
        )
        report.validate(row)
        for change in (
            {"digest_update_bytes": 65536},
            {"lz4_frames": 1},
            {"crc_inline_observed": True},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.validate({**row, **change})

    def test_lifecycle_retention_page_replay_and_empty_extent(self):
        row = measurement(True)
        row.update(
            scope="lifecycle",
            sealed_segments=1,
            retry_pages=2,
            page_source_reads=4,
            retain_results=True,
            retained_results=2,
            reopen_profile=True,
            reopens=2,
        )
        report.validate(row)
        for change in (
            {"sealed_segments": 0},
            {"retained_results": 1},
            {"page_source_reads": 2},
            {"reopens": 1},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.validate({**row, **change})
        empty = copy.deepcopy(row)
        empty.update(
            case="empty",
            groups=0,
            batches=0,
            encoded_bytes=0,
            child_bytes=0,
            child_fragments=0,
            encoded_fragments=0,
            cuts=[],
            encoded_bytes_by_segment=[0],
            submitted_groups=0,
            barriers=0,
            retry_pages=0,
            page_source_reads=0,
            retain_results=False,
            retained_results=0,
            reopen_profile=False,
            reopens=0,
        )
        report.validate(empty)

    def test_group_commit_shape_is_bound(self):
        # Two batches in one group share one footer and the only barrier.
        grouped = measurement()
        grouped.update(
            groups=1,
            blocks=2,
            batches=2,
            window=1,
            cuts=[[40960, 40960]],
            submitted_groups=1,
        )
        report.validate(grouped)
        for change in (
            {"batches": 3},
            {"blocks": 0, "batches": 0},
            {"blocks": 9, "batches": 9},
            {"groups": 65, "batches": 130},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                report.validate({**grouped, **change})
        with self.assertRaises(ValueError):
            report.compare([measurement()], [grouped])

    def test_raw_compressed_children_are_counted_per_batch(self):
        row = measurement()
        row.update(
            scope="raw_evidence",
            encoding="lz4",
            groups=1,
            blocks=2,
            batches=2,
            window=1,
            cuts=[[40960, 40960]],
            disk=False,
            submitted_groups=0,
            barriers=0,
            native_geometry=[0] * 6,
            native_bytes=0,
            native_calls=0,
            native_depth=0,
            native_write_min=0,
            native_write_max=0,
            file_flushes=0,
            flush_times=[],
            flush_service_ns=0,
            write_service_ns=0,
            write_busy_ns=0,
            write_max_service_ns=0,
            staging_copy_upper_bound=0,
            lz4_frames=2,
        )
        report.validate(row)
        with self.assertRaises(ValueError):
            report.validate({**row, "lz4_frames": 1})

    def test_boolean_is_not_a_counter(self):
        row = measurement()
        row["barriers"] = True
        with self.assertRaises(ValueError):
            report.validate(row)


if __name__ == "__main__":
    unittest.main()
