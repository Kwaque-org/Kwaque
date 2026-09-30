"""Validate and summarize segment-only measurements without mixing work scopes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS, percentile
else:
    from wal_cohort_report import PROFILE_KEYS, percentile

PREFIX = "segment_bench_v4 "
SCOPES = {
    "layout",
    "encode_evidence",
    "typed_evidence",
    "raw_evidence",
    "preencoded_barrier",
    "writer_append_barrier",
    "lifecycle",
}
COUNTERS = (
    "payload_bytes",
    "groups",
    "blocks",
    "batches",
    "window",
    "segments",
    "alignment",
    "data_start",
    "encoded_bytes",
    "child_bytes",
    "child_fragments",
    "encoded_fragments",
    "page_bytes",
    "elapsed_ns",
    "digest_drain_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    "runtime_operations",
    "foreground_turns",
    "fixture_retained_bound",
    "sampled_admission_peak",
    "admission_after",
    "barriers",
    "submitted_groups",
    "sealed_segments",
    "retry_pages",
    "page_source_reads",
    "retained_results",
    "reopens",
    "layout_calls",
    "setup_extent",
    "setup_allocated_bytes",
    "device",
)
DIAGNOSTICS = (
    "native_calls",
    "native_bytes",
    "native_write_min",
    "native_write_max",
    "native_depth",
    "file_flushes",
    "directory_syncs",
    "directory_sync_ns",
    "write_service_ns",
    "write_busy_ns",
    "write_max_service_ns",
    "flush_service_ns",
    "staging_copy_upper_bound",
    "digest_update_calls",
    "digest_update_bytes",
    "crc_bulk_calls",
    "crc_bulk_bytes",
    "lz4_calls",
    "lz4_frames",
    "lz4_input_bytes",
    "lz4_output_bytes",
)
FLAGS = (
    "ndebug",
    "libcxx_hardening_none",
    "timing_only",
    "preallocated",
    "foreground_probe",
    "memory_observed",
    "fragmented",
    "replacement",
    "closed_admission",
    "retain_results",
    "reopen_profile",
    "disk",
    "complete",
)


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def digest(value, octets: int = 32) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 2 * octets
        and all(c in "0123456789abcdef" for c in value)
    )


def content_digest(value) -> bool:
    return digest(value, 16)


def validate(row: dict) -> None:
    if type(row.get("zero_timestamps", False)) is not bool:
        raise ValueError("invalid timestamp profile")
    for key in COUNTERS:
        integer(row[key], key)
    for key in FLAGS:
        if type(row[key]) is not bool:
            raise ValueError(f"invalid {key}")
    scope = row["scope"]
    if scope not in SCOPES or not row["complete"] or row["elapsed_ns"] == 0:
        raise ValueError("unknown or incomplete measurement")
    if (
        not isinstance(row["case"], str)
        or not row["case"]
        or row["encoding"] not in ("none", "lz4")
    ):
        raise ValueError("missing workload identity")
    if (
        not 1 <= row["segments"] <= 4
        or not 1 <= row["window"] <= 8
        or not 1 <= row["blocks"] <= 8
        or row["batches"] != row["groups"] * row["blocks"]
        or row["batches"] > 128
    ):
        raise ValueError("workload exceeds bounded driver")
    if (
        row["alignment"] not in (4096, 8192, 65536)
        or row["data_start"] != row["alignment"]
    ):
        raise ValueError("unsupported benchmark header geometry")
    if row["page_bytes"] not in (4096, 65536):
        raise ValueError("unsupported metadata page profile")
    if (
        len(row["segment_digest"]) != row["segments"]
        or not all(content_digest(value) for value in row["segment_digest"])
        or row["digest"] != row["segment_digest"][0]
    ):
        raise ValueError("incomplete extent identities")
    lengths = row["encoded_bytes_by_segment"]
    if len(row["header_digest"]) != row["segments"] or not all(
        content_digest(value) for value in row["header_digest"]
    ):
        raise ValueError("incomplete header identities")
    if (
        len(lengths) != row["segments"]
        or any(type(n) is not int or n < 0 for n in lengths)
        or sum(lengths) != row["encoded_bytes"]
    ):
        raise ValueError("extent byte totals disagree")
    if row["comparison_id"] and not digest(row["comparison_id"]):
        raise ValueError("invalid comparison context")
    if set(row["build_profile"]) != set(PROFILE_KEYS):
        raise ValueError("measurement lacks its effective build profile")
    if row["build_profile"]["allocator"] not in ("native", "system") or any(
        row["build_profile"][k] not in ("true", "false")
        for k in PROFILE_KEYS
        if k != "allocator"
    ):
        raise ValueError("invalid build profile")
    affinity = row["affinity_cpus"]
    if (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid measured affinity")
    expected_disk = scope in (
        "preencoded_barrier",
        "writer_append_barrier",
        "lifecycle",
    )
    if row["disk"] != expected_disk:
        raise ValueError("measurement changed its I/O boundary")
    if len(row["native_geometry"]) != 6 or any(
        type(n) is not int or n < int(expected_disk) for n in row["native_geometry"]
    ):
        raise ValueError("native DMA geometry was omitted")
    cuts = row["cuts"]
    if len(cuts) != row["groups"]:
        raise ValueError("group cuts omitted")
    end = row["data_start"]
    for i, pair in enumerate(cuts):
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("invalid captured cut")
        current, covering = pair
        integer(current, "group end", end + 1)
        integer(covering, "covering end", current)
        last = min(len(cuts) - 1, ((i // row["window"]) + 1) * row["window"] - 1)
        if current % row["alignment"] or covering != cuts[last][0]:
            raise ValueError("invalid aligned group or covering membership")
        end = current
    if end - row["data_start"] != lengths[0]:
        raise ValueError("cuts disagree with encoded bytes")
    expected_groups = row["groups"] * row["segments"] if expected_disk else 0
    expected_barriers = (
        ((row["groups"] + row["window"] - 1) // row["window"]) * row["segments"]
        if expected_disk
        else 0
    )
    if (
        row["submitted_groups"] != expected_groups
        or row["barriers"] != expected_barriers
    ):
        raise ValueError("required group completions or barriers omitted")
    if row["sealed_segments"] != (row["segments"] if scope == "lifecycle" else 0):
        raise ValueError("lifecycle did not seal every owner")
    if row["retained_results"] != (expected_groups if row["retain_results"] else 0):
        raise ValueError("retained-result profile dropped an owner")
    if row["reopens"] != (2 * row["segments"] if row["reopen_profile"] else 0):
        raise ValueError("read-reopen profile omitted a reopen")
    if row["page_source_reads"] != row["retry_pages"] * 2:
        raise ValueError("seal page replay count changed")
    if scope != "lifecycle" and any(row[k] for k in ("retry_pages", "reopens")):
        raise ValueError("append scope included lifecycle work")
    if row["layout_calls"] != (256 if scope == "layout" else 0):
        raise ValueError("scalar work count changed")
    if row["digest_drain_ns"] and scope != "writer_append_barrier":
        raise ValueError("only a live append owner can trail its digest")
    if row["preallocated"]:
        if (
            scope not in ("preencoded_barrier", "writer_append_barrier")
            or row["segments"] != 1
            or row["setup_extent"] < end
            or row["setup_extent"] % (2 << 20)
            or row["setup_allocated_bytes"] < row["setup_extent"]
        ):
            raise ValueError("preallocation changed its declared scope")
    elif row["setup_extent"]:
        raise ValueError("growing-file run claims a setup extent")
    if row["timing_only"]:
        if (
            row["memory_observed"]
            or any(key in row for key in DIAGNOSTICS)
            or row["flush_times"]
        ):
            raise ValueError("timing-only run claims instrumented observations")
    else:
        for key in DIAGNOSTICS:
            integer(row[key], key)
        if row.get("crc_inline_observed") is not False:
            raise ValueError("bulk CRC cannot claim inline coverage")
        if (
            not row["write_max_service_ns"]
            <= row["write_busy_ns"]
            <= row["write_service_ns"]
        ):
            raise ValueError("invalid write interval overlap")
        if row["write_busy_ns"] > row["elapsed_ns"]:
            raise ValueError("write interval escaped measurement")
        intervals = row["flush_times"]
        if len(intervals) != row["file_flushes"] or len(intervals) > 128:
            raise ValueError("file flush intervals omitted")
        for begin, end in intervals:
            integer(begin, "flush begin")
            integer(end, "flush end", begin)
            if end > row["elapsed_ns"]:
                raise ValueError("flush completed after timing stopped")
        if sum(end - begin for begin, end in intervals) != row["flush_service_ns"]:
            raise ValueError("flush interval sum changed")
        if row["staging_copy_upper_bound"] != row["native_bytes"]:
            raise ValueError("copy bound does not cover issued writes")
        if scope in ("preencoded_barrier", "writer_append_barrier"):
            # A writer barrier over synchronized writes needs no flush, so
            # writer rows may flush less often, never more, than they barrier.
            flushes = row["file_flushes"]
            if (
                row["native_bytes"] < row["encoded_bytes"]
                or flushes > row["barriers"]
                or (scope == "preencoded_barrier" and flushes != row["barriers"])
                or row["directory_syncs"]
            ):
                raise ValueError("append boundary changed physical work")
        if (
            scope == "typed_evidence"
            and row["digest_update_bytes"] != row["encoded_bytes"]
        ):
            raise ValueError("typed evidence did not hash each stored byte once")
        if (
            scope in ("typed_evidence", "encode_evidence", "writer_append_barrier")
            and row["lz4_frames"]
        ):
            raise ValueError("typed child unexpectedly decompressed")
        if (
            scope == "raw_evidence"
            and row["encoding"] == "lz4"
            and row["lz4_frames"] != row["batches"]
        ):
            raise ValueError("raw evidence did not validate each compressed child")
    if row["memory_observed"]:
        if row.get("memory_complete") is not True:
            raise ValueError("allocation observation is incomplete")
        integer(row["memory_peak"], "memory peak")
        integer(row["largest_allocation"], "largest allocation")
        if row["largest_allocation"] > 131072:
            raise ValueError("contiguous allocation ceiling exceeded")
        if row["largest_allocation"] > row["memory_peak"]:
            raise ValueError("largest allocation exceeds peak bound")
        critical = (
            row["build_profile"]["allocator"] == "native"
            and row["build_profile"]["injection"] == "true"
        )
        if row.get("critical_observed") is not critical:
            raise ValueError("critical-allocation capability mismatch")
        if critical:
            integer(row["critical_peak"], "critical peak")
            if row["critical_peak"] > row["memory_peak"]:
                raise ValueError("critical subset exceeds total allocation bound")
        elif row["critical_peak"] is not None:
            raise ValueError("unavailable critical observation must be null")


def read_measurements(path: Path) -> list[dict]:
    rows = []
    profile = {}
    for line in path.read_text().splitlines():
        if line.startswith(("allocator=", "kwaque-benchmark-profile-v1 ")):
            fields = dict(token.split("=", 1) for token in line.split() if "=" in token)
            if "abort" in fields:
                fields["oom_abort"] = fields["abort"]
            for key in PROFILE_KEYS:
                if key in fields:
                    if key in profile and profile[key] != fields[key]:
                        raise ValueError("conflicting effective build profiles")
                    profile[key] = fields[key]
        if line.startswith(PREFIX):
            row = json.loads(line.removeprefix(PREFIX))
            row["build_profile"] = dict(profile)
            validate(row)
            rows.append(row)
    if not rows:
        raise ValueError(f"{path}: no completed segment measurements")
    return rows


def signature(row: dict, *, include_scope: bool = True) -> tuple:
    validate(row)
    keys = (
        "case",
        "payload_bytes",
        "encoding",
        "fragmented",
        "alignment",
        "data_start",
        "groups",
        "blocks",
        "window",
        "segments",
        "encoded_bytes",
        "child_bytes",
        "child_fragments",
        "encoded_fragments",
        "page_bytes",
        "preallocated",
        "setup_extent",
        "setup_allocated_bytes",
        "replacement",
        "closed_admission",
        "retain_results",
        "reopen_profile",
        "timing_only",
        "ndebug",
        "libcxx_hardening_none",
        "foreground_probe",
        "memory_observed",
        "device",
        "comparison_id",
    )
    return (
        tuple(row[key] for key in keys)
        + (row.get("zero_timestamps", False),)
        + (
            tuple(row["segment_digest"]),
            tuple(row["header_digest"]),
            tuple(tuple(pair) for pair in row["cuts"]),
            tuple(row["encoded_bytes_by_segment"]),
            tuple(row["affinity_cpus"]),
            tuple(row["native_geometry"]),
            tuple(sorted(row["build_profile"].items())),
        )
        + ((row["scope"],) if include_scope else ())
    )


def summarize(rows: list[dict]) -> dict:
    if not rows or any(signature(row) != signature(rows[0]) for row in rows):
        raise ValueError("cannot combine different work or observation conditions")

    def distribution(key):
        values = [row[key] for row in rows]
        return {
            "samples": values,
            "min": min(values),
            "median": percentile(values, 0.5),
            "max": max(values),
        }

    result = {
        "scope": rows[0]["scope"],
        "case": rows[0]["case"],
        "iterations": len(rows),
        "elapsed_ns": distribution("elapsed_ns"),
        "reactor_cpu_ns": distribution("reactor_cpu_ns"),
        "allocations": distribution("allocations"),
        "tasks": distribution("tasks"),
        "fixture_retained_bound": max(row["fixture_retained_bound"] for row in rows),
        "sampled_admission_peak": max(row["sampled_admission_peak"] for row in rows),
        "foreground_turns": sum(row["foreground_turns"] for row in rows),
        "sealed_segments": sum(row["sealed_segments"] for row in rows),
        "retry_pages": sum(row["retry_pages"] for row in rows),
        "page_source_reads": sum(row["page_source_reads"] for row in rows),
        "overlapping_io_intervals_are_not_additive_latency_components": True,
        "tail_latency_qualification": False,
    }
    if rows[0]["scope"] == "layout":
        result["layouts_per_second"] = (
            sum(row["layout_calls"] for row in rows)
            * 1e9
            / sum(row["elapsed_ns"] for row in rows)
        )
    else:
        result["encoded_bytes_per_second"] = (
            sum(row["encoded_bytes"] for row in rows)
            * 1e9
            / sum(row["elapsed_ns"] for row in rows)
        )
    if not rows[0]["timing_only"]:
        result["diagnostics"] = {key: distribution(key) for key in DIAGNOSTICS}
        total = sum(row["child_bytes"] for row in rows)
        result["native_write_amplification_over_child_wire"] = (
            sum(row["native_bytes"] for row in rows) / total if total else None
        )
    if rows[0]["memory_observed"]:
        result["new_native_peak_upper_bound"] = max(row["memory_peak"] for row in rows)
        result["critical_peak_upper_bound"] = max(
            (row["critical_peak"] for row in rows if row["critical_observed"]),
            default=None,
        )
    return result


def compare(left: list[dict], right: list[dict]) -> dict:
    if not left or not right or not left[0]["comparison_id"]:
        raise ValueError("paired runs require a common recorded context")
    if any(signature(row) != signature(left[0]) for row in left + right):
        raise ValueError(
            "comparison changed scope, bytes, barriers, profile, affinity or device"
        )
    return {
        "left": summarize(left),
        "right": summarize(right),
        "median_elapsed_ratio_right_over_left": percentile(
            [r["elapsed_ns"] for r in right], 0.5
        )
        / percentile([r["elapsed_ns"] for r in left], 0.5),
        "median_cpu_ratio_right_over_left": (
            percentile([r["reactor_cpu_ns"] for r in right], 0.5)
            / percentile([r["reactor_cpu_ns"] for r in left], 0.5)
            if percentile([r["reactor_cpu_ns"] for r in left], 0.5)
            else None
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    try:
        batches = [read_measurements(path) for path in args.logs]
        if args.compare:
            if len(batches) != 2:
                parser.error("--compare requires exactly two logs")
            result = compare(*batches)
        else:
            groups = {}
            for row in (row for batch in batches for row in batch):
                groups.setdefault(signature(row), []).append(row)
            result = [summarize(group) for group in groups.values()]
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
