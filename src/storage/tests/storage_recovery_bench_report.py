"""Validate and summarize restart measurements.

Each row is one restart of a store the local append path wrote, measured from
the device's classification until the shard is ready: the time of each restart
step, native reads, writes, flushes and opens by file kind, checksum and
content-identity bytes, the budget's sampled peak and, where observed, the
allocation peak. A case classifies the same store the same way in every run;
a run that does not is rejected rather than averaged.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS
else:
    from wal_cohort_report import PROFILE_KEYS

PREFIX = "storage_recovery_bench_v1 "
STATES = ("stopped", "damaged", "lost_data", "restarted")
STEPS = (
    "inspect",
    "control",
    "decisions",
    "inventory",
    "merge",
    "durable",
)
KINDS = ("wal", "data", "other")
# One read window is at most one contiguous allocation; the scan reader keeps
# at most this many windows in flight beyond the one it examines.
MAXIMUM_WINDOW = 128 * 1024
MAXIMUM_READ_AHEAD = 4
ACTIONS = ("stop", "retain", "publish_recovering", "activate_successor")
CLASSIFICATIONS = (
    "satisfied",
    "footer_only",
    "candidate",
    "suffix",
    "conflict",
    "content",
    "uncertified_tail",
    "corruption",
    "slack",
)
COUNTERS = (
    "window_bytes",
    "read_ahead",
    "payload_bytes",
    "requests",
    "segments",
    "child_bytes",
    "wal_files",
    "wal_file_bytes",
    "data_file_bytes",
    "elapsed_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    "runtime_operations",
    "sampled_admission_peak",
    "sampled_handle_peak",
)
FLAGS = (
    "timing_only",
    "ndebug",
    "libcxx_hardening_none",
    "memory_observed",
    "complete",
)
KIND_COUNTERS = (
    "reads",
    "read_bytes",
    "read_minimum",
    "read_maximum",
    "opens",
    "writes",
    "write_bytes",
    "flushes",
)
STEP_COUNTERS = (
    "reads",
    "read_bytes",
    "writes",
    "write_bytes",
    "flushes",
    "opens",
    "directory_syncs",
    "crc_bulk_bytes",
    "digest_bytes",
)
NATIVE_COUNTERS = (
    "directory_opens",
    "directory_syncs",
    "listings",
    "stats",
    "crc_bulk_calls",
    "crc_bulk_bytes",
    "digest_calls",
    "digest_bytes",
)
CLASSIFICATION_COUNTERS = (
    "predecessor_position",
    "wal_scan_files",
    "prepares",
    "unresolved",
    "nonzero_slack",
    "items",
    "reloads",
    "obligations",
    "segments_scanned",
    "boundaries",
    "candidates",
    "suffix",
    "published",
)
# Per-step native counters, summed across the steps, never exceed the
# interval's per-kind totals: work after the last step still counts there.
STEP_TOTALS = ("reads", "read_bytes", "writes", "write_bytes", "flushes", "opens")


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def read_measurements(path: Path) -> list[dict]:
    profile: dict[str, str] = {}
    rows = []
    for line in path.read_text().splitlines():
        if line.startswith(("allocator=", "kwaque-benchmark-profile-v1 ")):
            fields = dict(item.split("=", 1) for item in line.split() if "=" in item)
            if "abort" in fields:
                fields["oom_abort"] = fields["abort"]
            for key in PROFILE_KEYS:
                if key in fields:
                    if key in profile and profile[key] != fields[key]:
                        raise ValueError(f"{path}: conflicting build profile")
                    profile[key] = fields[key]
        if line.startswith(PREFIX):
            row = json.loads(line.removeprefix(PREFIX))
            row["build_profile"] = dict(profile)
            validate(row)
            rows.append(row)
    if not rows:
        raise ValueError(f"{path}: no completed restart measurements")
    return rows


def validate_kind(row: dict, name: str) -> None:
    kind = row[name]
    for key in KIND_COUNTERS:
        integer(kind[key], f"{name} {key}")
    if kind["reads"] and kind["read_minimum"] > kind["read_maximum"]:
        raise ValueError(f"invalid {name} read sizes")
    if not kind["reads"] and (kind["read_bytes"] or kind["read_maximum"]):
        raise ValueError(f"{name} read bytes without reads")
    if kind["read_bytes"] > kind["reads"] * kind["read_maximum"]:
        raise ValueError(f"{name} read bytes exceed their largest read")


def validate_classification(row: dict) -> None:
    value = row["classification"]
    if value["verdict"] != "ready" or value["wal_action"] != "activate_successor":
        raise ValueError("a restart that did not become ready")
    for key in CLASSIFICATION_COUNTERS:
        integer(value[key], key)
    actions = value["segment_actions"]
    if set(actions) != set(ACTIONS) or any(
        type(actions[a]) is not int or actions[a] < 0 for a in ACTIONS
    ):
        raise ValueError("invalid segment actions")
    if actions["activate_successor"] or actions["stop"]:
        raise ValueError("a segment was planned as a WAL action or stopped")
    if sum(actions.values()) != row["segments"]:
        raise ValueError("segment actions disagree with the segments")
    for group in ("slots", "regions"):
        counts = value[group]
        if set(counts) != set(CLASSIFICATIONS) or any(
            type(counts[c]) is not int or counts[c] < 0 for c in CLASSIFICATIONS
        ):
            raise ValueError(f"invalid {group}")
    slots = value["slots"]
    if (
        value["published"] != actions["publish_recovering"]
        or value["candidates"] != slots["candidate"]
        or value["suffix"] != slots["suffix"]
        or slots["conflict"]
        or value["regions"]["corruption"]
        or value["boundaries"] > row["segments"]
        or value["segments_scanned"] > row["segments"]
        or value["prepares"] > row["requests"]
    ):
        raise ValueError("classification counts disagree with each other")


def validate(row: dict) -> None:
    for key in COUNTERS:
        integer(row[key], key)
    for key in FLAGS:
        if type(row[key]) is not bool:
            raise ValueError(f"invalid {key}")
    if not row["complete"] or row["elapsed_ns"] == 0:
        raise ValueError("incomplete measurement")
    if any(k not in row["build_profile"] for k in PROFILE_KEYS):
        raise ValueError("measurement lacks the effective build profile")
    if not isinstance(row["case"], str) or not row["case"]:
        raise ValueError("missing store identity")
    if row["state"] not in STATES or row["fdatasync"] not in ("aio", "thread"):
        raise ValueError("unknown store state or flush mode")
    requests, segments = row["requests"], row["segments"]
    if (segments == 0) != (requests == 0) or (segments and requests % segments):
        raise ValueError("requests disagree with the segments")
    if row["wal_files"] == 0:
        raise ValueError("a store without a WAL file")
    window = row["window_bytes"]
    if (
        not 4096 <= window <= MAXIMUM_WINDOW
        or window % 4096
        or row["read_ahead"] > MAXIMUM_READ_AHEAD
    ):
        raise ValueError("invalid read window or read-ahead")
    steps = row["steps"]
    if not isinstance(steps, dict) or set(steps) != set(STEPS):
        raise ValueError("missing restart steps")
    for name in STEPS:
        integer(steps[name]["elapsed_ns"], f"{name} elapsed_ns")
    if sum(steps[name]["elapsed_ns"] for name in STEPS) > row["elapsed_ns"]:
        raise ValueError("restart steps exceed the time to ready")
    if row["timing_only"]:
        if any(kind in row for kind in KINDS) or any(
            key in steps[name] for name in STEPS for key in STEP_COUNTERS
        ):
            raise ValueError("timing-only profile cannot claim native work")
    else:
        for kind in KINDS:
            validate_kind(row, kind)
        for key in NATIVE_COUNTERS:
            integer(row[key], key)
        for name in STEPS:
            for key in STEP_COUNTERS:
                integer(steps[name][key], f"{name} {key}")
        for key in STEP_TOTALS:
            stepped = sum(steps[name][key] for name in STEPS)
            if stepped > sum(row[kind][key] for kind in KINDS):
                raise ValueError(f"restart steps exceed the interval's {key}")
        for key in ("crc_bulk_bytes", "digest_bytes"):
            if sum(steps[name][key] for name in STEPS) > row[key]:
                raise ValueError(f"restart steps exceed the interval's {key}")
        if row["classification"]["prepares"] and not steps["merge"]["crc_bulk_bytes"]:
            raise ValueError("a scan of PREPAREs verified no checksum")
        if row["data"]["flushes"] < row["classification"]["published"]:
            raise ValueError("a recovering publication without its fresh flush")
        if row["wal"]["flushes"] < 1:
            raise ValueError("the head was closed without its fresh flush")
    validate_classification(row)
    if row["memory_observed"]:
        for key in ("memory_peak", "largest_allocation"):
            integer(row[key], key)
        if row["memory_complete"] is not True:
            raise ValueError("incomplete allocation observation")
    affinity = row["affinity_cpus"]
    if (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid observed CPU affinity")


def signature(row: dict) -> tuple:
    return (
        row["case"],
        row["state"],
        row["window_bytes"],
        row["read_ahead"],
        row["fdatasync"],
        row["timing_only"],
        row["memory_observed"],
        tuple(sorted(row["build_profile"].items())),
    )


def store(row: dict) -> dict:
    return {
        key: row[key]
        for key in (
            "requests",
            "segments",
            "payload_bytes",
            "child_bytes",
            "wal_files",
            "wal_file_bytes",
            "data_file_bytes",
        )
    }


def median(rows: list[dict], key: str) -> float:
    return statistics.median(row[key] for row in rows)


def ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def summarize(rows: list[dict]) -> dict:
    first = rows[0]
    for row in rows[1:]:
        if store(row) != store(first):
            raise ValueError(f"{first['case']}: runs restarted different stores")
        if row["classification"] != first["classification"]:
            raise ValueError(f"{first['case']}: runs classified the store otherwise")
    result = {
        "case": first["case"],
        "state": first["state"],
        "window_bytes": first["window_bytes"],
        "read_ahead": first["read_ahead"],
        "fdatasync": first["fdatasync"],
        "timing_only": first["timing_only"],
        "build_profile": first["build_profile"],
        "runs": len(rows),
        "store": store(first),
        "time_to_ready_ns": {
            "median": median(rows, "elapsed_ns"),
            "minimum": min(r["elapsed_ns"] for r in rows),
            "maximum": max(r["elapsed_ns"] for r in rows),
        },
        "reactor_cpu_ns": median(rows, "reactor_cpu_ns"),
        "allocations": median(rows, "allocations"),
        "tasks": median(rows, "tasks"),
        "runtime_operations": median(rows, "runtime_operations"),
        "steps_ns": {
            name: statistics.median(r["steps"][name]["elapsed_ns"] for r in rows)
            for name in STEPS
        },
        "sampled_admission_peak": max(r["sampled_admission_peak"] for r in rows),
        "sampled_handle_peak": max(r["sampled_handle_peak"] for r in rows),
        "classification": first["classification"],
    }
    if first["memory_observed"]:
        result["memory_peak"] = max(r["memory_peak"] for r in rows)
        result["largest_allocation"] = max(r["largest_allocation"] for r in rows)
    if not first["timing_only"]:
        native = {}
        for kind in KINDS:
            native[kind] = {
                key: statistics.median(r[kind][key] for r in rows)
                for key in (
                    "reads",
                    "read_bytes",
                    "opens",
                    "writes",
                    "write_bytes",
                    "flushes",
                )
            }
            native[kind]["read_minimum"] = min(
                (r[kind]["read_minimum"] for r in rows if r[kind]["reads"]),
                default=None,
            )
            native[kind]["read_maximum"] = max(r[kind]["read_maximum"] for r in rows)
        for key in ("directory_opens", "directory_syncs", "listings", "stats"):
            native[key] = median(rows, key)
        result["native"] = native
        # Bytes inspected per file kind, against the bytes the store holds.
        result["read_amplification"] = {
            "wal": ratio(native["wal"]["read_bytes"], first["wal_file_bytes"]),
            "data": ratio(native["data"]["read_bytes"], first["data_file_bytes"]),
        }
        result["hashed_bytes"] = {
            "crc32c_bulk": median(rows, "crc_bulk_bytes"),
            "xxh3": median(rows, "digest_bytes"),
        }
        result["steps"] = {
            name: {
                key: statistics.median(r["steps"][name][key] for r in rows)
                for key in STEP_COUNTERS
            }
            for name in STEPS
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        groups: dict[tuple, list[dict]] = {}
        for row in rows:
            groups.setdefault(signature(row), []).append(row)
        result = [summarize(group) for group in groups.values()]
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
