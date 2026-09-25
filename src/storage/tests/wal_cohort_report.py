"""Summarize WAL-only cohort measurements; reject unequal fixed-work pairs."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

PREFIX = "wal_cohort_v1 "
PROFILE_KEYS = ("allocator", "injection", "optimized", "asan", "ubsan", "oom_abort")
WAL_ALIGNMENT = 8192


def arrival_lateness(request: dict) -> int:
    # Retain readability of measurements saved before the field was clarified.
    return (
        request["arrival_lateness_ns"]
        if "arrival_lateness_ns" in request
        else request["queue_ns"]
    )


def observation(row: dict) -> str:
    return row.get("observation", "requests")


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
        raise ValueError(f"{path}: no completed cohort measurements")
    return rows


def cohort_keys(row: dict) -> list[tuple[int, int]]:
    return sorted(
        {(r["file"], r["covering_end"]) for r in row["requests"] if r["accepted"]}
    )


def validate(row: dict) -> None:
    requests = row["requests"]
    if not 0 < row["offered"] <= 128 or len(requests) != row["offered"]:
        raise ValueError("offered work was omitted or exceeded the bounded driver")
    if sum(r["accepted"] for r in requests) != row["accepted"]:
        raise ValueError("accepted count disagrees with request observations")
    if (
        row["elapsed_ns"] <= 0
        or len(row["sha256"]) != 64
        or any(c not in "0123456789abcdef" for c in row["sha256"])
    ):
        raise ValueError("missing duration or byte identity")
    if any(k not in row["build_profile"] for k in PROFILE_KEYS):
        raise ValueError("measurement lacks the effective build profile")
    if any(r["timeout"] and not r["accepted"] for r in requests):
        raise ValueError("unaccepted request cannot time out after acceptance")
    mode = observation(row)
    if mode not in ("requests", "aggregate"):
        raise ValueError("unknown observation profile")
    if mode == "aggregate":
        if (
            row["case"] != "fixed"
            or row["accepted"] != row["offered"]
            or any(r["timeout"] for r in requests)
        ):
            raise ValueError("aggregate observation requires completed fixed work")
        if any(
            any(
                r.get(k) is not None
                for k in (
                    "arrival_lateness_ns",
                    "queue_ns",
                    "admission_ns",
                    "latency_ns",
                    "terminal",
                )
            )
            for r in requests
        ):
            raise ValueError("aggregate observation cannot claim request timing")
    else:
        if any(
            min(r["latency_ns"], arrival_lateness(r), r["admission_ns"], r["terminal"])
            < 0
            for r in requests
        ):
            raise ValueError("negative request timing")
        if any(
            r["latency_ns"] < arrival_lateness(r) + r["admission_ns"] for r in requests
        ):
            raise ValueError("terminal time precedes admission")
    affinity = row.get("affinity_cpus")
    if "affinity_cpus" in row and (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid observed CPU affinity")
    if "reactor_cpu_ns" in row and (
        type(row["reactor_cpu_ns"]) is not int or row["reactor_cpu_ns"] < 0
    ):
        raise ValueError("invalid reactor CPU time")
    write_fields = ("write_busy_ns", "write_max_service_ns")
    if any(key in row for key in write_fields):
        if not all(key in row for key in write_fields):
            raise ValueError("incomplete write interval measurements")
        if row["timing_only"]:
            if any(row[key] is not None for key in write_fields):
                raise ValueError("timing-only profile cannot claim write intervals")
        elif (
            any(type(row[key]) is not int or row[key] < 0 for key in write_fields)
            or not row["write_max_service_ns"]
            <= row["write_busy_ns"]
            <= row["write_service_ns"]
            or row["write_busy_ns"] > row["elapsed_ns"]
        ):
            raise ValueError("invalid write interval accounting")
    endings: dict[int, int] = {}
    coverings: dict[int, int] = {}
    group_ends = {(r["file"], r["end"]) for r in requests if r["accepted"]}
    encoded_bytes = 0
    last_file = 0
    for request in requests:
        if not request["accepted"]:
            if request["end"] or request["covering_end"]:
                raise ValueError("rejected request has a WAL extent")
            continue
        file, end, covering = (request[k] for k in ("file", "end", "covering_end"))
        previous = endings.get(file, WAL_ALIGNMENT)
        if (
            (not endings and file != 0)
            or file < last_file
            or file > last_file + 1
            or end <= previous
            or end % WAL_ALIGNMENT
            or covering % WAL_ALIGNMENT
            or covering < coverings.get(file, 0)
            or (file, covering) not in group_ends
        ):
            raise ValueError("invalid ordered group boundary or covering cut")
        if previous < coverings.get(file, 0) and covering != coverings[file]:
            raise ValueError("covering cut changed inside frozen cohort membership")
        encoded_bytes += end - previous
        endings[file] = end
        coverings[file] = covering
        last_file = file
    if encoded_bytes != row["encoded_bytes"]:
        raise ValueError("group extents disagree with encoded byte count")
    if any(r["covering_end"] < r["end"] for r in requests if r["accepted"]):
        raise ValueError("receipt does not cover its request")
    keys = cohort_keys(row)
    if len(keys) != row["flushes"]:
        raise ValueError("physical barrier count disagrees with completed cohorts")
    if not row["timing_only"]:
        flushes = row["flush_times"]
        if len(flushes) != len(keys) or any(end < begin for begin, end in flushes):
            raise ValueError("incomplete flush timing")
        if (
            any(
                previous[1] > current[0]
                for previous, current in zip(flushes, flushes[1:])
            )
            or sum(end - begin for begin, end in flushes) != row["flush_service_ns"]
            or row["flush_service_ns"] > row["elapsed_ns"]
        ):
            raise ValueError("invalid flush interval accounting")
        terminal = dict(zip(keys, (end for _, end in flushes), strict=True))
        if mode == "requests" and any(
            r["terminal"] < terminal[(r["file"], r["covering_end"])]
            for r in requests
            if r["accepted"] and not r["timeout"]
        ):
            raise ValueError("notification preceded its covering physical flush")


def percentile(values: list[int], quantile: float) -> int | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)]


def summarize(rows: list[dict]) -> dict:
    modes = {observation(row) for row in rows}
    if len(modes) != 1:
        raise ValueError("cannot combine different observation profiles")
    for field, label in (
        ("reactor_cpu_ns", "CPU"),
        ("write_busy_ns", "write interval"),
    ):
        if len({field in row for row in rows}) != 1:
            raise ValueError(f"cannot combine different {label} observation profiles")
    request_timing = modes == {"requests"}
    requests = [r for row in rows for r in row["requests"]]
    accepted = [r for r in requests if r["accepted"]]
    completed = [r for r in accepted if not r["timeout"]]
    latency = [r["latency_ns"] for r in completed] if request_timing else []
    service = (
        [r["latency_ns"] - arrival_lateness(r) for r in completed]
        if request_timing
        else []
    )
    durations = [row["elapsed_ns"] for row in rows]
    elapsed = sum(row["elapsed_ns"] for row in rows)
    notifications = []
    for row in rows:
        if row["timing_only"] or not request_timing:
            continue
        endings = dict(
            zip(cohort_keys(row), (f[1] for f in row["flush_times"]), strict=True)
        )
        notifications.extend(
            r["terminal"] - endings[(r["file"], r["covering_end"])]
            for r in row["requests"]
            if r["accepted"] and not r["timeout"]
        )
    result = {
        "iterations": len(rows),
        "offered": len(requests),
        "accepted": len(accepted),
        "rejected": len(requests) - len(accepted),
        "timeouts": len(accepted) - len(completed),
        "completed": len(completed),
        "latency_samples": len(latency),
        "one_per_thousand_tail_samples_available": len(latency) >= 1000,
        "wal_bytes_per_second": sum(row["encoded_bytes"] for row in rows)
        * 1e9
        / elapsed,
        "completed_requests_per_second": len(completed) * 1e9 / elapsed,
        "latency_ns": {
            name: percentile(latency, q)
            for name, q in (("p50", 0.5), ("p99", 0.99), ("p99_9", 0.999))
        },
        "service_from_submit_ns": {
            name: percentile(service, q)
            for name, q in (("p50", 0.5), ("p99", 0.99), ("p99_9", 0.999))
        },
        "arrival_lateness_p99_ns": (
            percentile([arrival_lateness(r) for r in requests], 0.99)
            if request_timing
            else None
        ),
        "elapsed_ns": {
            "samples": durations,
            "min": min(durations),
            "median": percentile(durations, 0.5),
            "max": max(durations),
        },
        "admission_p99_ns": (
            percentile([r["admission_ns"] for r in accepted], 0.99)
            if request_timing
            else None
        ),
        "notification_p99_ns": percentile(notifications, 0.99),
        "batch_wait_ns": (
            None
            if all(r["timing_only"] for r in rows)
            else sum(r["batch_wait_ns"] for r in rows)
        ),
        "write_service_ns": (
            None
            if all(r["timing_only"] for r in rows)
            else sum(r["write_service_ns"] for r in rows)
        ),
        "flush_service_ns": (
            None
            if all(r["timing_only"] for r in rows)
            else sum(r["flush_service_ns"] for r in rows)
        ),
        "physical_flushes": sum(r["flushes"] for r in rows),
        "logical_writes": sum(r["logical_writes"] for r in rows),
        "native_calls": (
            None
            if all(r["timing_only"] for r in rows)
            else sum(r["native_calls"] for r in rows)
        ),
        "allocations": sum(r["allocations"] for r in rows),
        "tasks": sum(r["tasks"] for r in rows),
        "sampled_retained_admission": max(
            r["sampled_retained_admission"] for r in rows
        ),
        "foreground_turns": sum(r["foreground_turns"] for r in rows),
        "native_memory_peak_upper_bound": max(
            (r["memory_peak"] for r in rows if r["memory_observed"]), default=None
        ),
        "critical_peak_upper_bound": max(
            (r["critical_peak"] for r in rows if r["memory_observed"]), default=None
        ),
        "overlapping_service_times_are_not_additive_latency_components": True,
    }
    if "reactor_cpu_ns" in rows[0]:
        cpu = [row["reactor_cpu_ns"] for row in rows]
        result["reactor_cpu_ns"] = {
            "samples": cpu,
            "min": min(cpu),
            "median": percentile(cpu, 0.5),
            "max": max(cpu),
        }
    if "write_busy_ns" in rows[0]:
        busy = [
            row["write_busy_ns"] for row in rows if row["write_busy_ns"] is not None
        ]
        longest = [
            row["write_max_service_ns"]
            for row in rows
            if row["write_max_service_ns"] is not None
        ]
        result["write_busy_ns"] = sum(busy) if busy else None
        result["write_max_service_ns"] = max(longest, default=None)
    return result


def pair_signature(row: dict) -> tuple:
    validate(row)
    if (
        row["case"] != "fixed"
        or row["accepted"] != row["offered"]
        or any(r["timeout"] for r in row["requests"])
    ):
        raise ValueError(
            "fixed-work comparison requires every offered request to complete"
        )
    if not row["comparison_id"]:
        raise ValueError("paired runs need a recorded common host/device/run context")
    keys = (
        "size",
        "fragmented",
        "child_bytes",
        "child_fragments",
        "delay_ns",
        "sha256",
        "encoded_bytes",
        "offered",
        "flushes",
        "setup_extent",
        "device",
        "preallocated",
        "timing_only",
        "foreground_probe",
        "memory_observed",
        "comparison_id",
        "target_members",
        "target_bytes",
        "group_capacity",
    )
    boundaries = tuple(
        (r["file"], r["end"], r["covering_end"]) for r in row["requests"]
    )
    return tuple(row[k] for k in keys) + (
        boundaries,
        tuple(sorted(row["build_profile"].items())),
        observation(row),
        tuple(row.get("affinity_cpus", [])),
        "reactor_cpu_ns" in row,
        "write_busy_ns" in row,
    )


def compare(left: list[dict], right: list[dict]) -> dict:
    if not left or not right:
        raise ValueError("comparison requires two measured samples")
    signature = pair_signature(left[0])
    if any(pair_signature(row) != signature for row in left + right):
        raise ValueError(
            "comparison changed bytes, barriers, allocation/build profile, device or measurement conditions"
        )
    return {
        "left": summarize(left),
        "right": summarize(right),
        "median_elapsed_ratio_right_over_left": percentile(
            [r["elapsed_ns"] for r in right], 0.5
        )
        / percentile([r["elapsed_ns"] for r in left], 0.5),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    rows = [read_measurements(path) for path in args.logs]
    if args.compare:
        if len(rows) != 2:
            parser.error("--compare requires exactly two logs for one fixed case")
        result = compare(*rows)
    else:
        groups: dict[tuple, list[dict]] = {}
        for row in (r for batch in rows for r in batch):
            key = tuple(
                row[k]
                for k in (
                    "owner",
                    "case",
                    "size",
                    "fragmented",
                    "delay_ns",
                    "timing_only",
                    "preallocated",
                    "foreground_probe",
                    "memory_observed",
                    "device",
                    "comparison_id",
                    "setup_extent",
                    "target_members",
                    "target_bytes",
                    "group_capacity",
                )
            )
            key += (
                tuple(sorted(row["build_profile"].items())),
                observation(row),
                tuple(row.get("affinity_cpus", [])),
            )
            key += ("reactor_cpu_ns" in row, "write_busy_ns" in row)
            groups.setdefault(key, []).append(row)
        result = [
            {"configuration": key, "summary": summarize(group)}
            for key, group in groups.items()
        ]
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
