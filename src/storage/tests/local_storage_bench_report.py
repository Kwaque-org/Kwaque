"""Validate and summarize two-barrier local append measurements.

Pair mode proves equal work before comparing scopes: the owner and its
composed baseline must store the same PREPARE and segment bytes with the same
group, footer and block counts.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS, percentile
else:
    from wal_cohort_report import PROFILE_KEYS, percentile

PREFIX = "local_storage_bench_v1 "
SCOPES = ("candidate", "baseline", "sequential")
ARRIVALS = ("held", "serial", "isolated", "burst", "paced")
OBSERVED = ("isolated", "burst", "paced")
KINDS = ("wal", "data", "other")
MAXIMUM_SEGMENTS = 32
# Fewer samples than this leave p99.9 unqualified.
TAIL_SAMPLES = 1000
COUNTERS = (
    "payload_bytes",
    "requests",
    "segments",
    "spacing_ns",
    "wait_ns",
    "flush_cap",
    "wal_device",
    "data_device",
    "child_bytes",
    "elapsed_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    "runtime_operations",
    "foreground_turns",
    "fixture_retained_bound",
    "sampled_admission_peak",
    "wal_groups",
    "wal_flushes",
    "wal_writes",
    "footers",
    "blocks",
)
KIND_COUNTERS = (
    "calls",
    "bytes",
    "flushes",
    "depth",
    "flush_depth",
    "write_service_ns",
    "write_busy_ns",
    "flush_service_ns",
    "cap_waits",
    "cap_wait_ns",
)
FLAGS = (
    "fragmented",
    "zero_timestamps",
    "timing_only",
    "ndebug",
    "libcxx_hardening_none",
    "foreground_probe",
    "memory_observed",
    "complete",
)


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def digest(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(c in "0123456789abcdef" for c in value)
    )


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
        raise ValueError(f"{path}: no completed local append measurements")
    return rows


def validate_kind(row: dict, name: str) -> None:
    kind = row[name]
    for key in KIND_COUNTERS:
        integer(kind[key], f"{name} {key}")
    flushes = kind["flush_times"]
    if kind["flush_times_complete"] is not True or len(flushes) != kind["flushes"]:
        raise ValueError(f"incomplete {name} flush timing")
    if any(
        not isinstance(f, list)
        or len(f) != 2
        or any(type(v) is not int or v < 0 for v in f)
        or f[1] < f[0]
        or f[1] > row["elapsed_ns"]
        for f in flushes
    ):
        raise ValueError(f"invalid {name} flush interval")
    if sum(end - begin for begin, end in flushes) != kind["flush_service_ns"]:
        raise ValueError(f"{name} flush intervals disagree with their service time")
    if kind["write_busy_ns"] > min(kind["write_service_ns"], row["elapsed_ns"]):
        raise ValueError(f"invalid {name} write interval accounting")
    if kind["cap_waits"] and (name != "data" or not row["flush_cap"]):
        raise ValueError("only capped data flushes wait for the device flush cap")


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
    scope, arrival = row["scope"], row["arrival"]
    if scope not in SCOPES or arrival not in ARRIVALS:
        raise ValueError("unknown scope or arrival profile")
    if scope != "candidate" and arrival not in ("held", "serial"):
        raise ValueError("a composed path repeats only a held or serial cut")
    if not isinstance(row["case"], str) or not row["case"]:
        raise ValueError("missing workload identity")
    if row["topology"] not in ("shared", "separate") or row["fdatasync"] not in (
        "aio",
        "thread",
    ):
        raise ValueError("missing device topology or flush mode")
    if (row["topology"] == "shared") != (row["wal_device"] == row["data_device"]):
        raise ValueError("device topology disagrees with the device identities")
    requests, segments = row["requests"], row["segments"]
    if (
        not 1 <= segments <= MAXIMUM_SEGMENTS
        or requests == 0
        or requests % segments
        or row["blocks"] != requests
    ):
        raise ValueError("stored blocks disagree with the offered requests")
    groups, footers, flushes = row["wal_groups"], row["footers"], row["wal_flushes"]
    if arrival == "held":
        fixed = groups == 1 and footers == segments
    elif arrival in ("serial", "isolated"):
        fixed = groups == requests and footers == requests
    else:
        fixed = 1 <= groups <= requests and segments <= footers <= requests
    if not fixed or not 1 <= flushes <= groups:
        raise ValueError("group, footer or WAL barrier counts disagree with the cut")
    if not digest(row["wal_digest"]) or (
        not isinstance(row["segment_digest"], list)
        or len(row["segment_digest"]) != segments
        or not all(digest(d) for d in row["segment_digest"])
    ):
        raise ValueError("missing stored byte identity")
    observed = arrival in OBSERVED
    if row["observation"] != ("requests" if observed else "aggregate"):
        raise ValueError("observation profile disagrees with the arrival profile")
    samples = row["samples"]
    if len(samples) != (requests if observed else 0) or any(
        not isinstance(s, list)
        or len(s) != 4
        or any(type(v) is not int or v < 0 for v in s)
        or not s[0] <= s[1] <= s[2] <= s[3]
        for s in samples
    ):
        raise ValueError("invalid request timing")
    if row["timing_only"]:
        if any(kind in row for kind in KINDS):
            raise ValueError("timing-only profile cannot claim native intervals")
    else:
        for kind in KINDS:
            validate_kind(row, kind)
        if row["wal"]["flushes"] != flushes or row["data"]["flushes"] < segments:
            raise ValueError("native flushes disagree with the barriers")
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


def busy(intervals: list[list[int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for begin, end in sorted(intervals):
        if merged and begin <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((begin, end))
    return merged


def length(intervals: list[tuple[int, int]]) -> int:
    return sum(end - begin for begin, end in intervals)


def overlap(left: list[tuple[int, int]], right: list[tuple[int, int]]) -> int:
    total, i, j = 0, 0, 0
    while i < len(left) and j < len(right):
        total += max(0, min(left[i][1], right[j][1]) - max(left[i][0], right[j][0]))
        if left[i][1] < right[j][1]:
            i += 1
        else:
            j += 1
    return total


def quantiles(values: list[int]) -> dict:
    return {
        "samples": len(values),
        "p50": percentile(values, 0.5),
        "p99": percentile(values, 0.99),
        "p999": percentile(values, 0.999),
        "p999_qualified": len(values) >= TAIL_SAMPLES,
    }


def setup(row: dict) -> tuple:
    return (
        row["case"],
        row["arrival"],
        row["wait_ns"],
        row["flush_cap"],
        row["topology"],
        row["fdatasync"],
        row["zero_timestamps"],
        row["timing_only"],
        row["memory_observed"],
        row["foreground_probe"],
        tuple(sorted(row["build_profile"].items())),
    )


def signature(row: dict) -> tuple:
    return (row["scope"], *setup(row))


def median(rows: list[dict], key: str, per_request: bool = False) -> float:
    return statistics.median(
        row[key] / row["requests"] if per_request else row[key] for row in rows
    )


def summarize(rows: list[dict]) -> dict:
    first = rows[0]
    result = {
        "scope": first["scope"],
        "case": first["case"],
        "arrival": first["arrival"],
        "wait_ns": first["wait_ns"],
        "flush_cap": first["flush_cap"],
        "topology": first["topology"],
        "fdatasync": first["fdatasync"],
        "timing_only": first["timing_only"],
        "build_profile": first["build_profile"],
        "runs": len(rows),
        "requests": first["requests"],
        "segments": first["segments"],
        "payload_bytes": first["payload_bytes"],
        "elapsed_ns_per_request": median(rows, "elapsed_ns", True),
        "reactor_cpu_ns_per_request": median(rows, "reactor_cpu_ns", True),
        "allocations_per_request": median(rows, "allocations", True),
        "tasks_per_request": median(rows, "tasks", True),
        "footers_per_request": median(rows, "footers", True),
        "wal_groups": median(rows, "wal_groups"),
        "wal_flushes": median(rows, "wal_flushes"),
        "foreground_turns": median(rows, "foreground_turns"),
        "sampled_admission_peak": max(r["sampled_admission_peak"] for r in rows),
        "fixture_retained_bound": max(r["fixture_retained_bound"] for r in rows),
    }
    if not first["timing_only"]:
        native = {}
        for kind in ("wal", "data"):
            flushes = [
                end - begin for r in rows for begin, end in r[kind]["flush_times"]
            ]
            native[kind] = {
                "writes": statistics.median(r[kind]["calls"] for r in rows),
                "bytes": statistics.median(r[kind]["bytes"] for r in rows),
                "flushes": statistics.median(r[kind]["flushes"] for r in rows),
                "flush_depth": max(r[kind]["flush_depth"] for r in rows),
                "flush_ns": quantiles(flushes),
                "cap_waits": statistics.median(r[kind]["cap_waits"] for r in rows),
            }
        # The WAL and segment barriers as components: each one's busy time and
        # the time both were flushing. Sequential composition leaves no overlap.
        components = []
        for r in rows:
            wal = busy(r["wal"]["flush_times"])
            data = busy(r["data"]["flush_times"])
            components.append((length(wal), length(data), overlap(wal, data)))
        native["wal_flush_busy_ns"] = statistics.median(c[0] for c in components)
        native["data_flush_busy_ns"] = statistics.median(c[1] for c in components)
        native["overlapped_flush_ns"] = statistics.median(c[2] for c in components)
        result["native"] = native
    if first["arrival"] in OBSERVED:
        samples = [s for r in rows for s in r["samples"]]
        result["arrival_to_result_ns"] = quantiles([s[3] - s[0] for s in samples])
        result["accepted_stage_ns"] = quantiles([s[2] - s[1] for s in samples])
        result["arrival_lateness_ns"] = quantiles([s[1] - s[0] for s in samples])
    return result


def equal_work(left: list[dict], right: list[dict]) -> None:
    def work(row: dict) -> tuple:
        return (
            row["requests"],
            row["segments"],
            row["payload_bytes"],
            row["fragmented"],
            row["child_bytes"],
            row["wal_groups"],
            row["footers"],
            row["blocks"],
            row["wal_digest"],
            tuple(row["segment_digest"]),
        )

    identities = {work(row) for row in left + right}
    if len(identities) != 1:
        raise ValueError(
            f"{left[0]['case']}: scopes stored different bytes or group cuts"
        )


def pairs(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, dict[str, list[dict]]] = {}
    for row in rows:
        groups.setdefault(setup(row), {}).setdefault(row["scope"], []).append(row)
    result = []
    for scopes in groups.values():
        if "baseline" not in scopes:
            continue
        baseline = scopes["baseline"]
        entry = {"case": baseline[0]["case"], "arrival": baseline[0]["arrival"]}
        for scope in ("candidate", "sequential"):
            if scope not in scopes:
                continue
            equal_work(baseline, scopes[scope])
            entry[f"{scope}_to_baseline_elapsed"] = median(
                scopes[scope], "elapsed_ns"
            ) / median(baseline, "elapsed_ns")
            entry[f"{scope}_allocations"] = median(scopes[scope], "allocations")
            entry[f"{scope}_tasks"] = median(scopes[scope], "tasks")
        entry["baseline_allocations"] = median(baseline, "allocations")
        entry["baseline_tasks"] = median(baseline, "tasks")
        result.append(entry)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--pairs", action="store_true")
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        if args.pairs:
            result = pairs(rows)
        else:
            groups: dict[tuple, list[dict]] = {}
            for row in rows:
                groups.setdefault(signature(row), []).append(row)
            result = [summarize(group) for group in groups.values()]
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
