"""Validate and summarize two-barrier local append measurements.

Pair mode proves equal work before comparing scopes: the owner and its
composed baseline must store the same PREPARE and segment bytes with the same
group, footer and block counts.

Requests that one WAL barrier made durable are not independent of each other:
a tail quantile counts the barriers its requests took, and a p99.9 is given
only over 3,000 of them.

Placement mode compares today's composition with three that keep one barrier
per group: segment_placed writes no WAL record; wal_placed acknowledges on the
WAL and syncs its segments once after the last group; wal_held does the same
but holds its segment writes until every group is acknowledged. All must
store the same segment bytes before their times are compared.

Writes mode compares each scope with itself on zero-written files, where
small writes go through a synchronized handle and need no flush, against
files that grow with every write. Both must store the same bytes.
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
SCOPES = (
    "candidate",
    "baseline",
    "sequential",
    "segment_placed",
    "wal_placed",
    "wal_held",
)
PLACED = ("segment_placed", "wal_placed", "wal_held")
ARRIVALS = ("held", "serial", "isolated", "burst", "paced")
OBSERVED = ("isolated", "burst", "paced")
KINDS = ("wal", "data", "other")
MAXIMUM_SEGMENTS = 32
# Fewer independent groups than this leave p99.9 unqualified.
TAIL_GROUPS = 3000
# What a row of a later binary also says of its timed region: the barrier each
# side of the append took, and the shard's own native counts.
BARRIERS = ("none", "overwrite", "first_write", "extending", "synchronized")
REACTOR_KEYS = (
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "reactor_reads",
    "reactor_read_bytes",
)
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
    if "synchronized_calls" in kind and (
        integer(kind["synchronized_calls"], f"{name} synchronized_calls")
        > kind["calls"]
    ):
        raise ValueError(f"{name} synchronized writes exceed its writes")
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
    # On zero-written files a WAL barrier over synchronized writes alone
    # issues no flush; each barrier then has one or the other.
    synchronized = row.get("wal_barrier") == "synchronized"
    wal_synchronized = row.get("wal_synchronized_writes", 0)
    if synchronized:
        integer(wal_synchronized, "wal_synchronized_writes")
    if scope == "segment_placed":
        # Nothing reaches the WAL; each group still ends with one footer a
        # segment.
        fixed = groups == 0 and flushes == 0
        fixed = fixed and footers == (segments if arrival == "held" else requests)
    else:
        if arrival == "held":
            fixed = groups == 1 and footers == segments
        elif arrival in ("serial", "isolated"):
            fixed = groups == requests and footers == requests
        else:
            fixed = 1 <= groups <= requests and segments <= footers <= requests
        if synchronized:
            fixed = fixed and flushes <= groups and flushes + wal_synchronized >= 1
        else:
            fixed = fixed and 1 <= flushes <= groups
    if not fixed:
        raise ValueError("group, footer or WAL barrier counts disagree with the cut")
    # When the last request was acknowledged: never after the timed region.
    if "ack_ns" in row and (
        type(row["ack_ns"]) is not int or not 0 < row["ack_ns"] <= row["elapsed_ns"]
    ):
        raise ValueError("invalid acknowledgement time")
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
        data = row["data"]["flushes"]
        if synchronized:
            data += row["data"].get("synchronized_calls", 0)
        if row["wal"]["flushes"] != flushes or data < segments:
            raise ValueError("native flushes disagree with the barriers")
    if "reactor_flushes" in row:
        for key in REACTOR_KEYS:
            integer(row[key], key)
        if row["wal_barrier"] not in BARRIERS or row["data_barrier"] not in BARRIERS:
            raise ValueError("unknown barrier")
        # Every barrier the WAL claims reached the device.
        if row["reactor_flushes"] < flushes or (
            row["reactor_flushes"] == 0 and not synchronized
        ):
            raise ValueError("claimed barrier reached no device")
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


def quantiles(values: list[int], groups: int) -> dict:
    """`values` taken from `groups` independent groups."""
    qualified = groups >= TAIL_GROUPS
    return {
        "samples": len(values),
        "groups": groups,
        "p50": percentile(values, 0.5),
        "p99": percentile(values, 0.99),
        "p999": percentile(values, 0.999) if qualified else None,
        "p999_qualified": qualified,
    }


def setup(row: dict) -> tuple:
    return (*conditions(row), row.get("wal_barrier"), row.get("data_barrier"))


def conditions(row: dict) -> tuple:
    """Everything a row was measured under except the barriers its files took."""
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
                "flush_ns": quantiles(flushes, len(flushes)),
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
        groups = sum(r["wal_flushes"] for r in rows)
        result["arrival_to_result_ns"] = quantiles(
            [s[3] - s[0] for s in samples], groups
        )
        result["accepted_stage_ns"] = quantiles([s[2] - s[1] for s in samples], groups)
        result["arrival_lateness_ns"] = quantiles(
            [s[1] - s[0] for s in samples], groups
        )
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


def placement(rows: list[dict]) -> list[dict]:
    """Each placed scope against the two-barrier baseline of its case."""

    def stored(row: dict) -> tuple:
        return (
            row["requests"],
            row["segments"],
            row["payload_bytes"],
            row["fragmented"],
            row["child_bytes"],
            row["footers"],
            row["blocks"],
            tuple(row["segment_digest"]),
        )

    def side(scope_rows: list[dict]) -> dict:
        return {
            "runs": len(scope_rows),
            "ack_ns_per_request": median(scope_rows, "ack_ns", True),
            "elapsed_ns_per_request": median(scope_rows, "elapsed_ns", True),
            "reactor_cpu_ns_per_request": median(scope_rows, "reactor_cpu_ns", True),
            "device_write_bytes": median(scope_rows, "reactor_write_bytes"),
            "device_writes": median(scope_rows, "reactor_writes"),
            "device_flushes": median(scope_rows, "reactor_flushes"),
            "wal_flushes": median(scope_rows, "wal_flushes"),
        }

    groups: dict[tuple, dict[str, list[dict]]] = {}
    for row in rows:
        groups.setdefault(setup(row), {}).setdefault(row["scope"], []).append(row)
    result = []
    for scopes in groups.values():
        if "baseline" not in scopes or not any(s in scopes for s in PLACED):
            continue
        baseline = scopes["baseline"]
        for row in [r for s in ("baseline", *PLACED) for r in scopes.get(s, [])]:
            if "ack_ns" not in row or "reactor_write_bytes" not in row:
                raise ValueError(
                    f"{row['case']}: placement needs ack and device counts"
                )
        first = baseline[0]
        entry = {
            "case": first["case"],
            "arrival": first["arrival"],
            "payload_bytes": first["payload_bytes"],
            "child_bytes": first["child_bytes"],
            "requests": first["requests"],
            "segments": first["segments"],
            "topology": first["topology"],
            "fdatasync": first["fdatasync"],
            "baseline": side(baseline),
        }
        for scope in PLACED:
            if scope not in scopes:
                continue
            placed = scopes[scope]
            if {stored(r) for r in baseline + placed} != {stored(first)}:
                raise ValueError(
                    f"{first['case']}: {scope} stored other segment bytes or cuts"
                )
            if scope != "segment_placed" and {
                r["wal_digest"] for r in baseline + placed
            } != {first["wal_digest"]}:
                raise ValueError(f"{first['case']}: {scope} stored other PREPAREs")
            measured = side(placed)
            base = entry["baseline"]
            for key in ("ack_ns_per_request", "elapsed_ns_per_request"):
                measured[f"{key}_to_baseline"] = measured[key] / base[key]
            if base["device_write_bytes"]:
                measured["device_write_bytes_to_baseline"] = (
                    measured["device_write_bytes"] / base["device_write_bytes"]
                )
            entry[scope] = measured
        result.append(entry)
    return result


def writes(rows: list[dict]) -> list[dict]:
    """Each scope on zero-written files against itself on growing files."""

    def stored(row: dict) -> tuple:
        return (
            row["requests"],
            row["segments"],
            row["payload_bytes"],
            row["child_bytes"],
            row["wal_groups"],
            row["footers"],
            row["blocks"],
            row["wal_digest"],
            tuple(row["segment_digest"]),
        )

    def side(scope_rows: list[dict]) -> dict:
        native = scope_rows[0]["timing_only"] is False
        return {
            "runs": len(scope_rows),
            "ack_ns_per_request": median(scope_rows, "ack_ns", True),
            "elapsed_ns_per_request": median(scope_rows, "elapsed_ns", True),
            "device_write_bytes": median(scope_rows, "reactor_write_bytes"),
            "device_flushes": median(scope_rows, "reactor_flushes"),
            "wal_synchronized_writes": statistics.median(
                r.get("wal_synchronized_writes", 0) for r in scope_rows
            ),
            "data_synchronized_writes": statistics.median(
                r["data"].get("synchronized_calls", 0) for r in scope_rows
            )
            if native
            else None,
        }

    groups: dict[tuple, dict[str, list[dict]]] = {}
    for row in rows:
        key = (row["scope"], *conditions(row))
        groups.setdefault(key, {}).setdefault(row["wal_barrier"], []).append(row)
    result = []
    for kinds in groups.values():
        if "extending" not in kinds or "synchronized" not in kinds:
            continue
        growing, zeroed = kinds["extending"], kinds["synchronized"]
        first = growing[0]
        for row in growing + zeroed:
            if "ack_ns" not in row or "reactor_write_bytes" not in row:
                raise ValueError(f"{row['case']}: writes needs ack and device counts")
        if {stored(r) for r in growing + zeroed} != {stored(first)}:
            raise ValueError(
                f"{first['case']}: {first['scope']} stored other bytes on "
                "zero-written files"
            )
        before, after = side(growing), side(zeroed)
        for key in ("ack_ns_per_request", "elapsed_ns_per_request"):
            after[f"{key}_to_growing"] = after[key] / before[key]
        result.append(
            {
                "case": first["case"],
                "scope": first["scope"],
                "arrival": first["arrival"],
                "payload_bytes": first["payload_bytes"],
                "requests": first["requests"],
                "segments": first["segments"],
                "growing": before,
                "zero_written": after,
            }
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--pairs", action="store_true")
    mode.add_argument("--placement", action="store_true")
    mode.add_argument("--writes", action="store_true")
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        if args.writes:
            result = writes(rows)
        elif args.placement:
            result = placement(rows)
        elif args.pairs:
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
