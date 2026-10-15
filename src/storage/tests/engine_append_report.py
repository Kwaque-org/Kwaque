"""Validate and summarize append measurements through the engine.

Each row is one open-loop stream of small requests through the engine, the
product's composition of one shard's storage, to two segments: alone, beside
the seal or the removal of a third segment, beside a creation that the
shard's exhausted share of its filesystem refuses, over two data devices, or
at a fixed rate until enough WAL barriers are durable. Request i is planned
at a fixed spacing from the stream's first planned instant. Its latency runs
from that instant to its result and its delay from it to its offer; each is
kept in a histogram of one-second intervals, and so is the latency of the
requests that overlapped what ran beside the stream. The driver makes each
request inside the timed region, so its allocations and tasks are not the
owners' alone. Every row carries the shard's own counts of what it issued in
that region, flushes, writes and reads, every file together, in both builds.

Requests that one WAL barrier made durable are not independent of each
other: a row counts the distinct barriers its requests took, a quantile is
given as the bucket interval it falls in, and a p99.9 is given only over
3,000 barriers pooled over runs. These cases are reported and never paired:
a seal, a removal or a refused creation changes the work, so `--compare`
sets one case's quantiles beside another's over the same stream and ranks
nothing.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS
else:
    from wal_cohort_report import PROFILE_KEYS

PREFIX = "engine_append_v1 "
# Each case and what runs beside its stream.
CASES = {
    "quiet": "none",
    "during_seal": "seal",
    "during_removal": "removal",
    "exhausted_share": "creation",
    "two_data_devices": "none",
    "fixed_rate": "none",
}
# Fewer independent barriers than this leave p99.9 unqualified.
TAIL_GROUPS = 3000
BARRIERS = ("none", "overwrite", "first_write", "extending", "synchronized")
# The histogram's geometry: nanoseconds from 2^10 to 2^36, each power of two
# cut into 512 buckets; the last bucket holds everything from 2^36 on and the
# first everything up to its upper limit.
MINIMUM_NS = 1 << 10
MAXIMUM_NS = 1 << 36
PRECISION = 512
LAST_BUCKET = 26 * PRECISION
PERIOD_NS = 1_000_000_000
HISTOGRAM_KEYS = {
    "minimum_ns",
    "maximum_ns",
    "precision",
    "period_ns",
    "total",
    "buckets",
}
COUNTERS = (
    "requests",
    "completed",
    "request_bound",
    "spacing_ns",
    "targets",
    "groups",
    "wal_capacity_bytes",
    "share_bytes",
    "elapsed_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    "runtime_operations",
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "reactor_reads",
    "reactor_read_bytes",
)
FLAGS = (
    "same_filesystem",
    "timing_only",
    "ndebug",
    "libcxx_hardening_none",
    "complete",
)
BASE_KEYS = {
    "case",
    "beside",
    "fdatasync",
    "group_target",
    "wal_barrier",
    "data_barrier",
    "devices",
    "target_devices",
    "latency",
    "delay",
    "affinity_cpus",
    *COUNTERS,
    *FLAGS,
}
KINDS = ("wal", "data", "other")
KIND_COUNTERS = (
    "writes",
    "write_bytes",
    "write_service_ns",
    "write_busy_ns",
    "flushes",
    "flush_service_ns",
    "opens",
    "reads",
    "read_bytes",
    "unlinks",
    "unlink_ns",
    "unlink_longest_ns",
)
NAMESPACE_COUNTERS = (
    "directory_opens",
    "directory_syncs",
    "listings",
    "stats",
    "renames",
    "directory_removes",
)
# What every row with something beside its stream says of it, and what each
# kind of it adds.
BESIDE_KEYS = {
    "beside_at",
    "beside_begin_ns",
    "beside_end_ns",
    "beside_outcome",
    "overlapping",
    "overlap_groups",
    "overlap",
}
SEALED_KEYS = {"beside_fill_bytes", "sealed_file_end", "sealed_records"}
BESIDE_KIND_KEYS = {
    "none": set(),
    "seal": SEALED_KEYS,
    "removal": SEALED_KEYS | {"verdict", "gone", "settling", "removed", "foreign"},
    "creation": {"table_size"},
}


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def store_identity(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(c in "0123456789abcdef" for c in value)
    )


def bucket_lower(index: int) -> int:
    """The lower limit of one bucket, as the histogram computes it."""
    exponent, linear = divmod(index, PRECISION)
    return (MINIMUM_NS << exponent) + (linear << (exponent + 1))


def bucket_index(lower) -> int | None:
    """The bucket whose lower limit this is, or None for no bucket's."""
    if type(lower) is not int or not MINIMUM_NS <= lower <= MAXIMUM_NS:
        return None
    exponent = lower.bit_length() - MINIMUM_NS.bit_length()
    linear, rest = divmod(lower - (MINIMUM_NS << exponent), 1 << (exponent + 1))
    if rest or linear >= PRECISION:
        return None
    return exponent * PRECISION + linear


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
        raise ValueError(f"{path}: no completed engine append measurements")
    return rows


def validate_histogram(row: dict, name: str, total: int) -> None:
    """Its geometry, its total, and every kept bucket of every interval: in
    order, a bucket of the geometry, inside the timed region, counting what
    the row says it holds."""
    histogram = row[name]
    if not isinstance(histogram, dict) or set(histogram) != HISTOGRAM_KEYS:
        raise ValueError(f"invalid {name} histogram")
    if (
        histogram["minimum_ns"],
        histogram["maximum_ns"],
        histogram["precision"],
        histogram["period_ns"],
    ) != (MINIMUM_NS, MAXIMUM_NS, PRECISION, PERIOD_NS):
        raise ValueError(f"unknown {name} histogram geometry")
    if histogram["total"] != total:
        raise ValueError(f"{name} histogram holds other than its requests")
    buckets = histogram["buckets"]
    if not isinstance(buckets, list):
        raise ValueError(f"invalid {name} histogram")
    previous, counted = None, 0
    for bucket in buckets:
        if not isinstance(bucket, list) or len(bucket) != 4:
            raise ValueError(f"invalid {name} bucket")
        interval, lower, upper, count = bucket
        integer(interval, f"{name} interval")
        integer(count, f"{name} count", 1)
        index = bucket_index(lower)
        if index is None:
            raise ValueError(f"{name} bucket outside its range")
        if upper != (None if index == LAST_BUCKET else bucket_lower(index + 1)):
            raise ValueError(f"{name} bucket outside its range")
        if previous is not None and (interval, lower) <= previous:
            raise ValueError(f"{name} buckets out of order")
        # Everything was recorded inside the timed region, and nothing it
        # recorded lasted longer than the region.
        if interval * PERIOD_NS > row["elapsed_ns"] or lower > row["elapsed_ns"]:
            raise ValueError(f"{name} bucket outside the timed region")
        previous = (interval, lower)
        counted += count
    if counted != total:
        raise ValueError(f"{name} buckets disagree with their total")


def validate_devices(row: dict) -> None:
    devices = row["devices"]
    if not isinstance(devices, list) or len(devices) < 2:
        raise ValueError("invalid devices")
    for device in devices:
        if not isinstance(device, dict) or set(device) != {
            "store",
            "st_dev",
            "pool",
            "role",
        }:
            raise ValueError("invalid device")
        if not store_identity(device["store"]):
            raise ValueError("invalid device store identity")
        integer(device["st_dev"], "st_dev")
    if len({device["store"] for device in devices}) != len(devices):
        raise ValueError("a device store is named twice")
    # The WAL and control record on the first device; data on the others,
    # which are the device table's.
    control, data = devices[0], devices[1:]
    if control["role"] != "wal_control" or control["pool"] is not None:
        raise ValueError("the first device is not the WAL and control device")
    pools: dict[int, set] = {}
    for device in data:
        if device["role"] != "data":
            raise ValueError("a device after the first holds other than data")
        pools.setdefault(device["st_dev"], set()).add(integer(device["pool"], "pool"))
    # Devices on one filesystem are one pool, numbered from zero.
    numbers = [next(iter(pool)) for pool in pools.values()]
    if any(len(pool) != 1 for pool in pools.values()) or sorted(numbers) != list(
        range(len(pools))
    ):
        raise ValueError("device pools disagree with their filesystems")
    if row["same_filesystem"] != (len(pools) == 1):
        raise ValueError("same_filesystem disagrees with the data devices")
    expected = 2 if row["case"] == "two_data_devices" else 1
    if len(data) != expected:
        raise ValueError("the case has another number of data devices")
    targets = row["target_devices"]
    if (
        not isinstance(targets, list)
        or len(targets) != row["targets"]
        or any(type(t) is not int or not 1 <= t < len(devices) for t in targets)
    ):
        raise ValueError("a segment is on no data device")
    if expected == 2 and len(set(targets)) != 2:
        raise ValueError("two data devices hold the stream's segments on one")


def validate_beside(row: dict) -> None:
    beside = row["beside"]
    if beside == "none":
        return
    at = integer(row["beside_at"], "beside_at")
    if at >= row["requests"]:
        raise ValueError("the stream ended before what ran beside it began")
    begin = integer(row["beside_begin_ns"], "beside_begin_ns")
    if not begin <= integer(row["beside_end_ns"], "beside_end_ns") <= row["elapsed_ns"]:
        raise ValueError("what ran beside the stream is outside the timed region")
    # The request at whose planned instant it began overlapped it.
    overlapping = integer(row["overlapping"], "overlapping", 1)
    if overlapping > row["completed"]:
        raise ValueError("more requests overlapped than completed")
    validate_histogram(row, "overlap", overlapping)
    groups = integer(row["overlap_groups"], "overlap_groups", 1)
    if groups > min(row["groups"], overlapping):
        raise ValueError("overlapping requests took more barriers than there were")
    outcome = row["beside_outcome"]
    if beside in ("seal", "removal"):
        fill = integer(row["beside_fill_bytes"], "beside_fill_bytes", 1)
        integer(row["sealed_records"], "sealed_records", 1)
        if integer(row["sealed_file_end"], "sealed_file_end") < fill:
            raise ValueError("a sealed segment ends before what filled it")
    if beside == "seal" and outcome != "sealed":
        raise ValueError("the seal beside the stream did not seal")
    if beside == "removal":
        gone, settling = row["gone"], row["settling"]
        if type(gone) is not bool or type(settling) is not bool or gone == settling:
            raise ValueError("a removal is neither gone nor settling")
        if row["verdict"] != "authorized" or outcome != (
            "gone" if gone else "settling"
        ):
            raise ValueError("the removal beside the stream did not remove")
        integer(row["removed"], "removed", 1)
        integer(row["foreign"], "foreign")
    if beside == "creation":
        if outcome != "resource_exhausted":
            raise ValueError("a creation was not refused for space")
        if integer(row["table_size"], "table_size") < row["targets"]:
            raise ValueError("the table holds fewer segments than the stream")


def validate_observed(row: dict) -> None:
    for name in KINDS:
        kind = row[name]
        if not isinstance(kind, dict) or set(kind) != set(KIND_COUNTERS):
            raise ValueError(f"invalid {name} counts")
        for key in KIND_COUNTERS:
            integer(kind[key], f"{name} {key}")
        if (
            kind["write_busy_ns"] > kind["write_service_ns"]
            or kind["unlink_longest_ns"] > kind["unlink_ns"]
            or (kind["unlinks"] == 0 and kind["unlink_ns"] != 0)
        ):
            raise ValueError(f"invalid {name} interval accounting")
    names = row["namespace"]
    if not isinstance(names, dict) or set(names) != set(NAMESPACE_COUNTERS):
        raise ValueError("invalid namespace counts")
    for key in NAMESPACE_COUNTERS:
        integer(names[key], key)
    # Every WAL barrier the requests took flushed a WAL file.
    if row["wal"]["flushes"] < row["groups"]:
        raise ValueError("native WAL flushes are fewer than the barriers")
    # A removal renames the segment's directory and unlinks its data file.
    if row["beside"] == "removal" and (
        row["data"]["unlinks"] == 0 or names["renames"] == 0
    ):
        raise ValueError("a removal unlinked no data file")


def validate(row: dict) -> None:
    beside = row.get("beside")
    if row.get("case") not in CASES or CASES[row["case"]] != beside:
        raise ValueError("unknown case or what runs beside it")
    expected = BASE_KEYS | {"build_profile"}
    if beside != "none":
        expected |= BESIDE_KEYS | BESIDE_KIND_KEYS[beside]
    if not row.get("timing_only", True):
        expected |= {*KINDS, "namespace"}
    if set(row) != expected:
        raise ValueError("unexpected or missing fields")
    for key in COUNTERS:
        integer(row[key], key)
    for key in FLAGS:
        if type(row[key]) is not bool:
            raise ValueError(f"invalid {key}")
    if not row["complete"] or row["elapsed_ns"] == 0:
        raise ValueError("incomplete measurement")
    if any(k not in row["build_profile"] for k in PROFILE_KEYS):
        raise ValueError("measurement lacks the effective build profile")
    if row["fdatasync"] not in ("aio", "thread"):
        raise ValueError("missing flush mode")
    requests = row["requests"]
    if row["completed"] != requests or not 1 <= requests <= row["request_bound"]:
        raise ValueError("completed requests disagree with the planned ones")
    if row["spacing_ns"] == 0 or row["targets"] == 0:
        raise ValueError("invalid stream")
    groups = row["groups"]
    if not 1 <= groups <= row["completed"]:
        raise ValueError("barriers disagree with the requests")
    target = row["group_target"]
    if row["case"] == "fixed_rate":
        if type(target) is not int or not 1 <= target <= row["request_bound"]:
            raise ValueError("invalid barrier target")
        if groups < target and requests != row["request_bound"]:
            raise ValueError("a fixed-rate stream ended short of its target")
    elif target is not None or requests != row["request_bound"]:
        raise ValueError("a stream ended short of its plan")
    if row["wal_barrier"] not in BARRIERS or row["data_barrier"] not in BARRIERS:
        raise ValueError("unknown barrier")
    # Every barrier the WAL claims reached the device.
    if row["reactor_flushes"] < groups:
        raise ValueError("claimed barrier reached no device")
    if row["reactor_writes"] == 0 or row["reactor_write_bytes"] < row["reactor_writes"]:
        raise ValueError("a durable stream wrote nothing")
    if (row["share_bytes"] == 0) != (row["case"] == "exhausted_share"):
        raise ValueError("the share disagrees with the case")
    validate_devices(row)
    validate_histogram(row, "latency", row["completed"])
    validate_histogram(row, "delay", row["completed"])
    validate_beside(row)
    if not row["timing_only"]:
        validate_observed(row)
    affinity = row["affinity_cpus"]
    if (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid observed CPU affinity")


def merged(rows: list[dict], name: str) -> list[tuple[int, int | None, int]]:
    """Every run's intervals as one distribution: (lower, upper, count)."""
    counts: dict[int, list] = {}
    for row in rows:
        for _, lower, upper, count in row[name]["buckets"]:
            counts.setdefault(lower, [upper, 0])[1] += count
    return [(lower, upper, count) for lower, (upper, count) in sorted(counts.items())]


def bucket_interval(
    buckets: list[tuple[int, int | None, int]], quantile: float
) -> list | None:
    """The interval of the bucket the quantile falls in. The first bucket
    also holds every value below the range, so its interval starts at zero;
    the last has no upper limit."""
    total = sum(count for _, _, count in buckets)
    if total == 0:
        return None
    rank, seen = max(1, math.ceil(total * quantile)), 0
    for lower, upper, count in buckets:
        seen += count
        if seen >= rank:
            return [0 if lower == MINIMUM_NS else lower, upper]
    return None


def quantiles(rows: list[dict], name: str, groups: int) -> dict:
    """`name`'s values taken from `groups` independent barriers."""
    buckets = merged(rows, name)
    qualified = groups >= TAIL_GROUPS
    return {
        "values": sum(count for _, _, count in buckets),
        "groups": groups,
        "p50": bucket_interval(buckets, 0.5),
        "p99": bucket_interval(buckets, 0.99),
        "p999": bucket_interval(buckets, 0.999) if qualified else None,
        "p999_qualified": qualified,
    }


def stream(row: dict) -> tuple:
    """What two cases must share to be set side by side."""
    return (
        row["spacing_ns"],
        row["request_bound"],
        row["group_target"],
        row["targets"],
        row["timing_only"],
        tuple(sorted(row["build_profile"].items())),
    )


def setup(row: dict) -> tuple:
    return (
        row["case"],
        row["same_filesystem"],
        row["fdatasync"],
        *stream(row),
    )


def median(rows: list[dict], key: str, per_request: bool = False) -> float:
    return statistics.median(
        row[key] / row["requests"] if per_request else row[key] for row in rows
    )


def summarize(rows: list[dict]) -> dict:
    first = rows[0]
    groups = sum(row["groups"] for row in rows)
    requests = sum(row["requests"] for row in rows)
    result = {
        "case": first["case"],
        "beside": first["beside"],
        "spacing_ns": first["spacing_ns"],
        "request_bound": first["request_bound"],
        "group_target": first["group_target"],
        "data_devices": len(first["devices"]) - 1,
        "same_filesystem": first["same_filesystem"],
        "fdatasync": first["fdatasync"],
        "timing_only": first["timing_only"],
        "build_profile": first["build_profile"],
        "runs": len(rows),
        "requests": requests,
        "groups": groups,
        "requests_per_group": requests / groups,
        "elapsed_ns_per_request": median(rows, "elapsed_ns", True),
        "reactor_cpu_ns_per_request": median(rows, "reactor_cpu_ns", True),
        "allocations_per_request": median(rows, "allocations", True),
        "tasks_per_request": median(rows, "tasks", True),
        "reactor_flushes_per_request": median(rows, "reactor_flushes", True),
        "latency_ns": quantiles(rows, "latency", groups),
        "delay_ns": quantiles(rows, "delay", groups),
    }
    if first["beside"] != "none":
        result["beside_ns"] = statistics.median(
            row["beside_end_ns"] - row["beside_begin_ns"] for row in rows
        )
        outcomes: dict[str, int] = {}
        for row in rows:
            outcomes[row["beside_outcome"]] = outcomes.get(row["beside_outcome"], 0) + 1
        result["beside_outcomes"] = outcomes
        result["overlap_ns"] = quantiles(
            rows, "overlap", sum(row["overlap_groups"] for row in rows)
        )
    return result


def summaries(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault(setup(row), []).append(row)
    return [summarize(group) for group in groups.values()]


def compare(rows: list[dict], base: str, case: str) -> dict:
    """`case` beside `base`, each one's quantile intervals side by side."""
    chosen = {}
    for name in (base, case):
        selected = [row for row in rows if row["case"] == name]
        if not selected:
            raise ValueError(f"no rows of {name}")
        if len({stream(row) for row in selected}) != 1:
            raise ValueError(f"{name} was measured over more than one stream")
        chosen[name] = selected
    if stream(chosen[base][0]) != stream(chosen[case][0]):
        raise ValueError(f"{base} and {case} were not measured over one stream")
    left, right = summarize(chosen[base]), summarize(chosen[case])
    result = {"base": base, "case": case, "runs": [left["runs"], right["runs"]]}
    for name in ("latency_ns", "delay_ns"):
        result[name] = {
            "groups": [left[name]["groups"], right[name]["groups"]],
            **{
                quantile: [left[name][quantile], right[name][quantile]]
                for quantile in ("p50", "p99", "p999")
            },
        }
    for side, summary in ((base, left), (case, right)):
        if "overlap_ns" in summary:
            result[f"{side}_overlap_ns"] = summary["overlap_ns"]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--compare", nargs=2, metavar=("BASE", "CASE"))
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        if args.compare:
            result = compare(rows, *args.compare)
        else:
            result = summaries(rows)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
