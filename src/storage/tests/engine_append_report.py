"""Validate and summarize append measurements through the engine.

Each row is one open-loop stream of requests through the engine, the
product's composition of one shard's storage, to its segments (two unless a
run sets more, small fixture children unless it sets a record size, and the
append owner's placement threshold and hold as the run sets them): alone, beside
the seal or the removal of a third segment, beside a creation that the
shard's exhausted share of its filesystem refuses, over two data devices, at
a fixed rate until enough WAL barriers are durable, or rolling: the bench
owns its segments, offers what a full segment refused to a successor made as
a new file, and seals the full one beside the stream. Request i is planned
at a fixed spacing from the stream's first planned instant. Its latency runs
from that instant to its result and its delay from it to its offer; each is
kept in a histogram of one-second intervals, and so is the latency of the
requests that overlapped what ran beside the stream. The driver makes each
request inside the timed region, so its allocations and tasks are not the
owners' alone. Every row carries the shard's own counts of what it issued in
that region, flushes, writes and reads, every file together, in both builds,
and what the append owner placed, held and synced there. A synchronized run
has the WAL file and every data file zero-written before the region, so a
small write goes through a synchronized handle and needs no flush; the
observing build counts those writes. A run may set the alignment of its
files and the largest write through a synchronized handle, rest between
its setup and its stream, make its segments as new files rather than
recycled ones, and name a block device whose own counters, which also see
what the filesystem writes for itself, the row then gives for the
interval. Rows of different streams,
thresholds, holds, alignments or write profiles are summarized apart, so a
sweep over them reads as one summary each, with device bytes per byte of
records, writes per second and flushes and synchronized writes per barrier.

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
    "rolling": "none",
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
# What the append owner placed, held and synced in the timed region, and
# what stood at its end.
PLACEMENT_COUNTERS = (
    "wal_placed_portions",
    "segment_placed_portions",
    "wal_placed_bytes",
    "segment_placed_bytes",
    "held_portions",
    "held_portion_bytes",
    "runs_filled",
    "runs_wanted",
    "runs_limited",
    "runs_synced",
    "runs_pressed",
    "segment_barriers",
    "segment_syncs",
    "sync_rounds",
    "round_syncs",
    "held_bytes",
    "held_runs",
    "unsynced_bytes",
    "unsynced_wal_files",
)
COUNTERS = (
    "requests",
    "completed",
    "request_bound",
    "spacing_ns",
    "targets",
    "payload_bytes",
    "placement_bytes",
    "hold_bytes",
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
    "alignment_bytes",
    "segment_bytes",
    "synchronous_write_bytes",
    "settle_ms",
    "rolls",
    "roll_creation_ns",
    "roll_creation_longest_ns",
    *PLACEMENT_COUNTERS,
)
FLAGS = (
    "same_filesystem",
    "fresh_segments",
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
    "block",
    *COUNTERS,
    *FLAGS,
}
KINDS = ("wal", "data", "other")
# What a block device's counters give for the interval.
BLOCK_COUNTERS = ("reads", "read_bytes", "writes", "write_bytes", "flushes")
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
    "synchronized_calls",
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
    # The shard issued every flush its owners made. One flush covers every
    # write completed before it began, so the barriers do not bound the
    # flushes from below; a synchronized run's barriers reached the device
    # through a flush or a synchronized write.
    if sum(row[name]["flushes"] for name in KINDS) > row["reactor_flushes"]:
        raise ValueError("native flushes exceed the shard's")
    if row["wal_barrier"] == "synchronized" and (
        row["reactor_flushes"] == 0
        and sum(row[name]["synchronized_calls"] for name in KINDS) == 0
    ):
        raise ValueError("claimed barrier reached no device")
    # A removal renames the segment's directory and unlinks its data file.
    if row["beside"] == "removal" and (
        row["data"]["unlinks"] == 0 or names["renames"] == 0
    ):
        raise ValueError("a removal unlinked no data file")


def validate_block(row: dict) -> None:
    block = row["block"]
    if block is None:
        return
    if not isinstance(block, dict) or set(block) != {"device", *BLOCK_COUNTERS}:
        raise ValueError("invalid block device counters")
    if not isinstance(block["device"], str) or not block["device"]:
        raise ValueError("block counters name no device")
    for key in BLOCK_COUNTERS:
        integer(block[key], f"block {key}")
    # The device saw at least the shard's own writes.
    if block["writes"] == 0 or block["write_bytes"] < block["writes"] * 512:
        raise ValueError("the block device saw no writes")


def validate_placement(row: dict) -> None:
    """What the owner counted follows its threshold and hold: nothing is
    placed in the WAL at a threshold of zero, nothing is held with no hold,
    only WAL-placed portions are held, and a run is written only once
    something was held."""
    wal = row["wal_placed_portions"]
    portions = wal + row["segment_placed_portions"]
    if portions == 0:
        raise ValueError("the stream placed no portion")
    if row["placement_bytes"] == 0 and wal != 0:
        raise ValueError("a portion reached the WAL at a threshold of zero")
    if row["held_portions"] > wal or (
        row["hold_bytes"] == 0 and row["held_portions"] != 0
    ):
        raise ValueError("a portion was held that could not be")
    runs = sum(
        row[key]
        for key in (
            "runs_filled",
            "runs_wanted",
            "runs_limited",
            "runs_synced",
            "runs_pressed",
        )
    )
    if row["hold_bytes"] == 0 and (runs != 0 or row["held_runs"] != 0):
        raise ValueError("a run was held with no hold")
    if row["held_runs"] > row["targets"] + 1 or (
        (row["held_runs"] == 0) != (row["held_bytes"] == 0)
    ):
        raise ValueError("held runs disagree with their bytes")
    if row["round_syncs"] < row["sync_rounds"]:
        raise ValueError("a sync round asked for no sync")


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
    # Both files zero-written, or neither; only the observing build counts
    # synchronized writes.
    synchronized = row["wal_barrier"] == "synchronized"
    if synchronized != (row["data_barrier"] == "synchronized") or (
        synchronized and row["timing_only"]
    ):
        raise ValueError("invalid barrier profile")
    # Every barrier claimed reached the device.
    if not synchronized and row["reactor_flushes"] == 0:
        raise ValueError("claimed barrier reached no device")
    # A fresh segment is made zero-written; an alignment is a whole number
    # of 4 KiB that is a power of two.
    if row["fresh_segments"] and not synchronized:
        raise ValueError("fresh segments without zero-written files")
    alignment = row["alignment_bytes"]
    if alignment < 4096 or alignment & (alignment - 1):
        raise ValueError("invalid alignment")
    if row["segment_bytes"] == 0 or row["segment_bytes"] % alignment:
        raise ValueError("invalid segment size")
    if row["synchronous_write_bytes"] < 4096:
        raise ValueError("invalid synchronized write limit")
    # Only a rolling stream rolls, and its segments are new files.
    rolling = row["case"] == "rolling"
    if (row["rolls"] and not rolling) or (rolling and not row["fresh_segments"]):
        raise ValueError("rolls disagree with the case")
    if (row["roll_creation_ns"] == 0) != (row["rolls"] == 0) or (
        row["roll_creation_longest_ns"] > row["roll_creation_ns"]
    ):
        raise ValueError("invalid roll creation time")
    validate_block(row)
    if row["reactor_writes"] == 0 or row["reactor_write_bytes"] < row["reactor_writes"]:
        raise ValueError("a durable stream wrote nothing")
    if (row["share_bytes"] == 0) != (row["case"] == "exhausted_share"):
        raise ValueError("the share disagrees with the case")
    validate_placement(row)
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
        row["payload_bytes"],
        row["placement_bytes"],
        row["hold_bytes"],
        row["alignment_bytes"],
        row["segment_bytes"],
        row["synchronous_write_bytes"],
        row["settle_ms"],
        row["fresh_segments"],
        row["timing_only"],
        tuple(sorted(row["build_profile"].items())),
    )


def setup(row: dict) -> tuple:
    return (
        row["case"],
        row["same_filesystem"],
        row["fdatasync"],
        row["wal_barrier"],
        *stream(row),
    )


def median(rows: list[dict], key: str, per_request: bool = False) -> float:
    return statistics.median(
        row[key] / row["requests"] if per_request else row[key] for row in rows
    )


def block_summary(rows: list[dict]) -> dict | None:
    if any(row["block"] is None for row in rows):
        return None
    payload = rows[0]["payload_bytes"]
    return {
        "device": sorted({row["block"]["device"] for row in rows}),
        "write_bytes_per_payload_byte": (
            statistics.median(
                row["block"]["write_bytes"] / (row["requests"] * payload)
                for row in rows
            )
            if payload
            else None
        ),
        "writes_per_request": statistics.median(
            row["block"]["writes"] / row["requests"] for row in rows
        ),
        "flushes_per_request": statistics.median(
            row["block"]["flushes"] / row["requests"] for row in rows
        ),
    }


def summarize(rows: list[dict]) -> dict:
    first = rows[0]
    groups = sum(row["groups"] for row in rows)
    requests = sum(row["requests"] for row in rows)
    result = {
        "case": first["case"],
        "beside": first["beside"],
        "spacing_ns": first["spacing_ns"],
        "targets": first["targets"],
        "payload_bytes": first["payload_bytes"],
        "placement_bytes": first["placement_bytes"],
        "hold_bytes": first["hold_bytes"],
        "request_bound": first["request_bound"],
        "group_target": first["group_target"],
        "data_devices": len(first["devices"]) - 1,
        "same_filesystem": first["same_filesystem"],
        "fdatasync": first["fdatasync"],
        "barrier": first["wal_barrier"],
        "alignment_bytes": first["alignment_bytes"],
        "fresh_segments": first["fresh_segments"],
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
        "reactor_flushes_per_group": sum(row["reactor_flushes"] for row in rows)
        / groups,
        "reactor_write_bytes_per_request": median(rows, "reactor_write_bytes", True),
        # Device bytes for each byte of the requests' records.
        "device_bytes_per_payload_byte": (
            statistics.median(
                row["reactor_write_bytes"] / (row["requests"] * row["payload_bytes"])
                for row in rows
            )
            if first["payload_bytes"]
            else None
        ),
        "writes_per_second": statistics.median(
            row["reactor_writes"] * PERIOD_NS / row["elapsed_ns"] for row in rows
        ),
        # What the block device did, filesystem included, when every row
        # names one.
        "block": block_summary(rows),
        "synchronized_writes_per_group": (
            sum(row[name]["synchronized_calls"] for row in rows for name in KINDS)
            / groups
            if not first["timing_only"]
            else None
        ),
        "wal_placed_share": sum(row["wal_placed_portions"] for row in rows)
        / sum(
            row["wal_placed_portions"] + row["segment_placed_portions"] for row in rows
        ),
        "held_portions": sum(row["held_portions"] for row in rows),
        "run_closes": {
            why: sum(row[f"runs_{why}"] for row in rows)
            for why in ("filled", "wanted", "limited", "synced", "pressed")
        },
        "segment_syncs_per_round": (
            sum(row["round_syncs"] for row in rows)
            / sum(row["sync_rounds"] for row in rows)
            if any(row["sync_rounds"] for row in rows)
            else None
        ),
        "held_bytes_at_end": statistics.median(row["held_bytes"] for row in rows),
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
