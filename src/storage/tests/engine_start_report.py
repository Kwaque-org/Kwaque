"""Validate and summarize engine start measurements.

Each row is one start of a shard's engine on a store that engine wrote and
stopped, timed whole from the engine's construction until the shard is ready;
the driver does nothing inside the interval but make the engine and await its
start. The store's segments were written through the table, a few tiny
requests each, and all but those left writable were sealed. The catalog the
start is given lists some of the sealed segments and every writable one; the
store may retain sealed segments it does not list. A case's name carries
those three counts.

A row holds the case's dimensions, the start's own report, the reactor's
flushes, writes and reads, the store's files and bytes by kind and, in the
observing build, native reads, writes, flushes, opens and unlinks by file
kind, namespace operations and the bytes checksummed and hashed. Every segment
of a store is a generation of one segment identity, so all are in one bucket
directory (`one_bucket`); a production store spreads them.

Per listed segment, the summary gives what a case costs beyond its baseline:
the start of the same store with nothing listed when the log has it, the
store that never took an append otherwise. Across the cases that list
nothing, it says whether a start's reads, opens and namespace operations
grow with what the store retains.
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

PREFIX = "engine_start_v1 "
KINDS = ("wal", "data", "other")
DIMENSIONS = (
    "sealed",
    "unsealed",
    "unlisted",
    "listed",
    "requests_per_segment",
    "request_bytes",
    "catalog_bound",
    "writable_bound",
    "deleting_bound",
)
REACTOR_KEYS = (
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "reactor_reads",
    "reactor_read_bytes",
)
COUNTERS = (
    *DIMENSIONS,
    "elapsed_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    "runtime_operations",
    *REACTOR_KEYS,
)
FLAGS = ("one_bucket", "timing_only", "ndebug", "libcxx_hardening_none", "complete")
# What the start reported, beside its state and whether it restarted. All
# but `recovered` and `wal_files` count work a clean stop leaves none of:
# among them what the start would write back from the WAL, since a clean
# stop synced every run its segments held.
START_COUNTERS = (
    "recovered",
    "unadopted",
    "resumed",
    "marked_retained",
    "gone",
    "owed",
    "refused",
    "sealing",
    "unresolved",
    "materialized",
    "repaired",
    "candidates",
    "uncertified_tails",
    "wal_files",
)
UNFOUND = (
    "unadopted",
    "resumed",
    "marked_retained",
    "gone",
    "owed",
    "refused",
    "sealing",
    "unresolved",
    "materialized",
    "repaired",
    "candidates",
    "uncertified_tails",
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
    "unlinks",
)
NATIVE_COUNTERS = (
    "directory_opens",
    "directory_syncs",
    "listings",
    "stats",
    "renames",
    "directory_removes",
    "crc_bulk_calls",
    "crc_bulk_bytes",
    "digest_calls",
    "digest_bytes",
)
# What a start that does not grow with retained history reads, opens and
# asks of the namespace the same number of times whatever the store retains.
RETAINED_KEYS = ("listings", "stats", "directory_opens")


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def case_name(row: dict) -> str:
    parts = [
        f"{key}_{row[key]}" for key in ("sealed", "unsealed", "unlisted") if row[key]
    ]
    return "_".join(parts) or "no_segments"


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
        raise ValueError(f"{path}: no completed engine start measurements")
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


def validate_dimensions(row: dict) -> None:
    if row["case"] != case_name(row):
        raise ValueError("the case name disagrees with its dimensions")
    if row["listed"] != row["sealed"] + row["unsealed"]:
        raise ValueError("listed segments disagree with the dimensions")
    integer(row["requests_per_segment"], "requests_per_segment", 1)
    integer(row["request_bytes"], "request_bytes", 1)
    integer(row["writable_bound"], "writable_bound", 1)
    # The engine refuses a shard that could leave more unsealed or being
    # removed than one catalog lists, and a case lists no more than leaves
    # room for every segment the shard may leave so.
    if (
        row["unsealed"] > row["writable_bound"]
        or row["writable_bound"] + row["deleting_bound"] > row["catalog_bound"]
        or row["listed"]
        + row["writable_bound"]
        - row["unsealed"]
        + row["deleting_bound"]
        > row["catalog_bound"]
    ):
        raise ValueError("a catalog beyond what one restart lists")


def validate_start(row: dict) -> None:
    start = row["start"]
    for key in START_COUNTERS:
        integer(start[key], f"start {key}")
    if start["state"] != "ready" or start["restarted"] is not True:
        raise ValueError("a start that did not leave the shard ready")
    if start["recovered"] != row["unsealed"] or any(start[key] for key in UNFOUND):
        raise ValueError("the start found another store than the one written")
    if start["wal_files"] < 1:
        raise ValueError("a start without a WAL file")


def validate_store(row: dict) -> None:
    store = row["store"]
    if set(store) != set(KINDS):
        raise ValueError("missing store kinds")
    for kind in KINDS:
        integer(store[kind]["files"], f"store {kind} files")
        integer(store[kind]["bytes"], f"store {kind} bytes")
    if store["data"]["files"] != row["sealed"] + row["unsealed"] + row["unlisted"]:
        raise ValueError("the store holds another number of segments")
    if store["wal"]["files"] < 1:
        raise ValueError("a store without a WAL file")


def validate_native(row: dict) -> None:
    for kind in KINDS:
        validate_kind(row, kind)
    for key in NATIVE_COUNTERS:
        integer(row[key], key)
    data, listed = row["data"], row["listed"]
    # Each listed segment's data header is read to find it; nothing of a
    # segment the catalog does not list is.
    if listed == 0 and (data["reads"] or data["opens"]):
        raise ValueError("a start read a data file its catalog does not list")
    if data["reads"] < listed or data["opens"] < listed:
        raise ValueError("a listed segment's data file was not read")
    if data["flushes"] < row["start"]["recovered"]:
        raise ValueError("a recovered segment without its fresh flush")
    if row["wal"]["flushes"] < 1:
        raise ValueError("the head was closed without its fresh flush")
    # The shard issued at least what the observer saw: a flush of a file or
    # directory, a write or a read submitted, and a submitted write's bytes.
    total = {key: sum(row[kind][key] for kind in KINDS) for key in KIND_COUNTERS}
    if (
        row["reactor_flushes"] < total["flushes"] + row["directory_syncs"]
        or row["reactor_writes"] < total["writes"]
        or row["reactor_write_bytes"] < total["write_bytes"]
        or row["reactor_reads"] < total["reads"]
    ):
        raise ValueError("the observer counted work the shard did not issue")


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
    if not isinstance(row["case"], str) or row["fdatasync"] not in ("aio", "thread"):
        raise ValueError("unknown case or flush mode")
    validate_dimensions(row)
    validate_start(row)
    validate_store(row)
    # The head is closed with a successor: every start makes something
    # durable.
    if row["reactor_flushes"] == 0 or row["reactor_writes"] == 0:
        raise ValueError("a start made nothing durable")
    if row["timing_only"]:
        if any(key in row for key in (*KINDS, *NATIVE_COUNTERS)):
            raise ValueError("timing-only profile cannot claim native work")
    else:
        validate_native(row)
    affinity = row["affinity_cpus"]
    if (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid observed CPU affinity")


def flavour(row: dict) -> tuple:
    return (
        row["requests_per_segment"],
        row["request_bytes"],
        row["fdatasync"],
        row["timing_only"],
        tuple(sorted(row["build_profile"].items())),
    )


def signature(row: dict) -> tuple:
    return (row["case"], *flavour(row))


def median(rows: list[dict], key: str) -> float:
    return statistics.median(row[key] for row in rows)


def ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def summarize(rows: list[dict]) -> dict:
    first = rows[0]
    for row in rows[1:]:
        if row["start"] != first["start"]:
            raise ValueError(f"{first['case']}: runs found different stores")
    result = {
        "case": first["case"],
        "dimensions": {
            key: first[key]
            for key in (
                "sealed",
                "unsealed",
                "unlisted",
                "listed",
                "requests_per_segment",
                "request_bytes",
                "one_bucket",
            )
        },
        "fdatasync": first["fdatasync"],
        "timing_only": first["timing_only"],
        "build_profile": first["build_profile"],
        "flavour": flavour(first),
        "runs": len(rows),
        "store": {
            kind: {
                key: statistics.median(r["store"][kind][key] for r in rows)
                for key in ("files", "bytes")
            }
            for kind in KINDS
        },
        "time_to_ready_ns": {
            "median": median(rows, "elapsed_ns"),
            "minimum": min(r["elapsed_ns"] for r in rows),
            "maximum": max(r["elapsed_ns"] for r in rows),
        },
        "reactor_cpu_ns": median(rows, "reactor_cpu_ns"),
        "allocations": median(rows, "allocations"),
        "tasks": median(rows, "tasks"),
        "runtime_operations": median(rows, "runtime_operations"),
        "reactor": {key: median(rows, key) for key in REACTOR_KEYS},
        "start": first["start"],
    }
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
                    "unlinks",
                )
            }
        for key in NATIVE_COUNTERS:
            native[key] = median(rows, key)
        result["native"] = native
        # Bytes read of each kind, against the bytes the store holds of it.
        result["read_amplification"] = {
            kind: ratio(native[kind]["read_bytes"], result["store"][kind]["bytes"])
            for kind in KINDS
        }
        result["hashed_bytes"] = {
            "crc32c_bulk": native["crc_bulk_bytes"],
            "xxh3": native["digest_bytes"],
        }
    return result


def written(summary: dict) -> int:
    d = summary["dimensions"]
    return d["sealed"] + d["unsealed"] + d["unlisted"]


def baseline(summary: dict, summaries: list[dict]) -> dict | None:
    """The same store started with nothing listed, else the store that never
    took an append, in the same flavour."""
    peers = [s for s in summaries if s["flavour"] == summary["flavour"]]
    if summary["dimensions"]["unsealed"] == 0:
        for peer in peers:
            d = peer["dimensions"]
            if d["listed"] == 0 and d["unlisted"] == written(summary):
                return peer
    for peer in peers:
        if written(peer) == 0:
            return peer
    return None


def per_listed_segment(summary: dict, summaries: list[dict]) -> dict | None:
    listed = summary["dimensions"]["listed"]
    base = baseline(summary, summaries) if listed else None
    if base is None:
        return None
    result = {
        "baseline": base["case"],
        "elapsed_ns": (
            summary["time_to_ready_ns"]["median"] - base["time_to_ready_ns"]["median"]
        )
        / listed,
    }
    if not summary["timing_only"]:
        mine, theirs = summary["native"], base["native"]
        for key in ("reads", "read_bytes", "opens"):
            result[key] = {
                kind: (mine[kind][key] - theirs[kind][key]) / listed for kind in KINDS
            }
        result["crc32c_bulk"] = (
            mine["crc_bulk_bytes"] - theirs["crc_bulk_bytes"]
        ) / listed
        result["xxh3"] = (mine["digest_bytes"] - theirs["digest_bytes"]) / listed
    return result


def retained_counts(summary: dict) -> dict:
    native = summary["native"]
    counts = {
        f"{kind}_{key}": native[kind][key]
        for kind in KINDS
        for key in ("reads", "opens")
    }
    counts.update({key: native[key] for key in RETAINED_KEYS})
    return counts


def retained_history(summaries: list[dict]) -> list[dict]:
    """For each observing flavour, the counts of every case that lists
    nothing: equal counts across what the stores retain are what the claim
    that a start does not grow with retained history predicts. A store that
    never took an append has no durable checkpoint to read, so its counts are
    reported beside them and not compared."""
    result = []
    flavours: dict[tuple, list[dict]] = {}
    for summary in summaries:
        if not summary["timing_only"] and summary["dimensions"]["listed"] == 0:
            flavours.setdefault(summary["flavour"], []).append(summary)
    for group in flavours.values():
        retained = sorted(
            (s for s in group if s["dimensions"]["unlisted"]),
            key=lambda s: s["dimensions"]["unlisted"],
        )
        if not retained:
            continue
        counts = {s["case"]: retained_counts(s) for s in retained}
        entry = {
            "cases": [s["case"] for s in retained],
            "counts": counts,
            "grows": (
                any(c != counts[retained[0]["case"]] for c in counts.values())
                if len(retained) > 1
                else None
            ),
        }
        never = [s for s in group if written(s) == 0]
        if never:
            fixed = retained_counts(never[0])
            entry["beyond_no_segments"] = {
                name: {key: value - fixed[key] for key, value in c.items()}
                for name, c in counts.items()
            }
        result.append(entry)
    return result


def report(rows: list[dict]) -> dict:
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault(signature(row), []).append(row)
    summaries = [summarize(group) for group in groups.values()]
    for summary in summaries:
        summary["per_listed_segment"] = per_listed_segment(summary, summaries)
    retained = retained_history(summaries)
    for summary in summaries:
        del summary["flavour"]
    return {"cases": summaries, "retained_history": retained}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        result = report(rows)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
