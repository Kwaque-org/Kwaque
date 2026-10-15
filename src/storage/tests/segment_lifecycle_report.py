"""Validate and summarize the segment lifecycle cases.

Each case builds what it needs untimed and times one operation over native
files, with the observing file system counting by file kind:

- seal_without_index, seal_with_index: a segment written the same way
  through its own writer, which fed an index that takes every block as an
  anchor, then sealed with no index or with that index published in the same
  step. Only the seal is timed.
- seal_through_table: the table's whole seal of a segment the engine wrote,
  from the call to its answer, with the table's stride taking every block.
- roll_ahead, roll_on_refusal: the gap from the last durable result on a
  segment its own bound filled to the first on its successor, which was
  created before the first took its last request, or only once the first
  refused one as needing a roll. The baseline is the time between the first
  segment's last two durable results. Making each request, and in
  roll_ahead reading the first segment's room after each, is the driver's
  work and lies in the gap and the baseline alike.
- removal_extents_<k>: the table's whole removal of a sealed segment whose
  data file was punched to hold k extents, counted with FS_IOC_FIEMAP. A file
  system that refuses to punch leaves the file as written, and the row says
  so. The observer times the data file's unlink alone.
- creation: one segment created through the table, after a first creation
  that took the store's first identity.

The binary prints one row a case run. This script rejects a row whose counts
do not follow from one another or from what its case does, and a log whose
two writer seals differ by anything but one publication: the index bundle's
flush, rename and directory sync.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS
else:
    from wal_cohort_report import PROFILE_KEYS

PREFIX = "segment_lifecycle_v1 "
KINDS = ("wal", "data", "other")
KIND_COUNTS = (
    "writes",
    "write_bytes",
    "flushes",
    "reads",
    "read_bytes",
    "opens",
    "unlinks",
    "unlink_ns",
    "unlink_longest_ns",
)
REACTOR_COUNTS = (
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "reactor_reads",
    "reactor_read_bytes",
)
COUNTS = (
    "elapsed_ns",
    "reactor_cpu_ns",
    "allocations",
    "tasks",
    *REACTOR_COUNTS,
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
WRITER_SEALS = ("seal_without_index", "seal_with_index")
SEALS = (*WRITER_SEALS, "seal_through_table")
SEAL_COUNTS = (
    "blocks",
    "alignment",
    "index_pages",
    "index_bundle_bytes",
    "retry_pages",
    "retry_bundle_bytes",
    "data_file_bytes",
)
ROLLS = ("roll_ahead", "roll_on_refusal")
ROLL_COUNTS = ("gap_ns", "baseline_ns", "first_requests", "first_bytes", "block_bytes")
REMOVAL = re.compile(r"removal_extents_([1-9][0-9]*)")
REMOVAL_COUNTS = (
    "requests",
    "extent_target",
    "holes",
    "extents",
    "fs_block_bytes",
    "data_file_bytes",
    "data_allocated_bytes",
    "removed_files",
)
# Anchors in one index page.
INDEX_PAGE_ENTRIES = 1024
# A creation as the code reads: three directories, each made with a sync of
# the one above, then the descriptor, the data file's header and the first
# published pointer, each a temporary that is flushed, renamed and its
# directory synced. A row says what its run counted; these are what it is
# compared with, and a count that differs is reported, not refused.
CREATION_DIRECTORY_SYNCS = 6
CREATION_FLUSHES = 3
CREATION_RENAMES = 3
# A seal's publications are its retry bundle and its sealed pointer, and its
# index bundle when it names one; each flushes, renames and syncs once.
SEAL_PUBLICATIONS = 2


def whole(row: dict, keys: tuple[str, ...]) -> bool:
    return all(type(row.get(key)) is int and row[key] >= 0 for key in keys)


def flushes(row: dict) -> int:
    """File flushes the observer saw, of every kind."""
    return sum(row[kind]["flushes"] for kind in KINDS)


def unlinks(row: dict) -> int:
    return sum(row[kind]["unlinks"] for kind in KINDS)


def removal_target(case: str) -> int | None:
    match = REMOVAL.fullmatch(case)
    return int(match.group(1)) if match else None


def validate_kind(kind: object) -> None:
    if not isinstance(kind, dict) or not whole(kind, KIND_COUNTS):
        raise ValueError("a file kind lacks a count")
    if (kind["write_bytes"] and not kind["writes"]) or (
        kind["read_bytes"] and not kind["reads"]
    ):
        raise ValueError("bytes moved by no call")
    if not kind["unlinks"] and (kind["unlink_ns"] or kind["unlink_longest_ns"]):
        raise ValueError("unlink time without an unlink")
    if kind["unlink_longest_ns"] > kind["unlink_ns"]:
        raise ValueError("the longest unlink took more than all of them")


def validate_common(row: dict) -> None:
    if not whole(row, COUNTS):
        raise ValueError("row lacks a count")
    if not row["elapsed_ns"]:
        raise ValueError("timed region took no time")
    for kind in KINDS:
        validate_kind(row.get(kind))
    if row.get("timing_only") is not False or type(row.get("ndebug")) is not bool:
        raise ValueError("row lacks its build flags or claims a timing-only build")
    cpus = row.get("affinity_cpus")
    if (
        not isinstance(cpus, list)
        or not cpus
        or any(type(cpu) is not int or cpu < 0 for cpu in cpus)
        or len(set(cpus)) != len(cpus)
    ):
        raise ValueError("row lacks its CPU affinity")
    if "barrier" in row:
        raise ValueError("a lifecycle row names no append barrier")
    # Every file flush and directory sync the observer saw is a flush the
    # shard issued; the shard's count also holds whatever else ran.
    if row["reactor_flushes"] < flushes(row) + row["directory_syncs"]:
        raise ValueError("the shard issued fewer flushes than the observer saw")


def validate_seal(row: dict) -> None:
    if not whole(row, SEAL_COUNTS) or type(row.get("indexed")) is not bool:
        raise ValueError("seal row lacks a count")
    indexed = row["indexed"]
    if indexed != (row["case"] != "seal_without_index"):
        raise ValueError("a seal named an index other than its case says")
    if (row["index_pages"] > 0) != indexed or (
        row["index_bundle_bytes"] > 0
    ) != indexed:
        raise ValueError("index pages disagree with whether the seal named one")
    # Every block is an anchor: an index of more than one page, as many as
    # its blocks fill.
    if indexed and (
        row["blocks"] <= INDEX_PAGE_ENTRIES
        or row["index_pages"] != -(-row["blocks"] // INDEX_PAGE_ENTRIES)
    ):
        raise ValueError("an indexed seal's pages are not its blocks' anchors")
    if (
        not row["blocks"]
        or not row["alignment"]
        or not row["retry_bundle_bytes"]
        or row["data_file_bytes"] < row["blocks"] * row["alignment"]
    ):
        raise ValueError("a seal lacks its blocks or its retry bundle")
    if not row["data"]["flushes"]:
        raise ValueError("a seal did not flush its sealed root")
    published = SEAL_PUBLICATIONS + indexed
    if row["renames"] < published or row["directory_syncs"] < published:
        raise ValueError("a seal published less than it names")


def validate_roll(row: dict) -> None:
    if (
        not whole(row, ROLL_COUNTS)
        or type(row.get("successor_ahead")) is not bool
        or type(row.get("refused")) is not bool
    ):
        raise ValueError("roll row lacks a count")
    ahead = row["case"] == "roll_ahead"
    if row["successor_ahead"] != ahead or row["refused"] == ahead:
        raise ValueError("a roll's successor was not made as its case says")
    if row["gap_ns"] != row["elapsed_ns"]:
        raise ValueError("a roll's gap is not its timed region")
    if row["first_requests"] < 2 or not row["baseline_ns"]:
        raise ValueError("a roll has no baseline")
    if (
        not row["block_bytes"]
        or row["first_requests"] * row["block_bytes"] > row["first_bytes"]
    ):
        raise ValueError("the first segment took more than its bound holds")
    # The successor's first request is durable inside the gap: its WAL record
    # and its block are each flushed.
    if not row["wal"]["flushes"] or not row["data"]["flushes"]:
        raise ValueError("a roll's gap holds no durable request")
    if not ahead and (not row["directory_syncs"] or not row["renames"]):
        raise ValueError("a successor made on a refusal was not created in the gap")


def validate_removal(row: dict, target: int) -> None:
    if (
        not whole(row, REMOVAL_COUNTS)
        or type(row.get("punch_refused")) is not bool
        or type(row.get("extents_known")) is not bool
    ):
        raise ValueError("removal row lacks a count")
    if row["extent_target"] != target:
        raise ValueError("a removal's target is not its case's")
    if row["punch_refused"]:
        # The first hole was refused and the file is measured as written.
        if row["holes"] or target == 1:
            raise ValueError("holes punched where punching was refused")
    elif row["holes"] != target - 1:
        raise ValueError("a removal's file was not punched to its target")
    if row["extents_known"]:
        if not row["extents"]:
            raise ValueError("a mapped file has no extent")
        if not row["punch_refused"] and row["extents"] < target:
            raise ValueError("fewer extents counted than the holes separate")
    elif row["extents"]:
        raise ValueError("extents counted where none were mapped")
    if not row["fs_block_bytes"] or not row["data_file_bytes"]:
        raise ValueError("a removal's file has no size or block")
    if row["data"]["unlinks"] != 1 or not row["data"]["unlink_ns"]:
        raise ValueError("a removal did not unlink its data file once")
    if row["removed_files"] < 3 or unlinks(row) != row["removed_files"]:
        raise ValueError("a removal's unlinks are not the files it removed")
    # Its deleting name with a sync above, and its directories taken away,
    # the segment's own with one more sync.
    if row["renames"] < 1 or row["directory_syncs"] < 2 or row["directory_removes"] < 2:
        raise ValueError("a removal did not mark and take away its directory")
    if row["data"]["flushes"]:
        raise ValueError("a removal flushed a data file")


def validate_creation(row: dict) -> None:
    # A creation publishes at least one file into a directory it synced.
    if not row["directory_syncs"] or not flushes(row) or not row["renames"]:
        raise ValueError("a creation published nothing")
    if unlinks(row) or row["directory_removes"]:
        raise ValueError("a creation removed something")


def validate(row: dict) -> None:
    if not isinstance(row, dict) or not isinstance(row.get("case"), str):
        raise ValueError("row lacks its case")
    case = row["case"]
    target = removal_target(case)
    if case not in (*SEALS, *ROLLS, "creation") and target is None:
        raise ValueError(f"unknown lifecycle case {case}")
    validate_common(row)
    if case in SEALS:
        validate_seal(row)
    elif case in ROLLS:
        validate_roll(row)
    elif target is not None:
        validate_removal(row, target)
    else:
        validate_creation(row)


def publications(row: dict) -> tuple[int, int, int]:
    return flushes(row), row["renames"], row["directory_syncs"]


def validate_pair(rows: list[dict]) -> None:
    """The writer seals of one log seal the same segment shape, each case the
    same way every run, and the indexed one publishes exactly one bundle
    more."""
    found = {}
    for case in WRITER_SEALS:
        shapes = {
            (row["blocks"], publications(row)) for row in rows if row["case"] == case
        }
        if len(shapes) > 1:
            raise ValueError(f"{case} rows disagree on their segment or publications")
        found[case] = shapes
    without, indexed = found[WRITER_SEALS[0]], found[WRITER_SEALS[1]]
    if not without or not indexed:
        return
    ((blocks, plain),) = without
    ((indexed_blocks, more),) = indexed
    if blocks != indexed_blocks:
        raise ValueError("the writer seals sealed segments of different sizes")
    if tuple(b - a for a, b in zip(plain, more, strict=True)) != (1, 1, 1):
        raise ValueError(
            "a seal with its index is not one publication more than one without"
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
        if not line.startswith(PREFIX):
            continue
        row = json.loads(line.removeprefix(PREFIX))
        validate(row)
        if any(key not in profile for key in PROFILE_KEYS):
            raise ValueError(f"{path}: row lacks the effective build profile")
        row["build_profile"] = dict(profile)
        rows.append(row)
    if not rows:
        raise ValueError(f"{path}: no segment lifecycle rows")
    validate_pair(rows)
    return rows


def median(rows: list[dict], value) -> float:
    return statistics.median(value(row) for row in rows)


def summarize(rows: list[dict]) -> dict:
    profiles = {tuple(sorted(row["build_profile"].items())) for row in rows}
    if len(profiles) != 1:
        raise ValueError("rows of different build profiles")
    cases: dict[str, dict] = {}
    for case in sorted({row["case"] for row in rows}):
        group = [row for row in rows if row["case"] == case]
        entry = {
            "runs": len(group),
            "elapsed_ns": median(group, lambda row: row["elapsed_ns"]),
            "reactor_cpu_ns": median(group, lambda row: row["reactor_cpu_ns"]),
            "reactor_flushes": median(group, lambda row: row["reactor_flushes"]),
            "flushes": median(group, flushes),
            "directory_syncs": median(group, lambda row: row["directory_syncs"]),
            "renames": median(group, lambda row: row["renames"]),
            "crc_bulk_bytes": median(group, lambda row: row["crc_bulk_bytes"]),
            "digest_bytes": median(group, lambda row: row["digest_bytes"]),
        }
        if case in SEALS:
            entry["blocks"] = sorted({row["blocks"] for row in group})
            entry["index_pages"] = sorted({row["index_pages"] for row in group})
            entry["index_bundle_bytes"] = median(
                group, lambda row: row["index_bundle_bytes"]
            )
        elif case in ROLLS:
            entry["gap_ns"] = median(group, lambda row: row["gap_ns"])
            entry["baseline_ns"] = median(group, lambda row: row["baseline_ns"])
            entry["gap_less_baseline_ns"] = entry["gap_ns"] - entry["baseline_ns"]
        elif removal_target(case) is not None:
            entry["extent_target"] = removal_target(case)
            entry["punch_refused"] = any(row["punch_refused"] for row in group)
            counted = [row["extents"] for row in group if row["extents_known"]]
            entry["extents"] = [min(counted), max(counted)] if counted else None
            entry["data_file_bytes"] = median(group, lambda row: row["data_file_bytes"])
            entry["data_unlink_ns"] = median(
                group, lambda row: row["data"]["unlink_ns"]
            )
        elif case == "creation":
            entry["read_from_code"] = {
                "directory_syncs": CREATION_DIRECTORY_SYNCS,
                "flushes": CREATION_FLUSHES,
                "renames": CREATION_RENAMES,
            }
        cases[case] = entry
    result = {"build_profile": rows[0]["build_profile"], "cases": cases}
    # What publishing the index adds to a seal: the two medians and their
    # difference, of segments written the same way.
    if all(case in cases for case in WRITER_SEALS):
        with_index = cases["seal_with_index"]["elapsed_ns"]
        without_index = cases["seal_without_index"]["elapsed_ns"]
        result["index_added"] = {
            "with_index_ns": with_index,
            "without_index_ns": without_index,
            "difference_ns": with_index - without_index,
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        result = summarize(rows)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
