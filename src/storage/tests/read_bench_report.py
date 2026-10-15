"""Validate and summarize read path measurements.

Each row is one unit of work of a reader over segments the segment writer
wrote: seeks into a sealed segment with nothing of it in memory and with
everything, a scan and seeks at each stride of a sweep, a read to the end at
each depth of read-ahead, a read through a cache every byte of which is
held, and the tail of a segment that is appended to, read at three distances
behind it. A row carries what it was measured over. A row that contradicts
what its case is about is rejected rather than summarized: a warm seek that
read a file, a scan that lost a batch, a tail that was fed and still read
its newest group from the device.

A stride row says how far its session looked ahead, whether that grew, and
how many runs it asked for ahead; rows of another read-ahead are another
measurement and never samples of one. A scanned tail's scan is a session
that does not fill the cache, reading in a fiber of its own beside the
appends and the readers, through files of its own: a reader's device reads
are its own, and the scan's are in the shard's counts alone.

The timing build's file layer counts nothing at the device: its rows carry
zeroes for the reads, the bytes read and the bytes checked, and its tail
rows count every read a hit. Such a row is held to what the cache itself
counted and summarized without what the file layer would have said. Every
measured row of both builds carries the shard's own counts of the flushes,
writes and reads it issued, and their bytes, every file together: a warm
seek reads nothing there either.

A seek into a run an earlier seek loaded is not a cold seek, and a case of
seeks at offsets drawn at random has some. What a cold seek costs is
therefore given for each run loaded as well as for each seek. Every summary
carries its samples' least and most beside their median: a stalled sample
moves no median and still says what the device did.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

PREFIX = "read_bench_v1 "
MEASURED = (
    "elapsed_ns",
    "cpu_ns",
    "allocations",
    "tasks",
    "index_reads",
    "index_read_bytes",
    "index_opens",
    "index_writes",
    "index_write_bytes",
    "index_flushes",
    "data_reads",
    "data_read_bytes",
    "data_opens",
    "path_stats",
    "directory_syncs",
    "hashed_bytes",
    "crc_bulk_bytes",
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "reactor_reads",
    "reactor_read_bytes",
)
DATA = (
    "blocks",
    "data_bytes",
    "stride_bytes",
    "stride_records",
    "anchors",
    "dataset_digest",
)
CACHE = (
    "cache_capacity",
    "cache_bytes",
    "cache_loads",
    "cache_waits",
    "cache_uncached",
    "cache_refused",
    "cache_evicted",
)
COMMON_FLAGS = ("timing_only", "ndebug")
LAGS = (0, 2, 8)
READS = ("reads", "batches", "returned_bytes")
SEALED_FLAGS = COMMON_FLAGS + ("segment_filled",)
TAIL = (
    (
        "groups",
        "group_blocks",
        "stride_bytes",
        "append_ns",
        "read_ns",
        "scan_ns",
        "scanned_batches",
        "feed_dropped",
        "foreground_turns",
        "longest_wait_ns",
    )
    + tuple(
        f"lag{lag}_{what}" for lag in LAGS for what in ("reads", "hits", "device_reads")
    )
    + MEASURED
    + CACHE,
    COMMON_FLAGS,
)
# case -> side -> (counters, flags)
SEALED = (DATA + READS + MEASURED + CACHE, SEALED_FLAGS)
# A stride row also says its read-ahead: how many runs, whether that grew,
# and how many runs were asked for ahead.
STRIDE = (
    DATA + ("depth",) + READS + ("asked_ahead",) + MEASURED + CACHE,
    SEALED_FLAGS + ("grows",),
)
CASES = {
    "read_seek": {
        side: (DATA + ("seeks", "returned_bytes") + MEASURED + CACHE, SEALED_FLAGS)
        for side in ("cold", "warm")
    },
    "read_stride": {
        "scan": STRIDE,
        "seek": STRIDE,
        "skipped": (("stride_bytes",), COMMON_FLAGS),
    },
    "read_ahead": {
        side: (
            DATA + ("depth",) + READS + ("asked_ahead",) + MEASURED + CACHE,
            SEALED_FLAGS,
        )
        for side in ("fixed", "growing")
    },
    "read_held": {"held": SEALED, "kept": SEALED},
    "read_tail": {side: TAIL for side in ("unfed", "untold", "told")},
    "read_tail_scanned": {side: TAIL for side in ("untold", "told")},
}


def integer(value, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid {name}")
    return value


def flag(value, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"invalid {name}")
    return value


def validate(row: dict) -> dict:
    case, side = row.get("case"), row.get("side")
    if case not in CASES or side not in CASES[case]:
        raise ValueError("unknown case or side")
    counters, flags = CASES[case][side]
    if set(row) != {"case", "side", *counters, *flags}:
        raise ValueError(f"{case}.{side} fields do not match its case")
    for name in counters:
        integer(row[name], name)
    for name in flags:
        flag(row[name], name)
    check(row)
    return row


def check(row: dict) -> None:
    """What a row's own case says of it."""
    case, side = row["case"], row["side"]
    where = f"{case}.{side}"
    # Reads at the device, and what is derived from them.
    observed = not row["timing_only"]
    if case == "read_seek":
        if observed and side == "warm" and (row["data_reads"] or row["index_reads"]):
            raise ValueError(f"{where} read a file")
        if observed and side == "cold" and not row["data_reads"]:
            raise ValueError(f"{where} read no data")
        if side == "warm" and row["reactor_reads"]:
            raise ValueError(f"{where} read the device")
        if side == "warm" and row["cache_loads"]:
            raise ValueError(f"{where} loaded what was kept")
        if side == "cold" and not row["cache_loads"]:
            raise ValueError(f"{where} loaded nothing")
        if row["cache_uncached"] or not row["seeks"]:
            raise ValueError(f"{where} was not read through the cache")
    if case == "read_stride" and side == "scan":
        if row["batches"] != row["blocks"]:
            raise ValueError(f"{where} did not return every batch")
    if case == "read_stride" and side != "skipped":
        if row["depth"] > 4 or (row["depth"] == 0 and row["asked_ahead"]):
            raise ValueError(f"{where} asked ahead where none was to be")
        if side == "seek" and row["depth"]:
            raise ValueError(f"{where} looked ahead of a seek")
    if case == "read_ahead":
        if row["batches"] != row["blocks"]:
            raise ValueError(f"{where} did not return every batch")
        if row["depth"] > 4 or (row["depth"] == 0 and row["asked_ahead"]):
            raise ValueError(f"{where} asked ahead where none was to be")
        # A scan of more than two runs that may look ahead and never did
        # measured a scan with nothing ahead under another name.
        if row["depth"] and row["anchors"] > 2 and not row["asked_ahead"]:
            raise ValueError(f"{where} asked for nothing ahead")
    if case == "read_held":
        if row["batches"] != row["blocks"]:
            raise ValueError(f"{where} did not return every batch")
        if side == "held" and (
            not row["cache_uncached"] or row["cache_uncached"] != row["cache_loads"]
        ):
            raise ValueError(f"{where} kept a run it had no room for")
        if side == "kept" and row["cache_uncached"]:
            raise ValueError(f"{where} read a run alone where there was room")
    if case in ("read_tail", "read_tail_scanned"):
        for lag in LAGS:
            reads, hits = row[f"lag{lag}_reads"], row[f"lag{lag}_hits"]
            if hits > reads or reads != max(0, row["groups"] - lag):
                raise ValueError(f"{where} lag {lag} does not add up")
        if observed and side == "told" and row["lag0_device_reads"]:
            raise ValueError(f"{where} read a fed tail from the device")
        if observed and side == "unfed" and row["lag0_hits"]:
            raise ValueError(f"{where} hit a tail nothing fed")
        if case == "read_tail" and (row["scan_ns"] or row["scanned_batches"]):
            raise ValueError(f"{where} scanned where no scan was to be")
        if case == "read_tail_scanned" and not row["scanned_batches"]:
            raise ValueError(f"{where} scanned nothing beside the tail")


def read_measurements(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith(PREFIX):
            continue
        try:
            row = json.loads(line[len(PREFIX) :])
        except json.JSONDecodeError as error:
            raise ValueError("malformed read measurement") from error
        if not isinstance(row, dict):
            raise ValueError("malformed read measurement")
        rows.append(validate(row))
    if not rows:
        raise ValueError("no read measurements")
    return rows


def per(total: int, count: int) -> float:
    return total / count if count else 0.0


def group_of(row: dict) -> tuple:
    """What two rows must share to be samples of one measurement."""
    case = row["case"]
    key = [case, row["side"], row["timing_only"], row["ndebug"]]
    if case in ("read_tail", "read_tail_scanned"):
        # A row taken beside the fiber that probes for stalls measured
        # another machine: that fiber takes half of it.
        key += [
            row["groups"],
            row["group_blocks"],
            row["stride_bytes"],
            row["cache_capacity"],
            bool(row["foreground_turns"]),
        ]
    elif row["side"] == "skipped":
        key += [row["stride_bytes"]]
    else:
        key += [row["dataset_digest"], row["stride_bytes"]]
        if case in ("read_ahead", "read_stride"):
            key += [row["depth"]]
        if case == "read_stride":
            key += [row["grows"]]
    return tuple(key)


def median(rows: list[dict], name: str) -> float:
    return statistics.median(row[name] for row in rows)


def summarize_group(rows: list[dict]) -> dict:
    first = rows[0]
    case, side = first["case"], first["side"]
    out = {"case": case, "side": side, "samples": len(rows)}
    if side == "skipped":
        out["stride_bytes"] = first["stride_bytes"]
        return out
    # A timing row says nothing of the device, and its summary leaves that
    # out rather than print zeroes.
    observed = not first["timing_only"]
    out["build"] = "observed" if observed else "timing"
    out["elapsed_ns"] = median(rows, "elapsed_ns")
    out["elapsed_least_ns"] = min(row["elapsed_ns"] for row in rows)
    out["elapsed_most_ns"] = max(row["elapsed_ns"] for row in rows)
    out["cpu_ns"] = median(rows, "cpu_ns")
    out["reactor_reads"] = median(rows, "reactor_reads")
    out["reactor_read_bytes"] = median(rows, "reactor_read_bytes")
    if observed:
        out["data_reads"] = median(rows, "data_reads")
        out["index_reads"] = median(rows, "index_reads")
    if case in ("read_tail", "read_tail_scanned"):
        appended = first["groups"] * first["group_blocks"]
        out["append_ns_per_block"] = per(median(rows, "append_ns"), appended)
        out["read_ns"] = median(rows, "read_ns")
        out["probed"] = bool(first["foreground_turns"])
        out["feed_dropped"] = median(rows, "feed_dropped")
        out["cache_evicted"] = median(rows, "cache_evicted")
        out["longest_wait_ns"] = max(row["longest_wait_ns"] for row in rows)
        for lag in LAGS if observed else ():
            out[f"lag{lag}_hit_rate"] = per(
                sum(row[f"lag{lag}_hits"] for row in rows),
                sum(row[f"lag{lag}_reads"] for row in rows),
            )
        return out
    out["stride_bytes"] = first["stride_bytes"]
    returned = median(rows, "returned_bytes")
    read_bytes = median(rows, "data_read_bytes")
    if observed:
        # What was read and what was checked for each byte handed out.
        out["read_bytes_per_returned"] = per(read_bytes, returned)
        out["checked_bytes_per_returned"] = per(
            median(rows, "crc_bulk_bytes") + median(rows, "hashed_bytes"), returned
        )
    out["cache_loads"] = median(rows, "cache_loads")
    out["cache_uncached"] = median(rows, "cache_uncached")
    seeking = case == "read_seek" or (case == "read_stride" and side == "seek")
    if seeking:
        seeks = first["seeks"] if case == "read_seek" else first["reads"]
        out["ns_per_seek"] = per(out["elapsed_ns"], seeks)
        out["reactor_reads_per_seek"] = per(out["reactor_reads"], seeks)
        if observed:
            if case == "read_seek":
                out["reads_per_seek"] = per(
                    out["data_reads"] + out["index_reads"], seeks
                )
            out["read_bytes_per_seek"] = per(read_bytes, seeks)
            # The runs loaded: the seeks that were cold. The routes and
            # the page of the index are the other loads, one read each.
            runs = max(0.0, out["cache_loads"] - out["index_reads"])
            out["runs_loaded"] = runs
            out["ns_per_run_loaded"] = per(out["elapsed_ns"], runs)
            out["read_bytes_per_run_loaded"] = per(read_bytes, runs)
            out["data_reads_per_run_loaded"] = per(out["data_reads"], runs)
    else:
        out["ns_per_batch"] = per(out["elapsed_ns"], first["batches"])
    if case in ("read_ahead", "read_stride"):
        out["depth"] = first["depth"]
        out["asked_ahead"] = median(rows, "asked_ahead")
        out["cache_waits"] = median(rows, "cache_waits")
    if case == "read_stride":
        out["grows"] = first["grows"]
    return out


def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault(group_of(row), []).append(row)
    return [summarize_group(groups[key]) for key in sorted(groups, key=repr)]


def render(summaries: list[dict]) -> str:
    lines = []
    for summary in summaries:
        head = f"{summary['case']}.{summary['side']}"
        rest = ", ".join(
            f"{name}={value:.3f}" if isinstance(value, float) else f"{name}={value}"
            for name, value in summary.items()
            if name not in ("case", "side")
        )
        lines.append(f"{head}: {rest}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, nargs="+")
    parser.add_argument("--json", action="store_true")
    arguments = parser.parse_args()
    rows = []
    for path in arguments.log:
        rows += read_measurements(path)
    summaries = summarize(rows)
    if arguments.json:
        print(json.dumps(summaries, indent=2, sort_keys=True))
    else:
        print(render(summaries), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
