"""Validate and summarize sparse index measurements.

Each row is one unit of work over a sealed segment the segment writer wrote:
lookups through the owner of its index, the same lookups composed without
the owner, the same lookups in an index held whole in memory, many segments
found again at once, one rebuild from the segment's data, and one stride of
a sweep. A row carries the data it was measured over. Rows of one case that
looked the same offsets up in the same data must have found the same
anchors, on every side and in every run; rows that did not are rejected
rather than averaged. The sides of a case are set beside each other only
when every one of them is present and was measured over the same data; a
log that holds some of them is summarized only when a partial report is
asked for.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

PREFIX = "index_bench_v1 "
TIB = 1 << 40
SIDES = ("candidate", "baseline", "resident")
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
)
BUDGET = ("budget_bytes", "budget_handles", "budget_tasks")
DATA = (
    "blocks",
    "data_bytes",
    "stride_bytes",
    "stride_records",
    "anchors",
    "dataset_digest",
)
LOOKUPS = (
    "lookups",
    "found",
    "result_digest",
    "near_result_digest",
    "far_lookups",
    "far_at_first_block",
    "scan_bytes",
    "longest_scan_bytes",
    "scan_bound_bytes",
    "longest_scan_bound_bytes",
)
COMMON_FLAGS = ("timing_only", "ndebug")
OBSERVED_MEMORY = (
    "open_live_bytes",
    "open_peak_bytes",
    "lookup_peak_bytes",
    "lookup_largest_allocation",
)
OBSERVED_MEMORY_FLAGS = ("open_memory_complete", "lookup_memory_complete")


def prefixed(prefix: str, names) -> tuple[str, ...]:
    return tuple(prefix + name for name in names)


LOOKUP_CASE = (
    DATA + LOOKUPS + MEASURED + prefixed("held_", BUDGET),
    COMMON_FLAGS + ("segment_filled",),
)
# case -> (counters, flags)
CASES = {
    "warm_lookup": LOOKUP_CASE,
    "cold_open_lookup": LOOKUP_CASE,
    "memory_lookup": (
        DATA + ("admitted_bytes",) + LOOKUPS + MEASURED,
        COMMON_FLAGS + ("segment_filled",),
    ),
    "open_many_segments": (
        (
            "segments",
            "blocks_per_segment",
            "data_bytes",
            "anchors",
            "open_root_bound",
            "roots_left_open",
            "root_loads",
        )
        + MEASURED
        + prefixed("held_", BUDGET)
        + prefixed("most_", BUDGET),
        COMMON_FLAGS,
    ),
    "rebuild": (
        DATA
        + LOOKUPS
        + MEASURED
        + prefixed("persist_", MEASURED)
        + prefixed("memory_", BUDGET)
        + ("foreground_turns", "foreground_longest_wait_ns"),
        COMMON_FLAGS + ("segment_filled", "foreground_probe"),
    ),
    "stride": (
        DATA
        + (
            "pages",
            "bundle_bytes",
            "root_bytes",
            "capacity_at_1gib",
            "admitted_at_1gib_bytes",
        )
        + LOOKUPS
        + MEASURED
        + prefixed("open_", MEASURED)
        + prefixed("cold_lookup_", MEASURED)
        + prefixed("root_", BUDGET),
        COMMON_FLAGS + ("segment_filled", "memory_observed"),
    ),
}
RESIDENT_EXTRA = ("resident_column_bytes", "resident_file_bytes")
# Which sides measure a case.
CASE_SIDES = {
    "warm_lookup": SIDES,
    "cold_open_lookup": SIDES,
    "memory_lookup": ("candidate",),
    "open_many_segments": ("candidate", "resident"),
    "rebuild": ("candidate", "resident"),
    "stride": ("candidate",),
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
    if case not in CASES or side not in CASE_SIDES.get(case, ()):
        raise ValueError("unknown case or side")
    counters, flags = CASES[case]
    expected = {"case", "side", *counters, *flags}
    if case in ("warm_lookup", "cold_open_lookup") and side == "resident":
        expected.update(RESIDENT_EXTRA)
        counters = counters + RESIDENT_EXTRA
    if case == "stride" and row.get("memory_observed") is True:
        expected.update(OBSERVED_MEMORY, OBSERVED_MEMORY_FLAGS)
        counters = counters + OBSERVED_MEMORY
        flags = flags + OBSERVED_MEMORY_FLAGS
    if set(row) != expected:
        raise ValueError(f"{case} row has other fields than its case")
    for name in counters:
        integer(row[name], name)
    for name in flags:
        flag(row[name], name)
    if "lookups" in row:
        if row["found"] > row["lookups"] or row["far_lookups"] > row["found"]:
            raise ValueError("more lookups answered than asked")
        if row["far_at_first_block"] > row["far_lookups"]:
            raise ValueError("more far lookups at the first block than asked")
    if "blocks" in row and (row["blocks"] == 0 or row["anchors"] == 0):
        raise ValueError("a segment without a block or an anchor")
    if "blocks" in row and row["anchors"] > row["blocks"]:
        raise ValueError("more anchors than blocks")
    if not row["timing_only"]:
        check_native_work(row)
    return row


def check_native_work(row: dict) -> None:
    """What an observed interval may and may not touch."""
    case, side = row["case"], row["side"]
    if case in ("warm_lookup", "cold_open_lookup", "open_many_segments"):
        # An index is never checked against its segment's data to be used.
        if row["data_reads"] or row["data_opens"] or row["index_writes"]:
            raise ValueError("a lookup read segment data or wrote")
    if case == "warm_lookup" and row["index_opens"]:
        raise ValueError("a warm lookup opened a file")
    if case == "cold_open_lookup" and row["index_opens"] != 1:
        raise ValueError("a cold lookup did not open exactly one file")
    if case == "warm_lookup" and side == "resident" and row["index_reads"]:
        raise ValueError("a resident lookup read the device")
    if case in ("warm_lookup", "cold_open_lookup") and side != "resident":
        # One page for every lookup that has an anchor, and the root once
        # where it is opened: nothing keeps a page between lookups.
        opened = 1 if case == "cold_open_lookup" else 0
        if row["index_reads"] != row["found"] + opened:
            raise ValueError("a lookup did not read exactly one page")
    if case == "memory_lookup" and (
        row["index_reads"] or row["data_reads"] or row["index_opens"]
    ):
        raise ValueError("a lookup in memory read the device")
    if case == "open_many_segments":
        if row["index_opens"] != row["segments"]:
            raise ValueError("a segment's index was not opened exactly once")
        if side == "candidate":
            if row["root_loads"] != row["segments"]:
                raise ValueError("a root was opened more than once")
            if row["roots_left_open"] > row["open_root_bound"]:
                raise ValueError("more roots stayed open than the bound")
            if row["most_budget_handles"] > row["open_root_bound"]:
                raise ValueError("more roots were open at once than the bound")
    if case == "rebuild":
        if row["index_reads"] or row["index_writes"] or not row["data_reads"]:
            raise ValueError("a rebuild did not read the data alone")
        if not row["persist_index_flushes"]:
            raise ValueError("an index was made durable without a flush")
    if case == "stride":
        if row["index_reads"] or not row["data_reads"]:
            raise ValueError("a rebuild did not read the data alone")
        if row["open_index_opens"] != 1 or row["open_data_reads"]:
            raise ValueError("opening an index did not open its bundle alone")
        if row["cold_lookup_index_opens"] or row["cold_lookup_data_reads"]:
            raise ValueError("a lookup in an open index opened or read data")


def read_measurements(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith(PREFIX):
            continue
        try:
            row = json.loads(line[len(PREFIX) :])
        except json.JSONDecodeError as error:
            raise ValueError("malformed measurement row") from error
        if not isinstance(row, dict):
            raise ValueError("malformed measurement row")
        rows.append(validate(row))
    return rows


def dataset_of(row: dict) -> tuple:
    """What a row was measured over; rows are compared only within one."""
    if row["case"] == "open_many_segments":
        return (row["segments"], row["blocks_per_segment"], row["data_bytes"])
    return (row["dataset_digest"], row["stride_bytes"], row["stride_records"])


def check_agreement(rows: list[dict]) -> None:
    """Every run and every side answered the same lookups the same way."""
    answered: dict[tuple, dict[str, int]] = {}
    for row in rows:
        if "result_digest" not in row:
            continue
        # Each of these cases draws the same offsets from the same seed.
        key = dataset_of(row)
        seen = answered.setdefault(key, {})
        whole = row["side"] != "resident"
        for name in ("near_result_digest",) + (("result_digest",) if whole else ()):
            if seen.setdefault(name, row[name]) != row[name]:
                raise ValueError(
                    f"{row['case']} {row['side']} found other anchors in the same data"
                )
        if seen.setdefault("anchors", row["anchors"]) != row["anchors"]:
            raise ValueError("the same data was indexed with other anchors")
        if whole and row["far_at_first_block"] and row["anchors"] > 1:
            raise ValueError("a far offset was answered with the first block")


def check_sides(rows: list[dict], partial: bool) -> None:
    """The sides of a case are compared over one dataset, or not at all."""
    measured: dict[tuple, dict[str, set]] = {}
    for row in rows:
        if row["case"] == "stride":
            continue
        sides = measured.setdefault((row["case"], row["timing_only"]), {})
        sides.setdefault(row["side"], set()).add(dataset_of(row))
    for (case, _), sides in measured.items():
        if len(set().union(*sides.values())) != 1:
            raise ValueError(f"the sides of {case} were measured over other data")
        missing = [side for side in CASE_SIDES[case] if side not in sides]
        if missing and not partial:
            raise ValueError(
                f"{case} lacks {', '.join(missing)}: its sides are not compared"
            )


def per(total: int, count: int) -> float:
    return total / count if count else 0.0


def median(rows: list[dict], name: str) -> float:
    return statistics.median(row[name] for row in rows)


def one(rows: list[dict], name: str) -> int:
    values = {row[name] for row in rows}
    if len(values) != 1:
        raise ValueError(f"runs of one case disagree on {name}")
    return values.pop()


def summarize_group(rows: list[dict]) -> dict:
    first = rows[0]
    case = first["case"]
    out = {
        "case": case,
        "side": first["side"],
        "runs": len(rows),
        "timing_only": one(rows, "timing_only"),
        "elapsed_ns": {
            "median": median(rows, "elapsed_ns"),
            "minimum": min(row["elapsed_ns"] for row in rows),
            "maximum": max(row["elapsed_ns"] for row in rows),
        },
        "allocations": median(rows, "allocations"),
        "tasks": median(rows, "tasks"),
        "index_reads": median(rows, "index_reads"),
        "index_read_bytes": median(rows, "index_read_bytes"),
        "data_read_bytes": median(rows, "data_read_bytes"),
    }
    for name in ("blocks", "data_bytes", "anchors", "stride_bytes", "segments"):
        if name in first:
            out[name] = one(rows, name)
    if "lookups" in first:
        lookups, found = one(rows, "lookups"), one(rows, "found")
        out["lookups"] = lookups
        out["found"] = found
        out["elapsed_ns_per_lookup"] = per(out["elapsed_ns"]["median"], lookups)
        out["allocations_per_lookup"] = per(out["allocations"], lookups)
        out["tasks_per_lookup"] = per(out["tasks"], lookups)
        # A lookup below the first anchor reads nothing.
        out["index_reads_per_found"] = per(out["index_reads"], found)
        out["index_read_bytes_per_found"] = per(out["index_read_bytes"], found)
        out["mean_scan_bytes"] = per(one(rows, "scan_bytes"), found)
        out["longest_scan_bytes"] = one(rows, "longest_scan_bytes")
        out["far_lookups"] = one(rows, "far_lookups")
        out["far_at_first_block"] = one(rows, "far_at_first_block")
    if case in ("warm_lookup", "cold_open_lookup"):
        out["held_budget_bytes"] = median(rows, "held_budget_bytes")
        out["held_budget_handles"] = median(rows, "held_budget_handles")
        if first["side"] == "resident":
            columns = one(rows, "resident_column_bytes")
            out["resident_column_bytes"] = columns
            out["resident_file_bytes"] = one(rows, "resident_file_bytes")
            # At this row's stride, which is one anchor a block here.
            out["resident_bytes_per_tib"] = per(columns * TIB, out["data_bytes"])
    if case == "open_many_segments":
        segments = out["segments"]
        out["elapsed_ns_per_segment"] = per(out["elapsed_ns"]["median"], segments)
        out["index_reads_per_segment"] = per(out["index_reads"], segments)
        out["index_read_bytes_per_segment"] = per(out["index_read_bytes"], segments)
        out["open_root_bound"] = one(rows, "open_root_bound")
        out["roots_left_open"] = one(rows, "roots_left_open")
        out["held_budget_bytes"] = median(rows, "held_budget_bytes")
        out["most_budget_bytes"] = max(row["most_budget_bytes"] for row in rows)
        out["most_budget_handles"] = max(row["most_budget_handles"] for row in rows)
    if case == "rebuild":
        out["read_amplification"] = per(out["data_read_bytes"], out["data_bytes"])
        out["hashed_bytes"] = median(rows, "hashed_bytes")
        out["memory_budget_bytes"] = median(rows, "memory_budget_bytes")
        out["persist_elapsed_ns"] = median(rows, "persist_elapsed_ns")
        out["persist_index_write_bytes"] = median(rows, "persist_index_write_bytes")
        out["persist_index_flushes"] = median(rows, "persist_index_flushes")
        out["persist_directory_syncs"] = median(rows, "persist_directory_syncs")
        if one(rows, "foreground_probe"):
            out["foreground_turns"] = median(rows, "foreground_turns")
            out["foreground_longest_wait_ns"] = max(
                row["foreground_longest_wait_ns"] for row in rows
            )
    if case == "stride":
        data = out["data_bytes"]
        out["pages"] = one(rows, "pages")
        out["bundle_bytes"] = one(rows, "bundle_bytes")
        out["root_bytes"] = one(rows, "root_bytes")
        out["capacity_at_1gib"] = one(rows, "capacity_at_1gib")
        out["admitted_at_1gib_bytes"] = one(rows, "admitted_at_1gib_bytes")
        out["read_amplification"] = per(out["data_read_bytes"], data)
        out["open_index_reads"] = median(rows, "open_index_reads")
        out["open_index_read_bytes"] = median(rows, "open_index_read_bytes")
        out["open_elapsed_ns"] = median(rows, "open_elapsed_ns")
        out["cold_lookup_index_reads"] = median(rows, "cold_lookup_index_reads")
        out["cold_lookup_index_read_bytes"] = median(
            rows, "cold_lookup_index_read_bytes"
        )
        out["cold_lookup_elapsed_ns"] = median(rows, "cold_lookup_elapsed_ns")
        out["root_budget_bytes"] = median(rows, "root_budget_bytes")
        # For a tebibyte of sealed log in segments of this size: the bytes
        # of the published bundles, and what an index held whole in memory
        # keeps resident for the same anchors, sixteen bytes each. The
        # published index keeps none of its anchors resident; what it holds
        # is its open roots, however much log there is.
        out["bundle_bytes_per_tib"] = per(out["bundle_bytes"] * TIB, data)
        out["resident_bytes_per_tib"] = per(out["anchors"] * 16 * TIB, data)
        if one(rows, "memory_observed"):
            out["open_live_bytes"] = max(row["open_live_bytes"] for row in rows)
            out["lookup_peak_bytes"] = max(row["lookup_peak_bytes"] for row in rows)
            out["memory_complete"] = all(
                row["open_memory_complete"] and row["lookup_memory_complete"]
                for row in rows
            )
    return out


def summarize(rows: list[dict], partial: bool = False) -> list[dict]:
    if not rows:
        raise ValueError("no measurement rows")
    check_sides(rows, partial)
    check_agreement(rows)
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["case"], row["side"], row["timing_only"], dataset_of(row))
        groups.setdefault(key, []).append(row)
    return [summarize_group(group) for group in groups.values()]


def render(summaries: list[dict]) -> str:
    lines = []
    for item in summaries:
        build = "timing" if item["timing_only"] else "observed"
        head = f"{item['case']} [{item['side']}, {build}, {item['runs']} runs]"
        elapsed = item["elapsed_ns"]
        lines.append(head)
        if "blocks" in item:
            lines.append(
                f"  data: {item['blocks']} blocks, {item['data_bytes']} bytes, "
                f"stride {item['stride_bytes']} bytes, {item['anchors']} anchors"
            )
        lines.append(
            f"  time: median {elapsed['median']:.0f} ns "
            f"({elapsed['minimum']} to {elapsed['maximum']})"
        )
        for name, value in item.items():
            if name in (
                "case",
                "side",
                "runs",
                "timing_only",
                "elapsed_ns",
                "blocks",
                "data_bytes",
                "stride_bytes",
                "anchors",
            ):
                continue
            shown = f"{value:.2f}" if isinstance(value, float) else str(value)
            lines.append(f"  {name}: {shown}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    parser.add_argument(
        "--partial",
        action="store_true",
        help="summarize a log that holds only some sides of a case",
    )
    arguments = parser.parse_args()
    rows = []
    for path in arguments.logs:
        rows.extend(read_measurements(path))
    summaries = summarize(rows, arguments.partial)
    if arguments.json:
        print(json.dumps(summaries, indent=2, sort_keys=True))
    else:
        print(render(summaries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
