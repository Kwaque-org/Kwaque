"""Validate and summarize concurrent synchronized writes to one device.

Each row is one case: N files on one device, each zero-written over every
byte its writes reach and flushed before the timed region, then opened for
synchronized writes, so each write is durable when it completes and needs no
flush. Every round writes the same bytes to each file at once and waits for
all of them; a row keeps every round's time. A summary sets each N beside
the first: how a round's time grows with the writes in flight, and the
synchronized writes a second the device took. Rows of different write sizes,
rounds or builds are summarized apart.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS, percentile
else:
    from wal_cohort_report import PROFILE_KEYS, percentile

PREFIX = "concurrent_dsync_v1 "
COUNTERS = (
    "files",
    "rounds",
    "write_bytes",
    "elapsed_ns",
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
    "write_p50_ns",
    "write_p99_ns",
    "write_max_ns",
)
KEYS = {
    *COUNTERS,
    "directory",
    "ndebug",
    "rounds_ns",
    "affinity_cpus",
    "build_profile",
}


def integer(value, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid {name}")
    return value


def validate(row: dict) -> None:
    if set(row) != KEYS:
        raise ValueError("unexpected or missing fields")
    for key in COUNTERS:
        integer(row[key], key)
    if row["files"] == 0 or row["rounds"] == 0 or row["write_bytes"] % 4096:
        raise ValueError("invalid case")
    if row["directory"] not in ("set", "test") or type(row["ndebug"]) is not bool:
        raise ValueError("invalid environment")
    if any(k not in row["build_profile"] for k in PROFILE_KEYS):
        raise ValueError("measurement lacks the effective build profile")
    rounds = row["rounds_ns"]
    if (
        not isinstance(rounds, list)
        or len(rounds) != row["rounds"]
        or any(type(r) is not int or r <= 0 for r in rounds)
        or sum(rounds) > row["elapsed_ns"]
    ):
        raise ValueError("rounds disagree with the case")
    # Every write was durable at completion: none needed a flush.
    if row["reactor_flushes"] != 0:
        raise ValueError("a synchronized write was flushed")
    writes = row["files"] * row["rounds"]
    if row["reactor_writes"] < writes or row["reactor_write_bytes"] < (
        writes * row["write_bytes"]
    ):
        raise ValueError("fewer writes reached the device than the case made")
    if not row["write_p50_ns"] <= row["write_p99_ns"] <= row["write_max_ns"]:
        raise ValueError("write quantiles out of order")
    if row["write_max_ns"] > max(rounds):
        raise ValueError("a write outlasted its round")
    affinity = row["affinity_cpus"]
    if (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        raise ValueError("invalid observed CPU affinity")


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
        raise ValueError(f"{path}: no completed concurrent dsync measurements")
    return rows


def setup(row: dict) -> tuple:
    return (
        row["write_bytes"],
        row["rounds"],
        row["directory"],
        tuple(sorted(row["build_profile"].items())),
    )


def summaries(rows: list[dict]) -> list[dict]:
    """Per write size and build, each N in order with its rounds pooled."""
    grouped: dict[tuple, dict[int, list[dict]]] = {}
    for row in rows:
        grouped.setdefault(setup(row), {}).setdefault(row["files"], []).append(row)
    result = []
    for key, by_files in grouped.items():
        widths = []
        base = None
        for files in sorted(by_files):
            same = by_files[files]
            rounds = [r for row in same for r in row["rounds_ns"]]
            p50 = percentile(rounds, 0.5)
            if base is None:
                base = p50
            writes = sum(row["files"] * row["rounds"] for row in same)
            elapsed = sum(row["elapsed_ns"] for row in same)
            widths.append(
                {
                    "files": files,
                    "runs": len(same),
                    "round_p50_ns": p50,
                    "round_p99_ns": percentile(rounds, 0.99),
                    "round_p50_over_first": p50 / base,
                    "writes_per_second": writes * 1e9 / elapsed,
                }
            )
        result.append(
            {
                "write_bytes": key[0],
                "rounds": key[1],
                "directory": key[2],
                "build_profile": dict(key[3]),
                "widths": widths,
            }
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        rows = [row for path in args.logs for row in read_measurements(path)]
        result = summaries(rows)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
