"""Validate and summarize WAL saturation searches.

A search steps the rate at which one-request groups are offered to the group
commit coordinator. A step holds one rate for a period and fails when more than
a tenth of the requests it planned were not reported durable by the end of
that period and one further period, or when none was; a refused request is
counted and is not achieved. Each pass raises the rate and the first failing
step ends the search. A search that meets no failing step inside its bound
claims no limit.

The binary prints one row a step and one final row. This script rejects a row
whose counts, verdict or rate do not follow from one another, and a search
whose rows do not follow the rule its final row states.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__:
    from .wal_cohort_report import PROFILE_KEYS
else:
    from wal_cohort_report import PROFILE_KEYS

PREFIX = "wal_saturation_v1 "
STEP_COUNTS = (
    "step",
    "offered_rate",
    "offered",
    "accepted",
    "refused",
    "completed",
    "elapsed_ns",
    "reactor_flushes",
    "reactor_writes",
    "reactor_write_bytes",
)
SETTINGS = ("first_rate", "increment", "step_requests", "step_ns", "maximum_steps")
OUTCOMES = ("limit", "not_reached")
RATES = ("last_passing_rate", "first_failing_rate")
# What ended a search that met no failing step.
BOUNDS = ("steps", "room")
NANOSECONDS = 10**9
# A printed rate keeps three decimal places.
RATE_TOLERANCE = 0.001


def whole(row: dict, keys: tuple[str, ...]) -> bool:
    return all(type(row.get(key)) is int and row[key] >= 0 for key in keys)


def passes(offered: int, completed: int) -> bool:
    """A step passes unless more than a tenth of its plan is missing or
    nothing completed at all."""
    return completed > 0 and 10 * (offered - completed) <= offered


def validate_step(row: dict, number: int) -> None:
    if not whole(row, STEP_COUNTS) or type(row.get("pass")) is not bool:
        raise ValueError("step row lacks a count or its verdict")
    if row["step"] != number:
        raise ValueError("steps are not numbered in order from one")
    if not row["offered_rate"] or not row["offered"] or not row["elapsed_ns"]:
        raise ValueError("step offered nothing or took no time")
    if row["accepted"] + row["refused"] != row["offered"]:
        raise ValueError("accepted and refused requests are not the offered ones")
    if row["completed"] > row["accepted"]:
        raise ValueError("more requests completed than were accepted")
    if row["pass"] != passes(row["offered"], row["completed"]):
        raise ValueError("verdict disagrees with the one-tenth rule")
    # The rate is held for the whole planned period before the step is judged.
    if row["elapsed_ns"] < row["offered"] * NANOSECONDS // row["offered_rate"]:
        raise ValueError("step ended before its period")
    achieved = row.get("achieved_rate")
    if (
        type(achieved) not in (int, float)
        or abs(achieved - row["completed"] * NANOSECONDS / row["elapsed_ns"])
        > RATE_TOLERANCE
    ):
        raise ValueError("achieved rate is not completions over the step's time")
    # A request is reported durable only behind a flush the shard issued.
    if row["completed"] and not row["reactor_flushes"]:
        raise ValueError("step completed requests with no flush at the device")


def planned_requests(final: dict, rate: int) -> int:
    """The step's own count, or as many as fill its shortest period."""
    return max(final["step_requests"], -(-rate * final["step_ns"] // NANOSECONDS))


def validate_search(steps: list[dict], final: dict) -> None:
    if (
        not whole(final, (*SETTINGS, "steps"))
        or final.get("outcome") not in OUTCOMES
        or not all(final[key] for key in SETTINGS)
    ):
        raise ValueError("final row lacks its outcome or a setting")
    if not steps or final["steps"] != len(steps):
        raise ValueError("final row disagrees with the number of steps")
    if len(steps) > final["maximum_steps"]:
        raise ValueError("search ran past its step bound")
    rate = final["first_rate"]
    for index, row in enumerate(steps):
        validate_step(row, index + 1)
        if row["offered_rate"] != rate:
            raise ValueError("offered rate does not rise by the stated rule")
        if row["offered"] != planned_requests(final, rate):
            raise ValueError("step did not plan the stated requests")
        if not row["pass"] and row is not steps[-1]:
            raise ValueError("search went on after a failing step")
        # Each pass in a row doubles what the next one adds.
        rate += final["increment"] << index
    limit = final["outcome"] == "limit"
    if limit == steps[-1]["pass"]:
        raise ValueError("outcome disagrees with the last step")
    # A search whose first step fails has no passing rate, and one that met
    # no failing step has neither rate.
    expected = {}
    if limit:
        expected["first_failing_rate"] = steps[-1]["offered_rate"]
        if len(steps) > 1:
            expected["last_passing_rate"] = steps[-2]["offered_rate"]
    if {key: final[key] for key in RATES if key in final} != expected:
        raise ValueError("final rates disagree with the steps")
    # No limit is claimed without a failing step; the row says what ended it.
    if limit:
        if "bound" in final:
            raise ValueError("a limit was reached inside the bound")
    elif final.get("bound") not in BOUNDS or (final["bound"] == "steps") != (
        len(steps) == final["maximum_steps"]
    ):
        raise ValueError("search ended early without a failing step")


def read_searches(path: Path) -> list[dict]:
    profile: dict[str, str] = {}
    searches = []
    steps: list[dict] = []
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
        if not isinstance(row, dict) or row.get("row") not in ("step", "final"):
            raise ValueError(f"{path}: unknown saturation row")
        if row["row"] == "step":
            steps.append(row)
            continue
        if any(key not in profile for key in PROFILE_KEYS):
            raise ValueError(f"{path}: search lacks the effective build profile")
        validate_search(steps, row)
        searches.append({"steps": steps, "final": row, "build_profile": dict(profile)})
        steps = []
    if steps:
        raise ValueError(f"{path}: search has no final row")
    if not searches:
        raise ValueError(f"{path}: no completed saturation search")
    return searches


def summarize(search: dict) -> dict:
    steps, final = search["steps"], search["final"]
    result = {
        "outcome": final["outcome"],
        "steps": len(steps),
        **{key: final.get(key) for key in RATES},
        "bound": final.get("bound"),
        "settings": {key: final[key] for key in SETTINGS},
        "build_profile": search["build_profile"],
        "rates": [
            {
                "offered": row["offered_rate"],
                "achieved": row["achieved_rate"],
                "refused": row["refused"],
                "pass": row["pass"],
            }
            for row in steps
        ],
    }
    for key in ("offered", "accepted", "refused", "completed", *STEP_COUNTS[-3:]):
        result[key] = sum(row[key] for row in steps)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        result = [
            summarize(search) for path in args.logs for search in read_searches(path)
        ]
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
