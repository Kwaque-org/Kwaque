"""A timing benchmark registers every case its pair list names.

The list is what a paired run is given. A case renamed in the benchmark and
not in the list would otherwise be found only when that run reaches it.
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

KINDS = ("device", "processor")
REACTOR_ARGUMENTS = (
    f"--reactor-backend={os.environ.get('KWAQUE_REACTOR_BACKEND', 'epoll')}",
    "--max-networking-io-control-blocks=1000",
    "--smp=1",
    "--memory=128M",
    "--overprovisioned",
    "--blocked-reactor-notify-ms=2000000",
)


def read_pairs(path: Path) -> list[tuple[str, str, str]]:
    """Each `<baseline>=<candidate> <kind>` line as its three parts."""
    pairs = []
    for number, line in enumerate(path.read_text(encoding="ascii").splitlines(), 1):
        fields = line.split()
        if not fields or fields[0].startswith("#"):
            continue
        names = fields[0].split("=")
        if len(fields) != 2 or len(names) != 2 or fields[1] not in KINDS:
            raise ValueError(f"{path.name}:{number}: not a pair and its kind")
        pairs.append((names[0], names[1], fields[1]))
    return pairs


def listed_cases(output: str) -> set[str]:
    """The names a benchmark binary prints for --list, one a tabbed line."""
    lines = output.splitlines()
    if "available tests:" not in lines:
        raise ValueError("the benchmark did not list its cases")
    return {
        line[1:]
        for line in lines[lines.index("available tests:") + 1 :]
        if line.startswith("\t")
    }


def kind_of(name: str) -> str | None:
    """What bounds a case's timed region, as its registered name says."""
    group, _, case = name.partition(".")
    if group == "wal_commit_bench":
        # Every cohort of the commit group ends at a barrier on the device.
        return "device"
    if group == "wal_bench" and case.endswith("_disk_barrier"):
        return "device"
    if group == "wal_bench" and case.endswith("_noop_written"):
        # The decorator discards the write; nothing reaches the device.
        return "processor"
    if group in ("baseline_append", "candidate_append"):
        # Every request ends at the WAL's barrier and its segment's.
        return "device"
    if group in ("baseline_index", "candidate_index") and case in (
        "warm_lookup",
        "cold_open_lookup",
    ):
        # Each lookup that finds an anchor reads its page from the device.
        return "device"
    return None


class BenchPairsTest(unittest.TestCase):
    def test_every_pair_names_registered_cases_of_its_kind(self) -> None:
        binary, pairs_file = Path(sys.argv[1]), Path(sys.argv[2])
        pairs = read_pairs(pairs_file)
        self.assertTrue(pairs, "the pair list is empty")
        names = [
            name for baseline, candidate, _ in pairs for name in (baseline, candidate)
        ]
        self.assertEqual(
            sorted(names), sorted(set(names)), "a case is named more than once"
        )
        listed = subprocess.run(
            [binary, "--list", *REACTOR_ARGUMENTS],
            capture_output=True,
            check=False,
            text=True,
            timeout=120.0,
        )
        self.assertEqual(listed.returncode, 0, listed.stdout + listed.stderr)
        registered = listed_cases(listed.stdout)
        self.assertEqual(
            sorted(set(names) - registered), [], "the benchmark registers no such case"
        )
        for baseline, candidate, kind in pairs:
            with self.subTest(baseline=baseline, candidate=candidate):
                self.assertEqual(
                    (kind_of(baseline), kind_of(candidate)),
                    (kind, kind),
                    "a pair's kind disagrees with its cases",
                )


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
