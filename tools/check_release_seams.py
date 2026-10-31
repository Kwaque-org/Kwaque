"""A release binary carries no simulation seam.

Production code reaches BUGGIFY sites and coverage marks only through the
backend's constants, which are false for the production backend, so every call
is a discarded branch. The release-built broker then defines no symbol in
kwaque::simulation and none of the site or mark tables, and its read-only data
holds no site or mark name. The same check must find, in a simulation binary
built the same way, a simulation symbol, each table that has an entry and
each name, so an empty answer cannot come from a check that sees nothing.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from bazel.native_test_environment import resolve_runfile

SEAM_SYMBOL = re.compile(
    r"\bkwaque::simulation::|\bkwaque::runtime::(?:buggify_sites|coverage_marks)\b"
)
# The sections a name in a table would live in. Release builds keep debug
# information (`--strip=never`), and DWARF may name an enumerator no code uses,
# so the debug sections are not read.
READ_ONLY_SECTIONS = (".rodata", ".data.rel.ro")
NAME_LINE = re.compile(r"^(buggify_site|coverage_mark) ([a-z0-9_]{1,48})$")
REPORTED = 10


def parse_symbols(nm_output: str) -> list[str]:
    """Defined symbol names from `nm --defined-only --demangle` lines."""
    symbols = []
    for line in nm_output.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) == 3:
            symbols.append(fields[2])
    return symbols


def parse_names(control_output: str) -> list[str]:
    """The site and mark names the positive control prints."""
    names = []
    for line in control_output.splitlines():
        match = NAME_LINE.fullmatch(line)
        if match is None:
            continue
        names.append(match.group(2))
    return names


# The table each kind of name the control prints comes from.
TABLE_SYMBOLS = {
    "buggify_site": "kwaque::runtime::buggify_sites",
    "coverage_mark": "kwaque::runtime::coverage_marks",
}


def parse_tables(control_output: str) -> list[str]:
    """The table symbols of the kinds of name the positive control prints."""
    kinds = set()
    for line in control_output.splitlines():
        match = NAME_LINE.fullmatch(line)
        if match is not None:
            kinds.add(match.group(1))
    return sorted(TABLE_SYMBOLS[kind] for kind in kinds)


def contains_name(data: bytes, name: str) -> bool:
    """A table's name is a string literal: the word alone, then a NUL."""
    pattern = re.compile(
        rb"(?<![a-z0-9_])" + re.escape(name.encode("ascii")) + rb"\x00"
    )
    return pattern.search(data) is not None


def seam_errors(
    release_symbols: list[str],
    release_data: bytes,
    control_symbols: list[str],
    control_data: bytes,
    names: list[str],
    tables: Sequence[str] = (),
) -> list[str]:
    errors = []
    if not any(SEAM_SYMBOL.search(symbol) for symbol in control_symbols):
        errors.append("the positive control defines no simulation symbol")
    absent = [table for table in tables if table not in control_symbols]
    if absent:
        errors.append("the positive control defines no table: " + " ".join(absent))
    missing = [name for name in names if not contains_name(control_data, name)]
    if missing:
        errors.append(
            "the positive control's read-only data lacks: " + " ".join(missing)
        )
    found = [symbol for symbol in release_symbols if SEAM_SYMBOL.search(symbol)]
    if found:
        errors.append(
            f"the release binary defines {len(found)} simulation symbol(s): "
            + "; ".join(found[:REPORTED])
        )
    present = [name for name in names if contains_name(release_data, name)]
    if present:
        errors.append("the release binary's read-only data names: " + " ".join(present))
    return errors


def defined_symbols(nm: Path, binary: Path) -> list[str]:
    output = subprocess.run(
        [str(nm), "--defined-only", "--demangle", str(binary)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return parse_symbols(output)


def read_only_data(objcopy: Path, binary: Path) -> bytes:
    """The raw bytes of each read-only section, a NUL between sections."""
    chunks = []
    with tempfile.TemporaryDirectory() as directory:
        for index, section in enumerate(READ_ONLY_SECTIONS):
            output = Path(directory) / f"section-{index}"
            subprocess.run(
                [
                    str(objcopy),
                    "-O",
                    "binary",
                    f"--only-section={section}",
                    str(binary),
                    str(output),
                ],
                check=True,
            )
            chunks.append(output.read_bytes() if output.exists() else b"")
    return b"\x00".join(chunks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nm", required=True)
    parser.add_argument("--objcopy", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--control", required=True)
    arguments = parser.parse_args(argv)
    nm = resolve_runfile(arguments.nm)
    objcopy = resolve_runfile(arguments.objcopy)
    release = resolve_runfile(arguments.release)
    control = resolve_runfile(arguments.control)
    printed = subprocess.run(
        [str(control)], check=True, capture_output=True, text=True
    ).stdout
    names = parse_names(printed)
    errors = seam_errors(
        defined_symbols(nm, release),
        read_only_data(objcopy, release),
        defined_symbols(nm, control),
        read_only_data(objcopy, control),
        names,
        parse_tables(printed),
    )
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(
        f"The release binary holds no simulation symbol and none of {len(names)} "
        "site and mark name(s); the positive control holds them"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
