"""Verify that the version command remains a single parseable line."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def assert_version_output(binary: Path, *arguments: str) -> None:
    result = subprocess.run(
        [binary, *arguments],
        capture_output=True,
        check=True,
        text=True,
    )

    lines = result.stdout.splitlines()
    if len(lines) != 1:
        raise AssertionError(f"expected one stdout line, got {len(lines)}")
    if result.stderr:
        raise AssertionError(f"expected empty stderr, got: {result.stderr}")

    fields = dict(field.split("=", maxsplit=1) for field in lines[0].split("\t"))
    expected = {
        "version",
        "revision",
        "dirty",
        "build_timestamp",
        "build_mode",
        "compiler",
        "protobuf",
        "seastar",
    }
    if fields.keys() != expected:
        raise AssertionError(f"unexpected version fields: {fields.keys()}")
    if any(not value for value in fields.values()):
        raise AssertionError("version fields must not be empty")


def main() -> None:
    binary = Path(sys.argv[1])
    assert_version_output(binary, "--version")
    assert_version_output(binary, "--version", "-c", "1")


if __name__ == "__main__":
    main()
