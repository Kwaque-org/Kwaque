"""Verify version output and startup-independent help and version commands."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def inspect_command(
    binary: Path,
    *arguments: str,
    environment: dict[str, str] | None = None,
    directory: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [binary, *arguments],
        capture_output=True,
        check=True,
        text=True,
        timeout=5.0,
        env=environment,
        cwd=directory,
    )
    for forbidden in ("configuration loaded", "startup stage=", "runtime shards="):
        if forbidden in result.stdout + result.stderr:
            raise AssertionError(f"inspection entered broker startup: {result}")
    return result


def assert_version_output(
    binary: Path,
    *arguments: str,
    environment: dict[str, str] | None = None,
    directory: Path | None = None,
) -> None:
    result = inspect_command(
        binary, *arguments, environment=environment, directory=directory
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
    binary = Path(sys.argv[1]).resolve()
    assert_version_output(binary, "--version")
    assert_version_output(binary, "--version", "-c", "1")

    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        home = directory / "home"
        personal_config = home / ".config" / "seastar"
        personal_config.mkdir(parents=True)
        (personal_config / "seastar.conf").write_text(
            "unsafe-bypass-fsync=true\nkernel-page-cache=true\nrelaxed-dma=true\n",
            encoding="utf-8",
        )
        (personal_config / "io.conf").write_text(
            "unknown-runtime-option=true\n", encoding="utf-8"
        )
        environment = {**os.environ, "HOME": str(home)}
        malformed = directory / "malformed.yaml"
        malformed.write_text("kwaque: [", encoding="utf-8")
        configured = directory / "configured.yaml"
        configured.write_text(
            "kwaque:\n  schema_version: 1\n"
            f'  data_directory: "{directory / "broker-data"}"\n',
            encoding="utf-8",
        )
        before = set(directory.rglob("*"))
        for config in (directory / "missing.yaml", malformed, configured):
            arguments = ("--config", str(config), "--smp=0", "--memory=1")
            assert_version_output(
                binary,
                "--version",
                *arguments,
                "--unsafe-bypass-fsync=true",
                environment=environment,
                directory=directory,
            )
            for option, expected in (
                ("--help", "--config"),
                ("--help-seastar", "--smp"),
                # The broker logger is initialized lazily during startup.
                ("--help-loggers", "    seastar\n"),
            ):
                result = inspect_command(
                    binary,
                    option,
                    *arguments,
                    environment=environment,
                    directory=directory,
                )
                if expected not in result.stdout:
                    raise AssertionError(
                        f"{option} did not report {expected!r}:\n{result.stdout}"
                    )
        after = set(directory.rglob("*"))
        if after != before:
            raise AssertionError(f"inspection created files: {sorted(after - before)}")


if __name__ == "__main__":
    main()
