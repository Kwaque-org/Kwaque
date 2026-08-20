#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def workspace_root() -> Path:
    configured = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if configured:
        return Path(configured).resolve()
    return Path.cwd().resolve()


def resolve_runfile(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    runfiles_roots = []
    if configured := os.environ.get("RUNFILES_DIR"):
        runfiles_roots.append(Path(configured))
    runfiles_roots.extend(
        parent
        for parent in Path(__file__).absolute().parents
        if parent.name.endswith(".runfiles")
    )
    for root in runfiles_roots:
        for base in (root, root.parent):
            resolved = base / path
            if resolved.exists():
                return resolved
    return candidate.resolve()


def is_production_source(path: str) -> bool:
    source = Path(path)
    return (
        source.parts[0] in {"src", "proto"}
        and "tests" not in source.parts
        and not source.name.endswith(("_test.cc", "_fuzz.cc"))
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run clang-tidy using compile_commands.json")
    parser.add_argument("--tool", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--production-only", action="store_true")
    parser.add_argument("files", nargs="*")
    arguments = parser.parse_args()

    root = workspace_root()
    database = root / "compile_commands.json"
    if not database.is_file():
        print("compile_commands.json is missing; run bazel run //tools:compile_commands", file=sys.stderr)
        return 2

    entries = json.loads(database.read_text())
    available = sorted({entry["file"] for entry in entries})
    selected = arguments.files or available
    if arguments.production_only:
        selected = [path for path in selected if is_production_source(path)]
    if not selected:
        print("No C++ files selected.")
        return 0

    command = [
        str(resolve_runfile(arguments.tool)),
        "--quiet",
        f"--config-file={root / arguments.config}",
        f"-p={root}",
        *selected,
    ]
    return subprocess.run(command, cwd=root, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
