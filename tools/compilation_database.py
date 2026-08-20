#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


CPP_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx"})


def workspace_root() -> Path:
    configured = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not configured:
        raise RuntimeError("run this generator with bazel run")
    return Path(configured).resolve()


def compiler_arguments(action: dict[str, Any], execution_root: Path) -> tuple[str, list[str]] | None:
    arguments = list(action.get("arguments", []))
    if not arguments:
        return None

    source = None
    for index, argument in enumerate(arguments[:-1]):
        if argument == "-c":
            source = arguments[index + 1]
            break
    if source is None or Path(source).suffix.lower() not in CPP_SUFFIXES:
        return None
    if source.startswith(("external/", "bazel-out/")):
        return None

    compiler = Path(arguments[0])
    if not compiler.is_absolute():
        compiler = execution_root / compiler
    if compiler.name == "cc_wrapper.sh":
        clang_cpp = compiler.parent / "clang-cpp"
        if clang_cpp.exists():
            clang = clang_cpp.resolve().parent / "clang"
            if clang.exists():
                compiler = clang
    arguments[0] = str(compiler)
    external_root = execution_root.parent.parent / "external"
    for index, argument in enumerate(arguments[1:], start=1):
        if argument.startswith("external/"):
            arguments[index] = str(external_root / argument.removeprefix("external/"))
        elif "=external/" in argument:
            arguments[index] = argument.replace(
                "=external/", f"={external_root}/", 1
            )
    return source, arguments


def generate(extra_bazel_args: list[str]) -> int:
    root = workspace_root()
    execution_root = Path(
        subprocess.run(
            ["bazel", "info", "execution_root"],
            cwd=root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
    )
    command = [
        "bazel",
        "aquery",
        'mnemonic("CppCompile", //...)',
        "--output=jsonproto",
        "--include_artifacts=false",
        "--features=-compiler_param_file",
        "--host_features=-compiler_param_file",
        "--features=-layering_check",
        "--host_features=-layering_check",
        "--features=-parse_headers",
        "--host_features=-parse_headers",
        "--noshow_progress",
        *extra_bazel_args,
    ]
    result = subprocess.run(
        command,
        cwd=root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    actions = json.loads(result.stdout).get("actions", [])
    entries: dict[str, dict[str, Any]] = {}
    for action in actions:
        parsed = compiler_arguments(action, execution_root)
        if parsed is None:
            continue
        source, arguments = parsed
        entries[source] = {
            "directory": str(root),
            "file": source,
            "arguments": arguments,
        }

    output = [entries[key] for key in sorted(entries)]
    if not output:
        raise RuntimeError("Bazel returned no workspace C++ compilation actions")
    destination = root / "compile_commands.json"
    destination.write_text(json.dumps(output, indent=2) + "\n")
    print(f"Wrote {len(output)} entries to {destination}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate compile_commands.json from Bazel")
    _, bazel_args = parser.parse_known_args()
    return generate(bazel_args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"compilation database generation failed: {error}", file=sys.stderr)
        sys.exit(1)
