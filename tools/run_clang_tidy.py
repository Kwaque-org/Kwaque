from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

PRODUCTION_DATABASE_DIRECTORY = Path(".cache") / "clang-tidy-production"


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
        bool(source.parts)
        and source.parts[0] in {"src", "proto"}
        and source.suffix.lower() in {".c", ".cc", ".cpp", ".cxx"}
        and not {"tests", "testing"}.intersection(source.parts)
        and not path.startswith("src/simulation/")
        and not source.name.endswith(("_test.cc", "_fuzz.cc", "_bench.cc"))
    )


def production_targets_from_query(query_xml: str) -> dict[str, set[str]]:
    targets = {}
    for rule in ET.fromstring(query_xml).findall("rule"):
        if rule.get("class") not in {"cc_library", "cc_binary"}:
            continue
        testonly = rule.find('boolean[@name="testonly"]')
        if testonly is not None and testonly.get("value") != "false":
            continue
        for value in rule.findall('list[@name="srcs"]/label'):
            label = value.get("value", "")
            if label.startswith("//") and ":" in label:
                path = label[2:].replace(":", "/", 1)
                if is_production_source(path):
                    targets.setdefault(rule.get("name"), set()).add(path)
    return targets


def production_sources_from_query(query_xml: str) -> set[str]:
    return set().union(*production_targets_from_query(query_xml).values())


def production_targets(root: Path) -> dict[str, set[str]]:
    result = subprocess.run(
        [
            "bazel",
            "query",
            'kind("cc_library|cc_binary", //src/... union //proto/...)',
            "--output=xml",
            "--noimplicit_deps",
            "--notool_deps",
        ],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    )
    return production_targets_from_query(result.stdout)


def select_files(
    entries: list[dict], requested: list[str], production: set[str] | None
) -> list[str]:
    if not entries or any(not isinstance(entry.get("file"), str) for entry in entries):
        raise ValueError("compilation database has no valid source inventory")
    available = {entry["file"] for entry in entries}
    if set(requested) - available:
        raise ValueError("requested sources are missing from the compilation database")
    selected = set(requested) if requested else available
    if production is not None:
        if not requested and production - available:
            raise ValueError(
                "production compilation database is incomplete; regenerate it in debug mode"
            )
        selected &= production
    if not selected:
        raise ValueError("no C++ files selected; check the database and target scope")
    return sorted(selected)


def positive_jobs(value: str) -> int:
    jobs = int(value)
    if jobs <= 0:
        raise argparse.ArgumentTypeError("jobs must be a positive integer")
    return jobs


def command_key(entry: dict) -> tuple:
    arguments = entry.get("arguments")
    if (
        not isinstance(entry.get("file"), str)
        or not isinstance(entry.get("directory"), str)
        or not isinstance(arguments, list)
        or not arguments
        or not all(isinstance(argument, str) for argument in arguments)
    ):
        raise ValueError("compilation database contains an invalid command")
    return entry["directory"], entry["file"], tuple(arguments)


def remaining_commands(entries: list[dict], production: list[dict]) -> list[dict]:
    """Remove only the exact commands that will receive production checks."""
    ordinary_keys = {command_key(entry) for entry in entries}
    production_keys = {command_key(entry) for entry in production}
    if not production_keys or not production_keys <= ordinary_keys:
        raise ValueError("production commands are stale; regenerate the database")
    return [entry for entry in entries if command_key(entry) not in production_keys]


def runner_command(
    runner: Path,
    tool: Path,
    config: Path,
    database_root: Path,
    entries: list[dict],
    selected: list[str],
    jobs: int,
    profile: bool,
) -> list[str]:
    selected_names = set(selected)
    # The native runner accepts regular expressions over absolute source paths.
    # Keep exact selection and every database command variant for those sources.
    paths = sorted(
        {
            os.path.abspath(os.path.join(entry["directory"], entry["file"]))
            for entry in entries
            if entry["file"] in selected_names
        }
    )
    return [
        sys.executable,
        "-u",
        str(runner),
        f"-clang-tidy-binary={tool}",
        f"-config-file={config}",
        f"-p={database_root}",
        f"-j={min(jobs, len(paths))}",
        "-quiet",
        *(["-enable-check-profile"] if profile else []),
        *(f"^{re.escape(path)}$" for path in paths),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run clang-tidy using compile_commands.json"
    )
    parser.add_argument("--tool", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--config", required=True)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--production-only", action="store_true")
    scope.add_argument(
        "--production-config",
        help="check production commands with this config and other commands once",
    )
    parser.add_argument(
        "--jobs",
        "-j",
        type=positive_jobs,
        default=2,
        help="maximum concurrent clang-tidy processes (default: 2)",
    )
    parser.add_argument(
        "--profile", action="store_true", help="report native per-check timing totals"
    )
    parser.add_argument("files", nargs="*")
    arguments = parser.parse_args()

    root = workspace_root()
    database_root = (
        root / PRODUCTION_DATABASE_DIRECTORY if arguments.production_only else root
    )
    database = database_root / "compile_commands.json"
    if not database.is_file():
        print(
            "compile_commands.json is missing; run bazel run //tools:compile_commands",
            file=sys.stderr,
        )
        return 2

    entries = json.loads(database.read_text())
    if arguments.production_config:
        production_entries = json.loads(
            (root / PRODUCTION_DATABASE_DIRECTORY / "compile_commands.json").read_text()
        )
        # Validate the complete partition before launching either pass. Select
        # by command, not filename: tests may compile production files differently.
        ordinary_entries = remaining_commands(entries, production_entries)
        select_files(
            production_entries, [], set().union(*production_targets(root).values())
        )
        selected = set(select_files(entries, arguments.files, None))
        result = 0
        with tempfile.TemporaryDirectory(prefix="kwaque-tidy-") as temporary:
            for name, commands, config in (
                ("ordinary", ordinary_entries, arguments.config),
                ("production", production_entries, arguments.production_config),
            ):
                commands = [entry for entry in commands if entry["file"] in selected]
                if not commands:
                    continue
                partition = Path(temporary) / name
                partition.mkdir()
                (partition / "compile_commands.json").write_text(json.dumps(commands))
                command = runner_command(
                    resolve_runfile(arguments.runner),
                    resolve_runfile(arguments.tool),
                    root / config,
                    partition,
                    commands,
                    sorted({entry["file"] for entry in commands}),
                    arguments.jobs,
                    arguments.profile,
                )
                completed = subprocess.run(command, cwd=root, check=False)
                # Still report production diagnostics if ordinary checks fail.
                result = result or completed.returncode
        return result

    selected = select_files(
        entries,
        arguments.files,
        (
            set().union(*production_targets(root).values())
            if arguments.production_only
            else None
        ),
    )

    command = runner_command(
        resolve_runfile(arguments.runner),
        resolve_runfile(arguments.tool),
        root / arguments.config,
        database_root,
        entries,
        selected,
        arguments.jobs,
        arguments.profile,
    )
    return subprocess.run(command, cwd=root, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
