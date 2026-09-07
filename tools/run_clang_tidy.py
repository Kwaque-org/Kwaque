from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
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
        ["bazel", "query", 'kind("cc_library|cc_binary", //src/... union //proto/...)',
         "--output=xml", "--noimplicit_deps", "--notool_deps"],
        cwd=root, text=True, stdout=subprocess.PIPE, check=True)
    return production_targets_from_query(result.stdout)


def select_files(entries: list[dict], requested: list[str], production: set[str] | None) -> list[str]:
    if not entries or any(not isinstance(entry.get("file"), str) for entry in entries):
        raise ValueError("compilation database has no valid source inventory")
    available = {entry["file"] for entry in entries}
    if set(requested) - available:
        raise ValueError("requested sources are missing from the compilation database")
    selected = set(requested) if requested else available
    if production is not None:
        if not requested and production - available:
            raise ValueError("production compilation database is incomplete; regenerate it in debug mode")
        selected &= production
    if not selected:
        raise ValueError("no C++ files selected; check the database and target scope")
    return sorted(selected)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run clang-tidy using compile_commands.json"
    )
    parser.add_argument("--tool", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--production-only", action="store_true")
    parser.add_argument("files", nargs="*")
    arguments = parser.parse_args()

    root = workspace_root()
    database_root = root / PRODUCTION_DATABASE_DIRECTORY if arguments.production_only else root
    database = database_root / "compile_commands.json"
    if not database.is_file():
        print(
            "compile_commands.json is missing; run bazel run //tools:compile_commands",
            file=sys.stderr,
        )
        return 2

    entries = json.loads(database.read_text())
    selected = select_files(
        entries, arguments.files,
        set().union(*production_targets(root).values()) if arguments.production_only else None)

    command = [
        str(resolve_runfile(arguments.tool)),
        "--quiet",
        f"--config-file={root / arguments.config}",
        f"-p={database_root}",
        *selected,
    ]
    return subprocess.run(command, cwd=root, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
