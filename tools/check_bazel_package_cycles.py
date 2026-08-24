"""Reject package-level dependency cycles across source and schema targets."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from pathlib import Path

_FIRST_PARTY_ROOTS = ("//src", "//proto")
_BAZEL_WORKSPACE_ENV = "BUILD_WORKSPACE_DIRECTORY"


def package_for_label(label: str) -> str | None:
    package = label.split(":", maxsplit=1)[0]
    if not any(
        package == root or package.startswith(f"{root}/") for root in _FIRST_PARTY_ROOTS
    ):
        return None
    return package


def required_name(element: ET.Element) -> str:
    try:
        return element.attrib["name"]
    except KeyError as error:
        raise ValueError(
            f"{element.tag} element is missing required name attribute"
        ) from error


def graph_from_query_xml(query_xml: str) -> dict[str, set[str]]:
    root = ET.fromstring(query_xml)
    graph: dict[str, set[str]] = {}

    for rule in root.findall("rule"):
        package = package_for_label(required_name(rule))
        if package is None:
            continue
        dependencies = graph.setdefault(package, set())
        for rule_input in rule.findall("rule-input"):
            dependency = package_for_label(required_name(rule_input))
            if dependency is not None and dependency != package:
                dependencies.add(dependency)
                graph.setdefault(dependency, set())

    return graph


def find_cycle(graph: Mapping[str, Iterable[str]]) -> list[str] | None:
    visited: set[str] = set()
    active: set[str] = set()
    path: list[str] = []

    def visit(package: str) -> list[str] | None:
        if package in active:
            cycle_start = path.index(package)
            return path[cycle_start:] + [package]
        if package in visited:
            return None

        active.add(package)
        path.append(package)
        for dependency in sorted(graph.get(package, [])):
            cycle = visit(dependency)
            if cycle is not None:
                return cycle
        path.pop()
        active.remove(package)
        visited.add(package)
        return None

    for package in sorted(graph):
        cycle = visit(package)
        if cycle is not None:
            return cycle
    return None


def query_workspace(workspace: Path) -> str:
    result = subprocess.run(
        [
            "bazel",
            "query",
            "--noimplicit_deps",
            "--notool_deps",
            "--output=xml",
            "deps(set(//src/... //proto/...))",
        ],
        cwd=workspace,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def resolve_workspace(workspace: Path | None) -> Path:
    invocation_workspace = os.environ.get(_BAZEL_WORKSPACE_ENV)
    if workspace is None:
        return (
            Path(invocation_workspace)
            if invocation_workspace is not None
            else Path.cwd()
        )
    if workspace.is_absolute() or invocation_workspace is None:
        return workspace
    return Path(invocation_workspace) / workspace


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query-xml",
        type=Path,
        help="read previously generated Bazel query XML instead of invoking Bazel",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        help=(
            "workspace in which to run Bazel; relative paths are resolved from "
            "the Bazel invocation workspace when run through `bazel run`"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        query_xml = (
            args.query_xml.read_text(encoding="utf-8")
            if args.query_xml is not None
            else query_workspace(resolve_workspace(args.workspace))
        )
        cycle = find_cycle(graph_from_query_xml(query_xml))
    except (OSError, subprocess.CalledProcessError, ET.ParseError, ValueError) as error:
        print(f"unable to inspect the Bazel package graph: {error}", file=sys.stderr)
        return 2

    if cycle is not None:
        print("Kwaque package dependency cycle: " + " -> ".join(cycle), file=sys.stderr)
        return 1
    print("Kwaque package dependency graph is acyclic")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
