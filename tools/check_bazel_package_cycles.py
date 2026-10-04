"""Check the first-party Bazel graph.

Rejects package-level dependency cycles across source and schema targets, C++
rules declared without a Kwaque macro, and tests that link both Boost.Test and
GoogleTest.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from pathlib import Path

_FIRST_PARTY_ROOTS = ("//src", "//proto")
_MACRO_ROOTS = ("//bazel", "//proto", "//src", "//tests")
_CC_RULE_CLASSES = frozenset({"cc_binary", "cc_library", "cc_test"})
_NATIVE_PROGRAM_CLASSES = frozenset({"cc_binary", "cc_test"})
# Macros that give a Python test the sanitizer and reactor-backend environment.
_ENVIRONMENT_MACROS = frozenset(
    {
        "kwaque_cc_benchmark",
        "kwaque_cc_fuzz_test",
        "kwaque_fuzz_signal_canary_test",
        "kwaque_py_native_test",
    }
)
_BAZEL_WORKSPACE_ENV = "BUILD_WORKSPACE_DIRECTORY"


def in_roots(label: str, roots: Iterable[str]) -> bool:
    package = label.split(":", maxsplit=1)[0]
    return any(package == root or package.startswith(f"{root}/") for root in roots)


def package_for_label(label: str) -> str | None:
    if not in_roots(label, _FIRST_PARTY_ROOTS):
        return None
    return label.split(":", maxsplit=1)[0]


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


def generator_function(rule: ET.Element) -> str:
    for attribute in rule.findall("string"):
        if attribute.attrib.get("name") == "generator_function":
            return attribute.attrib.get("value", "")
    return ""


def unwrapped_cc_rules(query_xml: str) -> list[str]:
    """Return first-party C++ rules that no kwaque_ macro generated."""
    return sorted(
        required_name(rule)
        for rule in ET.fromstring(query_xml).findall("rule")
        if rule.attrib.get("class") in _CC_RULE_CLASSES
        and in_roots(required_name(rule), _MACRO_ROOTS)
        and not generator_function(rule).startswith("kwaque_")
    )


def _is_boost_test(label: str) -> bool:
    repository, _, target = label.rpartition("//:")
    return "boost" in repository and target in {"test", "test.so"}


def _is_gtest(label: str) -> bool:
    repository, _, target = label.rpartition("//:")
    return "googletest" in repository and target == "gtest"


def mixed_test_frameworks(query_xml: str) -> list[str]:
    """Return tests whose dependencies include both Boost.Test and GoogleTest."""
    rules = ET.fromstring(query_xml).findall("rule")
    inputs = {
        required_name(rule): [
            required_name(rule_input) for rule_input in rule.findall("rule-input")
        ]
        for rule in rules
    }
    mixed = []
    for rule in rules:
        name = required_name(rule)
        if not rule.attrib.get("class", "").endswith("_test"):
            continue
        seen = {name}
        pending = [name]
        while pending:
            for dependency in inputs.get(pending.pop(), []):
                if dependency not in seen:
                    seen.add(dependency)
                    pending.append(dependency)
        if any(map(_is_boost_test, seen)) and any(map(_is_gtest, seen)):
            mixed.append(name)
    return sorted(mixed)


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
            "deps(set({}))".format(" ".join(f"{root}/..." for root in _MACRO_ROOTS)),
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


def unwrapped_native_python_tests(query_xml: str) -> list[str]:
    """Return Python tests that reach first-party native programs directly.

    Such a test may start a sanitized binary, which needs the macro-provided
    environment that makes sanitizer reports fatal.
    """
    rules = ET.fromstring(query_xml).findall("rule")
    classes = {required_name(rule): rule.attrib.get("class", "") for rule in rules}
    inputs = {
        required_name(rule): [
            required_name(rule_input) for rule_input in rule.findall("rule-input")
        ]
        for rule in rules
    }
    unwrapped = []
    for rule in rules:
        name = required_name(rule)
        if (
            rule.attrib.get("class") != "py_test"
            or not in_roots(name, _MACRO_ROOTS)
            or generator_function(rule) in _ENVIRONMENT_MACROS
        ):
            continue
        seen = {name}
        pending = [name]
        while pending:
            for dependency in inputs.get(pending.pop(), []):
                if dependency not in seen:
                    seen.add(dependency)
                    pending.append(dependency)
        if any(
            classes.get(dependency) in _NATIVE_PROGRAM_CLASSES
            and in_roots(dependency, _MACRO_ROOTS)
            for dependency in seen
        ):
            unwrapped.append(name)
    return sorted(unwrapped)


def main() -> int:
    args = parse_args()
    try:
        query_xml = (
            args.query_xml.read_text(encoding="utf-8")
            if args.query_xml is not None
            else query_workspace(resolve_workspace(args.workspace))
        )
        cycle = find_cycle(graph_from_query_xml(query_xml))
        unwrapped = unwrapped_cc_rules(query_xml)
        mixed = mixed_test_frameworks(query_xml)
        native_python = unwrapped_native_python_tests(query_xml)
    except (OSError, subprocess.CalledProcessError, ET.ParseError, ValueError) as error:
        print(f"unable to inspect the Bazel package graph: {error}", file=sys.stderr)
        return 2

    failed = False
    if cycle is not None:
        print("Kwaque package dependency cycle: " + " -> ".join(cycle), file=sys.stderr)
        failed = True
    for label in unwrapped:
        print(f"{label}: declare C++ rules with a kwaque_ macro", file=sys.stderr)
        failed = True
    for label in mixed:
        print(f"{label}: links both Boost.Test and GoogleTest", file=sys.stderr)
        failed = True
    for label in native_python:
        print(
            f"{label}: run native programs from kwaque_py_native_test",
            file=sys.stderr,
        )
        failed = True
    if failed:
        return 1
    print("Kwaque package dependency graph is acyclic and uses the build macros")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
