"""Check the forbidden edges between the replicated-state packages.

Each check asks a question of the first-party Bazel graph whose answer must be
empty: a dependency path a library may not have, a dependent the range engine
may not have, a visibility a package may not grant, a source a pure target may
not hold. Only libraries that are not testonly are checked; tests may depend
on anything they can see. The cross-shard usage check covers pointers between
shards, and the release-seam test covers simulation code in a release binary.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from tools.check_bazel_package_cycles import in_roots, resolve_workspace
from tools.check_determinism import is_pure_source

_QUERY_ROOTS = ("src", "specs")

# The metadata targets that apply commands. They are pure functions of the
# journal, so no clock, randomness, configuration, storage or journal may
# reach them.
PURE_METADATA_TARGETS = frozenset(
    {"//src/metadata:objects", "//src/metadata:apply", "//src/metadata:commands"}
)
# The runtime's value types: time, errors and positions. Every other runtime
# library reads a clock, draws randomness, does I/O, waits, schedules or
# exists only for simulation, so a new one is a service until listed here.
RUNTIME_VALUE_TYPES = frozenset(
    {"//src/runtime:time", "//src/runtime:error", "//src/runtime:file_position"}
)
# The packages pure metadata code may not include, as the determinism
# checker's pure-include rule names them.
PURE_FORBIDDEN_PACKAGES = (
    "//src/config",
    "//src/storage",
    "//src/raft",
    "//src/range",
    "//src/broker",
    "//src/simulation",
    "//src/resource",
    "//src/observability",
)
RANGE_ENGINE = "//src/range:engine"
# The one library that composes the broker; only it may start a range engine.
COMPOSITION_ROOT = "//src/broker:application_internal"

_PACKAGE_CALL = re.compile(r"^package\s*\(([^)]*)\)", re.MULTILINE)
_DEFAULT_VISIBILITY = re.compile(r"\bdefault_visibility\s*=\s*\[([^\]]*)\]")
_COMMENT = re.compile(r"#[^\n]*")
_LABEL = re.compile(r'"([^"]+)"')


@dataclass(frozen=True)
class Rule:
    label: str
    kind: str
    testonly: bool
    inputs: tuple[str, ...]
    # The rule's own visibility; None when it takes its package's default.
    visibility: tuple[str, ...] | None
    sources: tuple[str, ...]


Graph = Mapping[str, Rule]


def package_of(label: str) -> str:
    return label.split(":", maxsplit=1)[0]


def under(label: str, *roots: str) -> bool:
    return in_roots(label, roots)


def _labels(rule: ET.Element, attribute: str) -> tuple[str, ...] | None:
    for element in rule.findall("list"):
        if element.attrib.get("name") == attribute:
            return tuple(
                label.attrib["value"]
                for label in element.findall("label")
                if "value" in label.attrib
            )
    return None


def graph_from_query_xml(query_xml: str) -> dict[str, Rule]:
    graph: dict[str, Rule] = {}
    for element in ET.fromstring(query_xml).findall("rule"):
        try:
            label = element.attrib["name"]
        except KeyError as error:
            raise ValueError("rule element is missing its name") from error
        graph[label] = Rule(
            label=label,
            kind=element.attrib.get("class", ""),
            testonly=any(
                attribute.attrib.get("name") == "testonly"
                and attribute.attrib.get("value") == "true"
                for attribute in element.findall("boolean")
            ),
            inputs=tuple(
                rule_input.attrib["name"]
                for rule_input in element.findall("rule-input")
                if "name" in rule_input.attrib
            ),
            visibility=_labels(element, "visibility"),
            sources=(_labels(element, "srcs") or ()) + (_labels(element, "hdrs") or ()),
        )
    return graph


def is_production_library(rule: Rule) -> bool:
    return rule.kind == "cc_library" and not rule.testonly


def is_library(rule: Rule) -> bool:
    return rule.kind == "cc_library"


def is_runtime_service(rule: Rule) -> bool:
    return (
        is_library(rule)
        and under(rule.label, "//src/runtime")
        and rule.label not in RUNTIME_VALUE_TYPES
    )


def find_path(graph: Graph, source: str, forbidden: frozenset[str]) -> list[str]:
    """Return a dependency path from source to a forbidden rule, or none."""
    parents: dict[str, str | None] = {source: None}
    pending = [source]
    while pending:
        label = pending.pop()
        rule = graph.get(label)
        if rule is None:
            continue
        for dependency in rule.inputs:
            if dependency in parents or dependency not in graph:
                continue
            parents[dependency] = label
            if dependency in forbidden:
                path = [dependency]
                while (parent := parents[path[-1]]) is not None:
                    path.append(parent)
                return path[::-1]
            pending.append(dependency)
    return []


@dataclass(frozen=True)
class PathCheck:
    name: str
    source: Callable[[Rule], bool]
    forbidden: Callable[[Rule], bool]

    def violations(self, graph: Graph) -> list[str]:
        forbidden = frozenset(
            label for label, rule in graph.items() if self.forbidden(rule)
        )
        if not forbidden:
            return []
        found = []
        for label in sorted(graph):
            if not self.source(graph[label]):
                continue
            path = find_path(graph, label, forbidden)
            if path:
                found.append(f"{self.name}: {' -> '.join(path)}")
        return found


PATH_CHECKS = (
    PathCheck(
        "storage depends on raft, metadata or range",
        lambda rule: is_production_library(rule) and under(rule.label, "//src/storage"),
        lambda rule: (
            is_library(rule)
            and under(rule.label, "//src/raft", "//src/metadata", "//src/range")
        ),
    ),
    PathCheck(
        "raft depends on metadata or range",
        lambda rule: is_production_library(rule) and under(rule.label, "//src/raft"),
        lambda rule: (
            is_library(rule) and under(rule.label, "//src/metadata", "//src/range")
        ),
    ),
    PathCheck(
        "pure metadata depends on a runtime service or an owner package",
        lambda rule: rule.label in PURE_METADATA_TARGETS,
        lambda rule: (
            is_runtime_service(rule)
            or (is_library(rule) and under(rule.label, *PURE_FORBIDDEN_PACKAGES))
        ),
    ),
    PathCheck(
        "range depends on the broker",
        lambda rule: is_production_library(rule) and under(rule.label, "//src/range"),
        lambda rule: is_library(rule) and under(rule.label, "//src/broker"),
    ),
    PathCheck(
        "production code depends on simulation",
        lambda rule: (
            is_production_library(rule)
            and under(rule.label, "//src")
            and not under(rule.label, "//src/simulation")
        ),
        lambda rule: is_library(rule) and under(rule.label, "//src/simulation"),
    ),
    PathCheck(
        "a value package depends on a runtime service, storage, metadata or range",
        lambda rule: (
            is_production_library(rule)
            and under(
                rule.label, "//src/base", "//src/bytes", "//src/codec", "//src/model"
            )
        ),
        lambda rule: (
            is_runtime_service(rule)
            or (
                is_library(rule)
                and under(rule.label, "//src/storage", "//src/metadata", "//src/range")
            )
        ),
    ),
)


def range_engine_dependents(graph: Graph) -> list[str]:
    """Libraries outside the range packages that depend on the engine directly.

    An alias of the engine counts as the engine.
    """
    engine = {RANGE_ENGINE}
    while aliases := {
        label
        for label, rule in graph.items()
        if rule.kind == "alias" and label not in engine and engine & set(rule.inputs)
    }:
        engine |= aliases
    return [
        f"the range engine is started outside the composition root: {label}"
        for label, rule in sorted(graph.items())
        if is_production_library(rule)
        and not under(label, "//src/range")
        and label != COMPOSITION_ROOT
        and engine & set(rule.inputs)
    ]


def specification_rules(graph: Graph) -> list[str]:
    """Rules in the specification packages other than file groups."""
    return [
        f"a specification package holds a {rule.kind}: {label}"
        for label, rule in sorted(graph.items())
        if under(label, "//specs") and rule.kind != "filegroup"
    ]


def pure_sources(graph: Graph) -> list[str]:
    """Sources of a pure metadata target outside the pure-source predicate."""
    found = []
    for label in sorted(PURE_METADATA_TARGETS & graph.keys()):
        for source in graph[label].sources:
            package, _, name = source.partition(":")
            path = Path(package.removeprefix("//")) / name
            if not is_pure_source(path):
                found.append(
                    f"a pure metadata target holds a source the determinism "
                    f"checker does not treat as pure: {label} {path.as_posix()}"
                )
    return found


def grants(visibility: Iterable[str], package: str) -> bool:
    for entry in visibility:
        if entry == "//visibility:public":
            return True
        if entry == "//visibility:private":
            continue
        granted, _, form = entry.partition(":")
        if form == "__pkg__":
            if granted == package:
                return True
        elif form == "__subpackages__":
            if package == granted or package.startswith(f"{granted}/"):
                return True
        else:
            # A package group, or any other form, is read by Bazel alone.
            raise ValueError(f"unrecognized visibility label {entry}")
    return False


def storage_visible_to_metadata(
    graph: Graph, defaults: Mapping[str, tuple[str, ...]]
) -> list[str]:
    """Storage libraries a metadata package could depend on."""
    found = []
    for label, rule in sorted(graph.items()):
        if not is_production_library(rule) or not under(label, "//src/storage"):
            continue
        visibility = (
            rule.visibility
            if rule.visibility is not None
            else defaults.get(package_of(label), ())
        )
        if grants(visibility, "//src/metadata") or grants(
            visibility, "//src/metadata/tests"
        ):
            found.append(f"a storage library is visible to metadata: {label}")
    return found


def default_visibilities(workspace: Path, graph: Graph) -> dict[str, tuple[str, ...]]:
    """Each checked package's default visibility; none means private."""
    defaults: dict[str, tuple[str, ...]] = {}
    for package in sorted({package_of(label) for label in graph}):
        # Bazel reads BUILD.bazel when a package has both.
        build = workspace / package.removeprefix("//") / "BUILD.bazel"
        if not build.is_file():
            build = build.with_name("BUILD")
        if not build.is_file():
            continue
        text = _COMMENT.sub("", build.read_text(encoding="utf-8"))
        call = _PACKAGE_CALL.search(text)
        match = _DEFAULT_VISIBILITY.search(call.group(1)) if call else None
        defaults[package] = tuple(_LABEL.findall(match.group(1))) if match else ()
    return defaults


def check(graph: Graph, defaults: Mapping[str, tuple[str, ...]]) -> list[str]:
    violations: list[str] = []
    for path_check in PATH_CHECKS:
        violations.extend(path_check.violations(graph))
    violations.extend(range_engine_dependents(graph))
    violations.extend(specification_rules(graph))
    violations.extend(pure_sources(graph))
    violations.extend(storage_visible_to_metadata(graph, defaults))
    return violations


def query_workspace(workspace: Path) -> str:
    roots = [root for root in _QUERY_ROOTS if (workspace / root).is_dir()]
    result = subprocess.run(
        [
            "bazel",
            "query",
            "--noimplicit_deps",
            "--notool_deps",
            "--output=xml",
            "deps({})".format(" + ".join(f"//{root}/..." for root in roots)),
        ],
        cwd=workspace,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


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
            "workspace in which to run Bazel and read package defaults; relative "
            "paths are resolved from the Bazel invocation workspace"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = resolve_workspace(args.workspace)
    try:
        query_xml = (
            args.query_xml.read_text(encoding="utf-8")
            if args.query_xml is not None
            else query_workspace(workspace)
        )
        graph = graph_from_query_xml(query_xml)
        violations = check(graph, default_visibilities(workspace, graph))
    except (OSError, subprocess.CalledProcessError, ET.ParseError, ValueError) as error:
        print(f"unable to inspect the Bazel package graph: {error}", file=sys.stderr)
        return 2
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("Replicated-state package edges are allowed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
