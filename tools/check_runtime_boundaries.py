from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from tools.check_cross_shard_usage import code_view, line_number

CPP_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"})
HEADER_SUFFIXES = frozenset({".h", ".hh", ".hpp"})
EXCLUDED_PARTS = frozenset({"tests", "testing", "testdata"})

PRODUCTION_INCLUDE = re.compile(
    r'^\s*#\s*include\s*[<"]src/runtime/production/', re.MULTILINE
)
REVERSE_INCLUDE = re.compile(
    r'^\s*#\s*include\s*[<"]src/(?:runtime/testing|simulation)/', re.MULTILINE
)
CONCRETE_ENVIRONMENT_INCLUDE = re.compile(
    r'^\s*#\s*include\s*[<"]src/(?:runtime/production|simulation)/', re.MULTILINE
)
RESOURCE_INCLUDE = re.compile(
    r'^\s*#\s*include\s*[<"]src/resource/', re.MULTILINE
)
SIMULATION_TYPE_REFERENCE = re.compile(
    r"\b(?:kwaque::)?simulation::|\bnamespace\s+kwaque::simulation\b"
)
BROAD_RUNTIME_REFERENCE = re.compile(r"\bbasic_runtime\s*<")
BROAD_BACKEND_REFERENCE = re.compile(r"\bBackend\s*[*&]")
BROAD_RESOURCE_MANAGER_REFERENCE = re.compile(r"\bresource::resource_manager\b")
BROAD_RESOURCE_REGISTRY_REFERENCE = re.compile(r"\bresource::resource_registry\b")
CONCRETE_ENVIRONMENT_DEPENDENCY = re.compile(
    r'"(//src/(?:runtime/production|simulation)(?::|/)[^"]*)"'
)
LIBRARY_RULE = re.compile(r"kwaque_cc_library\((?:(?!\n\)).)*\n\)", re.DOTALL)
RULE_NAME = re.compile(r'\bname\s*=\s*"([^"]+)"')
SHARED_ENVIRONMENT_RULE = re.compile(
    r'\bname\s*=\s*"environment_(?:component|contract)"'
)
DEPENDENCY_LIST = re.compile(r"\bdeps\s*=\s*\[(.*?)\]", re.DOTALL)
EXPORTED_INTERNAL_COMPOSITION_HEADER = re.compile(
    r"\bhdrs\s*=\s*\[[^\]]*\"application_internal\.h\"", re.DOTALL
)
PUBLIC_VISIBILITY = re.compile(r'"//visibility:public"')
TEST_ONLY = re.compile(r"\btestonly\s*=\s*True\b")
APPLICATION_TEST_SUPPORT_DEPENDENCY = re.compile(
    r'"(?::|//src/broker:)application_test_support"'
)
BOOTSTRAP_TUNING_LITERAL = re.compile(
    r'''["'](?:runtime|simulation|resource_memory|resource_total_memory|'''
    r'''reactor_headroom|scheduler|event_log|fault_rules)["']'''
)
BOOTSTRAP_TUNING_YAML_KEY = re.compile(
    r"^\s*(?:runtime|simulation|resource_memory|resource_total_memory|"
    r"reactor_headroom|scheduler|event_log|fault_rules)\s*:",
    re.MULTILINE,
)
BOOTSTRAP_SCOPE_FILES = (
    "src/config/bootstrap_config.h",
    "src/config/bootstrap_config.cc",
    "conf/kwaque.yaml",
)
ALLOWED_PRODUCTION_INCLUDE_LINES = {
    "src/broker/application_internal.h": frozenset(
        {'#include "src/runtime/production/environment.h"'}
    ),
}
ALLOWED_CONCRETE_LIBRARY_DEPENDENCIES = {
    ("src/broker/BUILD", "application_internal"): frozenset(
        {"//src/runtime/production:environment"}
    ),
}
DECLARATION_OWNER_HEADERS = frozenset({"src/runtime/environment.h"})
RESOURCE_COMPOSITION_HEADERS = frozenset({"src/broker/application_internal.h"})
SHARED_ENVIRONMENT_HEADERS = frozenset(
    {
        "src/runtime/testing/contracts/environment_component.h",
        "src/runtime/testing/contracts/environment_contract.h",
    }
)
NARROW_COMPONENT_HEADERS = frozenset(
    {"src/runtime/testing/contracts/environment_component.h"}
)
ENVIRONMENT_CONTRACT_CONSUMERS = frozenset(
    {
        "src/runtime/tests/production_environment_test.cc",
        "src/simulation/tests/environment_replay_test.cc",
        "src/simulation/tests/environment_test.cc",
    }
)
COPIED_ENVIRONMENT_CONTRACT_BODY = re.compile(
    r"\benvironment_component\s*<|(?:\.|->)(?:admit|resources_cached)\s*\("
)
LOCAL_SIMULATION_PUMP = re.compile(
    r"\bseastar::future\s*<\s*>\s+pump_(?:deterministic_)?until\s*\("
)
LEGACY_BROKER_RUNTIME_OWNER = re.compile(
    r"\bruntime_service_|\bproduction_backends_|\bbackend_owner\b"
)
BROKER_RUNTIME_CHECKPOINT = re.compile(
    r"\bfailure_point\b|\bfail_at_start_boundary\b|\bstart_checkpoint_failure\b"
)

def is_test_path(path: Path) -> bool:
    return path.name.endswith(
        ("_test.cc", "_test.h", "_test_support.h", "_bench.cc")
    ) or any(part in EXCLUDED_PARTS for part in path.parts)


def source_files(root: Path) -> list[Path]:
    return sorted(
        path.relative_to(root)
        for path in (root / "src").rglob("*")
        if path.is_file() and path.suffix in CPP_SUFFIXES
    )


def build_files(root: Path) -> list[Path]:
    return sorted(path.relative_to(root) for path in (root / "src").rglob("BUILD"))


def code_match_lines(text: str, pattern: re.Pattern[str]) -> list[int]:
    code, offsets = code_view(text)
    return [line_number(text, offsets[match.start()]) for match in pattern.finditer(code)]


def text_match_lines(text: str, pattern: re.Pattern[str]) -> list[int]:
    return [1 + text.count("\n", 0, match.start()) for match in pattern.finditer(text)]


def scan(root: Path) -> list[str]:
    violations: list[str] = []
    for name in BOOTSTRAP_SCOPE_FILES:
        candidate = root / name
        if not candidate.is_file():
            continue
        text = candidate.read_text(encoding="utf-8")
        pattern = (
            BOOTSTRAP_TUNING_YAML_KEY
            if candidate.suffix in {".yaml", ".yml"}
            else BOOTSTRAP_TUNING_LITERAL
        )
        for line in text_match_lines(text, pattern):
            violations.append(
                f"{name}:{line}: bootstrap exposes runtime/simulation tuning"
            )

    for relative in source_files(root):
        text = (root / relative).read_text(encoding="utf-8")
        path = relative.as_posix()
        production_file = path.startswith("src/runtime/production/")
        simulation_file = path.startswith("src/simulation/")
        test_file = is_test_path(relative)

        if path in SHARED_ENVIRONMENT_HEADERS:
            for line in text_match_lines(text, CONCRETE_ENVIRONMENT_INCLUDE):
                violations.append(
                    f"{relative}:{line}: shared environment contract includes "
                    "a concrete adapter"
                )

        if path in NARROW_COMPONENT_HEADERS:
            for pattern in (
                BROAD_RUNTIME_REFERENCE,
                BROAD_BACKEND_REFERENCE,
                BROAD_RESOURCE_MANAGER_REFERENCE,
                BROAD_RESOURCE_REGISTRY_REFERENCE,
            ):
                for line in code_match_lines(text, pattern):
                    violations.append(
                        f"{relative}:{line}: component retains a broad owner"
                    )

        if path in ENVIRONMENT_CONTRACT_CONSUMERS:
            for line in code_match_lines(text, COPIED_ENVIRONMENT_CONTRACT_BODY):
                violations.append(
                    f"{relative}:{line}: concrete test duplicates the shared "
                    "environment contract body"
                )

        if path.startswith("src/simulation/tests/"):
            for line in code_match_lines(text, LOCAL_SIMULATION_PUMP):
                violations.append(
                    f"{relative}:{line}: simulation test duplicates the shared "
                    "scheduler driver"
                )

        if path.startswith("src/broker/application_") and not test_file:
            for line in code_match_lines(text, LEGACY_BROKER_RUNTIME_OWNER):
                violations.append(
                    f"{relative}:{line}: broker retains a superseded runtime owner"
                )
            for line in code_match_lines(text, BROKER_RUNTIME_CHECKPOINT):
                violations.append(
                    f"{relative}:{line}: broker retains runtime checkpoint state"
                )

        if not production_file and not simulation_file and not test_file:
            allowed = ALLOWED_PRODUCTION_INCLUDE_LINES.get(path, frozenset())
            for line in text_match_lines(text, CONCRETE_ENVIRONMENT_INCLUDE):
                if text.splitlines()[line - 1].strip() in allowed:
                    continue
                violations.append(f"{relative}:{line}: concrete runtime adapter include")

        if production_file:
            for line in text_match_lines(text, REVERSE_INCLUDE):
                violations.append(f"{relative}:{line}: production reverse dependency")

        if simulation_file and not test_file:
            for line in text_match_lines(text, PRODUCTION_INCLUDE):
                violations.append(f"{relative}:{line}: simulation reverse dependency")

        if (
            relative.suffix in HEADER_SUFFIXES
            and not production_file
            and not simulation_file
            and not test_file
            and path not in DECLARATION_OWNER_HEADERS
        ):
            for line in code_match_lines(text, BROAD_RUNTIME_REFERENCE):
                violations.append(f"{relative}:{line}: broad runtime reference")
            for line in code_match_lines(text, BROAD_RESOURCE_MANAGER_REFERENCE):
                violations.append(
                    f"{relative}:{line}: broad resource-owner reference"
                )
            if path not in RESOURCE_COMPOSITION_HEADERS:
                for line in code_match_lines(text, BROAD_RESOURCE_REGISTRY_REFERENCE):
                    violations.append(
                        f"{relative}:{line}: broad resource-owner reference"
                    )

        if path.startswith("src/runtime/") and not production_file and not test_file:
            for line in text_match_lines(text, RESOURCE_INCLUDE):
                violations.append(f"{relative}:{line}: runtime-to-resource dependency")

        if path.startswith("src/resource/") and not test_file:
            for line in code_match_lines(text, SIMULATION_TYPE_REFERENCE):
                violations.append(
                    f"{relative}:{line}: resource-to-simulation source dependency"
                )

    for relative in build_files(root):
        path = relative.as_posix()
        text = (root / relative).read_text(encoding="utf-8")
        if path == "src/runtime/testing/contracts/BUILD":
            for rule in LIBRARY_RULE.finditer(text):
                if not SHARED_ENVIRONMENT_RULE.search(rule.group()):
                    continue
                dependencies = DEPENDENCY_LIST.search(rule.group())
                if dependencies is None:
                    continue
                concrete = CONCRETE_ENVIRONMENT_DEPENDENCY.search(
                    dependencies.group(1)
                )
                if concrete is not None:
                    line = 1 + text.count(
                        "\n",
                        0,
                        rule.start() + dependencies.start(1) + concrete.start(),
                    )
                    violations.append(
                        f"{relative}:{line}: shared environment contract "
                        "depends on a concrete adapter"
                    )
        if path == "src/broker/BUILD":
            for rule in LIBRARY_RULE.finditer(text):
                name = RULE_NAME.search(rule.group())
                if (
                    EXPORTED_INTERNAL_COMPOSITION_HEADER.search(rule.group())
                    and PUBLIC_VISIBILITY.search(rule.group())
                ):
                    line = 1 + text.count("\n", 0, rule.start())
                    violations.append(
                        f"{relative}:{line}: internal composition header "
                        "exported publicly"
                    )
                if (
                    name is not None
                    and name.group(1) == "application_test_support"
                    and not TEST_ONLY.search(rule.group())
                ):
                    line = 1 + text.count("\n", 0, rule.start())
                    violations.append(
                        f"{relative}:{line}: application checkpoint support "
                        "is not test-only"
                    )
                if (
                    not TEST_ONLY.search(rule.group())
                    and APPLICATION_TEST_SUPPORT_DEPENDENCY.search(rule.group())
                ):
                    line = 1 + text.count("\n", 0, rule.start())
                    violations.append(
                        f"{relative}:{line}: production library depends on "
                        "application checkpoint support"
                    )
        adapter_build = path in {
            "src/runtime/production/BUILD",
            "src/simulation/BUILD",
        }
        test_build = any(part in EXCLUDED_PARTS for part in relative.parts)
        if adapter_build or test_build:
            continue
        for rule in LIBRARY_RULE.finditer(text):
            name = RULE_NAME.search(rule.group())
            dependencies = DEPENDENCY_LIST.search(rule.group())
            if name is None or dependencies is None:
                continue
            allowed = ALLOWED_CONCRETE_LIBRARY_DEPENDENCIES.get(
                (path, name.group(1)), frozenset()
            )
            for concrete in CONCRETE_ENVIRONMENT_DEPENDENCY.finditer(
                dependencies.group(1)
            ):
                if concrete.group(1) in allowed:
                    continue
                line = 1 + text.count(
                    "\n",
                    0,
                    rule.start() + dependencies.start(1) + concrete.start(),
                )
                violations.append(
                    f"{relative}:{line}: concrete runtime adapter dependency"
                )

    return violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject concrete runtime adapters outside composition"
    )
    parser.add_argument("--workspace", type=Path)
    arguments = parser.parse_args()
    root = (
        arguments.workspace
        or Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", Path.cwd()))
    ).resolve()
    violations = scan(root)
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("Runtime boundary checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
