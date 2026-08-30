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
RESOURCE_INCLUDE = re.compile(
    r'^\s*#\s*include\s*[<"]src/resource/', re.MULTILINE
)
BROAD_RUNTIME_REFERENCE = re.compile(r"\bbasic_runtime\s*<")
PRODUCTION_DEPENDENCY = re.compile(r'"//src/runtime/production(?::|/)')
LIBRARY_RULE = re.compile(r"kwaque_cc_library\((?:(?!\n\)).)*\n\)", re.DOTALL)
EXPORTED_INTERNAL_COMPOSITION_HEADER = re.compile(
    r"\bhdrs\s*=\s*\[[^\]]*\"application_internal\.h\"", re.DOTALL
)
PUBLIC_VISIBILITY = re.compile(r'"//visibility:public"')
ALLOWED_PRODUCTION_INCLUDE_LINES = {
    "src/broker/application_internal.h": frozenset(
        {'#include "src/runtime/production/backend.h"'}
    ),
}
ALLOWED_PRODUCTION_LABEL_LINES = {
    "src/broker/BUILD": frozenset({'"//src/runtime/production:backend",'}),
    "src/runtime/BUILD": frozenset(
        {'"//src/runtime/production:__pkg__",'}
    ),
}
DECLARATION_OWNER_HEADERS = frozenset({"src/runtime/environment.h"})

def is_test_path(path: Path) -> bool:
    return path.name.endswith(("_test.cc", "_test.h", "_bench.cc")) or any(
        part in EXCLUDED_PARTS for part in path.parts
    )


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
    for relative in source_files(root):
        text = (root / relative).read_text(encoding="utf-8")
        path = relative.as_posix()
        production_file = path.startswith("src/runtime/production/")
        test_file = is_test_path(relative)

        if not production_file and not test_file:
            allowed = ALLOWED_PRODUCTION_INCLUDE_LINES.get(path, frozenset())
            for line in text_match_lines(text, PRODUCTION_INCLUDE):
                if text.splitlines()[line - 1].strip() in allowed:
                    continue
                violations.append(f"{relative}:{line}: concrete runtime adapter include")

        if production_file:
            for line in text_match_lines(text, REVERSE_INCLUDE):
                violations.append(f"{relative}:{line}: production reverse dependency")

        if (
            relative.suffix in HEADER_SUFFIXES
            and not production_file
            and not test_file
            and path not in DECLARATION_OWNER_HEADERS
        ):
            for line in code_match_lines(text, BROAD_RUNTIME_REFERENCE):
                violations.append(f"{relative}:{line}: broad runtime reference")

        if path.startswith("src/runtime/") and not production_file and not test_file:
            for line in text_match_lines(text, RESOURCE_INCLUDE):
                violations.append(f"{relative}:{line}: runtime-to-resource dependency")

    for relative in build_files(root):
        path = relative.as_posix()
        if path in {
            "src/runtime/production/BUILD",
            "src/runtime/tests/BUILD",
        }:
            continue
        text = (root / relative).read_text(encoding="utf-8")
        if path == "src/broker/BUILD":
            for rule in LIBRARY_RULE.finditer(text):
                if (
                    EXPORTED_INTERNAL_COMPOSITION_HEADER.search(rule.group())
                    and PUBLIC_VISIBILITY.search(rule.group())
                ):
                    line = 1 + text.count("\n", 0, rule.start())
                    violations.append(
                        f"{relative}:{line}: internal composition header "
                        "exported publicly"
                    )
        allowed = ALLOWED_PRODUCTION_LABEL_LINES.get(path, frozenset())
        for match in PRODUCTION_DEPENDENCY.finditer(text):
            line = 1 + text.count("\n", 0, match.start())
            if text.splitlines()[line - 1].strip() in allowed:
                continue
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
