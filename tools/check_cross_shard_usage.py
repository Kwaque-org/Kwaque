from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SOURCE_SUFFIXES = frozenset(
    {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".inl", ".ipp", ".tcc"}
)
EXCLUDED_PARTS = frozenset({"testing", "tests", "testdata"})


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]


RULES = (
    Rule(
        "direct cross-shard submission",
        re.compile(r"\bsmp\s*::\s*submit_to\s*\("),
    ),
    Rule(
        "direct sharded-service ownership",
        re.compile(r"\bsharded\s*<"),
    ),
    Rule(
        "foreign pointer",
        re.compile(r"\b(?:foreign_ptr|make_foreign)\b"),
    ),
)

ALLOWED_RULE_COUNTS = {
    "src/admin/admin_server.cc": {"direct sharded-service ownership": 1},
    "src/runtime/cross_shard.h": {"direct cross-shard submission": 1},
    "src/runtime/sharded_service.h": {"direct sharded-service ownership": 1},
    "src/broker/application_start.cc": {"direct cross-shard submission": 1},
}


def splice_lines(source: str) -> tuple[str, tuple[int, ...]]:
    """Apply C++ line splicing and map logical characters to source offsets."""
    logical: list[str] = []
    physical_offsets: list[int] = []
    size = len(source)
    index = 0

    while index < size:
        if source[index] == "\\" and index + 1 < size:
            if source[index + 1] == "\n":
                index += 2
                continue
            if source[index + 1] == "\r":
                index += 3 if index + 2 < size and source[index + 2] == "\n" else 2
                continue

        logical.append(source[index])
        physical_offsets.append(index)
        index += 1

    return "".join(logical), tuple(physical_offsets)


def code_view(source: str) -> tuple[str, tuple[int, ...]]:
    """Return maskable C++ code and its logical-to-physical offset map."""
    logical_source, physical_offsets = splice_lines(source)
    masked = list(logical_source)
    size = len(logical_source)

    def mask(start: int, end: int) -> None:
        for index in range(start, end):
            if logical_source[index] not in "\r\n":
                masked[index] = " "

    def raw_string_end(start: int) -> int | None:
        if not logical_source.startswith('R"', start):
            return None
        delimiter_start = start + 2
        opening = logical_source.find("(", delimiter_start, delimiter_start + 17)
        if opening < 0:
            return None
        delimiter = logical_source[delimiter_start:opening]
        if any(character.isspace() or character in "()\\" for character in delimiter):
            return None
        closing = ")" + delimiter + '"'
        found = logical_source.find(closing, opening + 1)
        return size if found < 0 else found + len(closing)

    index = 0
    while index < size:
        raw_end = raw_string_end(index)
        if raw_end is not None:
            mask(index, raw_end)
            index = raw_end
            continue

        if logical_source.startswith("//", index):
            end = index + 2
            while end < size and logical_source[end] not in "\r\n":
                end += 1
            mask(index, end)
            index = end
            continue

        if logical_source.startswith("/*", index):
            closing = logical_source.find("*/", index + 2)
            end = size if closing < 0 else closing + 2
            mask(index, end)
            index = end
            continue

        if logical_source[index] in "\"'":
            quote = logical_source[index]
            end = index + 1
            while end < size:
                if logical_source[end] == "\\":
                    end = min(end + 2, size)
                    continue
                end += 1
                if logical_source[end - 1] == quote:
                    break
                if logical_source[end - 1] in "\r\n":
                    break
            mask(index, end)
            index = end
            continue

        index += 1

    return "".join(masked), physical_offsets


def line_number(source: str, offset: int) -> int:
    """Return the physical line at offset for LF, CRLF, and CR input."""
    prefix = source[:offset]
    return 1 + prefix.count("\n") + prefix.count("\r") - prefix.count("\r\n")


def is_production_source(path: Path) -> bool:
    return (
        path.suffix in SOURCE_SUFFIXES
        and not path.name.endswith(("_test.cc", "_test.h"))
        and not any(part in EXCLUDED_PARTS for part in path.parts)
    )


def source_files(root: Path, paths: list[Path]) -> list[Path]:
    files: set[Path] = set()
    for supplied in paths:
        resolved = root / supplied
        if resolved.is_file() and is_production_source(supplied):
            files.add(supplied)
        elif resolved.is_dir():
            files.update(
                candidate.relative_to(root)
                for candidate in resolved.rglob("*")
                if candidate.is_file()
                and is_production_source(candidate.relative_to(root))
            )
    return sorted(files)


def scan_paths(
    root: Path,
    paths: list[Path],
    allowed_rules: dict[str, dict[str, int]] | None = None,
) -> list[str]:
    allowed = ALLOWED_RULE_COUNTS if allowed_rules is None else allowed_rules
    violations: list[str] = []
    for relative in source_files(root, paths):
        with (root / relative).open(encoding="utf-8", newline="") as source:
            text = source.read()
        code, physical_offsets = code_view(text)
        permitted = allowed.get(relative.as_posix(), {})
        for rule in RULES:
            matches = tuple(rule.pattern.finditer(code))
            allowed_count = permitted.get(rule.name, 0)
            for match in matches[allowed_count:]:
                line = line_number(text, physical_offsets[match.start()])
                violations.append(f"{relative}:{line}: {rule.name}")
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject direct cross-shard primitives outside runtime owners"
    )
    parser.add_argument("paths", nargs="*", default=["src"])
    arguments = parser.parse_args()

    root = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", Path.cwd())).resolve()
    violations = scan_paths(root, [Path(path) for path in arguments.paths])
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("Cross-shard ownership checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
