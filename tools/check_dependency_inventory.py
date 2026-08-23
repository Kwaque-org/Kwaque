#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


MODULE_PATTERN = re.compile(r"bazel_dep\((.*?)\)", re.DOTALL)
NAME_PATTERN = re.compile(r'\bname\s*=\s*"([^"]+)"')
VERSION_PATTERN = re.compile(r'\bversion\s*=\s*"([^"]+)"')
WORKFLOW_REFERENCE_PATTERN = re.compile(
    r"(?<![./])\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)@([^\s\"']+)"
)

ARCHIVE_FUNCTIONS = ("http_archive", "sysroot")
ARCHIVE_EXTENSION = "native_dependencies"
STRING_PATTERN = re.compile(r'"([^"]+)"')
CONSTANT_PATTERN = re.compile(r'^([A-Z][A-Z0-9_]*)\s*=\s*"([^"]+)"', re.MULTILINE)
STRIP_PREFIX_PATTERN = re.compile(
    r'strip_prefix\s*=\s*"([^"]*)"'
    r"(?:\s*\.\s*format\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\))?"
)
CHECKSUM_PATTERN = re.compile(r'\b(?:sha256|integrity)\s*=\s*"([^"]+)"')
URL_PATTERN = re.compile(r'\burls?\s*=\s*\[?\s*"([^"]+)"')
ARCHIVE_SUFFIXES = (".tar.zst", ".tar.gz", ".tar.xz", ".tar.bz2", ".tgz", ".zip")
EXTENSION_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*use_extension\(\s*"
    r'"//bazel:extensions\.bzl"\s*,\s*"' + ARCHIVE_EXTENSION + r'"\s*\)'
)


def normalize(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


BACKTICK_PATTERN = re.compile(r"`([^`]+)`")
PARENTHETICAL_PATTERN = re.compile(r"\([^)]*\)")


def row_identifiers(title: str) -> set[str]:
    """Return the normalized names an inventory row may be addressed by.

    A title carries a readable project name and, by convention, the identifier
    the build uses in backticks. Accepting both lets matching be exact. A
    substring test would instead let 'foo' satisfy a row for 'foo-tools'.
    """
    candidates = {title, PARENTHETICAL_PATTERN.sub("", title)}
    candidates.update(BACKTICK_PATTERN.findall(title))
    return {identifier for identifier in map(normalize, candidates) if identifier}


def row_versions(cell: str) -> set[str]:
    """Return the exact version tokens a row records.

    Comparing tokens rather than testing containment keeps '1.2.3' from being
    satisfied by a row that records '11.2.3'.
    """
    tokens = re.split(r"[\s,]+", cell.replace("`", " "))
    return {token for token in (token.strip() for token in tokens) if token}


def call_bodies(text: str, function_name: str) -> list[str]:
    """Return the argument text of each call, tolerating nested calls and strings."""
    opening = re.compile(r"(?<![A-Za-z0-9_.])" + re.escape(function_name) + r"\s*\(")
    bodies = []
    for match in opening.finditer(text):
        start = index = match.end()
        depth = 1
        quote = None
        while index < len(text):
            character = text[index]
            if quote is not None:
                if character == "\\":
                    index += 2
                    continue
                if character == quote:
                    quote = None
            elif character in "\"'":
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    break
            index += 1
        bodies.append(text[start:index])
    return bodies


def module_dependencies(module_text: str) -> list[tuple[str, str | None]]:
    dependencies = []
    for match in MODULE_PATTERN.finditer(module_text):
        body = match.group(1)
        name = NAME_PATTERN.search(body)
        if name is None:
            continue
        version = VERSION_PATTERN.search(body)
        dependencies.append((name.group(1), version.group(1) if version else None))
    return dependencies


def version_from_url(url: str) -> str | None:
    basename = url.rsplit("/", 1)[-1]
    for suffix in ARCHIVE_SUFFIXES:
        if basename.endswith(suffix):
            return basename[: -len(suffix)] or None
    return None


def archive_dependencies(
    repositories_text: str, versions_text: str
) -> list[tuple[str, str | None, bool]]:
    """Return (name, pinned version or revision, has checksum) per declared archive."""
    constants = dict(CONSTANT_PATTERN.findall(versions_text))
    declarations = []
    for function_name in ARCHIVE_FUNCTIONS:
        for body in call_bodies(repositories_text, function_name):
            name = NAME_PATTERN.search(body)
            if name is None:
                continue
            version = None
            prefix = STRIP_PREFIX_PATTERN.search(body)
            if prefix is not None:
                literal, constant = prefix.group(1), prefix.group(2)
                if constant is not None:
                    literal = literal.replace("{}", constants.get(constant, constant))
                version = literal.rsplit("-", 1)[-1] or None
            if version is None:
                url = URL_PATTERN.search(body)
                if url is not None:
                    version = version_from_url(url.group(1))
            declarations.append(
                (name.group(1), version, CHECKSUM_PATTERN.search(body) is not None)
            )
    return declarations


def imported_archive_repositories(module_text: str) -> list[str] | None:
    """Return the archive repositories the root module imports, or None if unwired."""
    extension = EXTENSION_PATTERN.search(module_text)
    if extension is None:
        return None
    variable = extension.group(1)
    # Imports may be split across several use_repo calls for one extension, so
    # every matching call contributes rather than only the first.
    imported: list[str] = []
    for body in call_bodies(module_text, "use_repo"):
        arguments = [argument.strip() for argument in body.split(",")]
        if arguments and arguments[0] == variable:
            imported.extend(STRING_PATTERN.findall(body))
    return imported


def inventory_rows(inventory_text: str) -> list[tuple[str, str]]:
    rows = []
    for line in inventory_text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2 or cells[0] in {"Dependency", "---"}:
            continue
        rows.append((cells[0], cells[1]))
    return rows


def module_dependency_errors(module_text: str, inventory_text: str) -> list[str]:
    rows = inventory_rows(inventory_text)
    errors = []
    for name, version in module_dependencies(module_text):
        matching = [row for row in rows if normalize(name) in row_identifiers(row[0])]
        if not matching:
            errors.append(f"direct module dependency {name!r} is missing from THIRD_PARTY.md")
            continue
        if version is not None and all(
            version not in row_versions(row[1]) for row in matching
        ):
            errors.append(
                f"direct module dependency {name!r} version {version!r} is not inventoried"
            )
    return errors


def archive_dependency_errors(
    repositories_text: str, versions_text: str, inventory_text: str
) -> list[str]:
    rows = inventory_rows(inventory_text)
    errors = []
    for name, version, checksummed in archive_dependencies(
        repositories_text, versions_text
    ):
        matching = [row for row in rows if normalize(name) in row_identifiers(row[0])]
        if not matching:
            errors.append(f"pinned archive {name!r} is missing from THIRD_PARTY.md")
        elif version is not None and all(
            version not in row_versions(row[1]) for row in matching
        ):
            errors.append(f"pinned archive {name!r} version {version!r} is not inventoried")
        if not checksummed:
            errors.append(f"pinned archive {name!r} declares no checksum")
    return errors


def archive_import_errors(
    module_text: str, repositories_text: str, versions_text: str
) -> list[str]:
    imported = imported_archive_repositories(module_text)
    if imported is None:
        return ["MODULE.bazel does not use the pinned archive extension"]
    declared = {
        name for name, _, _ in archive_dependencies(repositories_text, versions_text)
    }
    errors = [
        f"MODULE.bazel imports {name!r}, which no pinned archive declares"
        for name in sorted(set(imported) - declared)
    ]
    errors.extend(
        f"pinned archive {name!r} is declared but never imported by MODULE.bazel"
        for name in sorted(declared - set(imported))
    )
    return errors


def workflow_dependency_errors(
    workflow_texts: list[str], inventory_text: str
) -> list[str]:
    rows = inventory_rows(inventory_text)
    references = {
        reference
        for text in workflow_texts
        for reference in WORKFLOW_REFERENCE_PATTERN.findall(text)
    }
    errors = []
    for name, revision in sorted(references):
        if not any(normalize(name) in row_identifiers(row[0]) for row in rows):
            errors.append(f"workflow dependency {name!r} is missing from THIRD_PARTY.md")
        if not re.fullmatch(r"[0-9a-fA-F]{40}|sha256:[0-9a-fA-F]{64}", revision):
            errors.append(f"workflow dependency {name!r} is not immutably pinned")
    return errors


def workspace_root() -> Path:
    configured = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    return Path(configured).resolve() if configured else Path.cwd().resolve()


def main() -> int:
    root = workspace_root()
    try:
        module_text = (root / "MODULE.bazel").read_text(encoding="utf-8")
        inventory_text = (root / "THIRD_PARTY.md").read_text(encoding="utf-8")
        compatibility_text = (root / "DEPENDENCIES.md").read_text(encoding="utf-8")
        repositories_text = (root / "bazel" / "repositories.bzl").read_text(
            encoding="utf-8"
        )
        versions_text = (root / "bazel" / "versions.bzl").read_text(encoding="utf-8")
        workflow_texts = [
            path.read_text(encoding="utf-8")
            for pattern in ("*.yml", "*.yaml")
            for path in (root / ".github").rglob(pattern)
        ]
        errors = module_dependency_errors(module_text, inventory_text)
        errors.extend(
            archive_dependency_errors(repositories_text, versions_text, inventory_text)
        )
        errors.extend(
            archive_import_errors(module_text, repositories_text, versions_text)
        )
        errors.extend(workflow_dependency_errors(workflow_texts, inventory_text))
        if not compatibility_text.strip():
            errors.append("DEPENDENCIES.md is empty")
    except OSError as error:
        print(f"unable to validate dependency inventory: {error}", file=sys.stderr)
        return 2

    if errors:
        print("Dependency inventory validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print("Dependency inventory checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
