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


def normalize(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


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
        matching = [row for row in rows if normalize(name) in normalize(row[0])]
        if not matching:
            errors.append(f"direct module dependency {name!r} is missing from THIRD_PARTY.md")
            continue
        if version is not None and all(version not in row[1] for row in matching):
            errors.append(
                f"direct module dependency {name!r} version {version!r} is not inventoried"
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
        if not any(normalize(name) in normalize(row[0]) for row in rows):
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
        workflow_texts = [
            path.read_text(encoding="utf-8")
            for pattern in ("*.yml", "*.yaml")
            for path in (root / ".github").rglob(pattern)
        ]
        errors = module_dependency_errors(module_text, inventory_text)
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
