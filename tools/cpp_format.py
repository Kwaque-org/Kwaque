from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

CPP_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"})


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


def git_paths(root: Path, arguments: list[str]) -> list[Path]:
    output = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    return [Path(value.decode()) for value in output.split(b"\0") if value]


def selected_files(root: Path, scope: str, explicit: list[str]) -> list[Path]:
    if explicit:
        candidates = [Path(value) for value in explicit]
    elif scope == "all":
        candidates = git_paths(root, ["ls-files", "-z"])
    else:
        candidates = []
        candidates.extend(git_paths(root, ["diff", "--name-only", "-z", "HEAD", "--"]))
        candidates.extend(
            git_paths(root, ["ls-files", "--others", "--exclude-standard", "-z"])
        )

    files = {
        path
        for path in candidates
        if path.suffix.lower() in CPP_SUFFIXES and (root / path).is_file()
    }
    return sorted(files)


def main() -> int:
    parser = argparse.ArgumentParser(description="Format Kwaque C++ source files")
    parser.add_argument("--tool", required=True)
    parser.add_argument("--scope", choices=("all", "changed"), default="changed")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("files", nargs="*")
    arguments = parser.parse_args()

    root = workspace_root()
    files = selected_files(root, arguments.scope, arguments.files)
    if not files:
        print("No C++ files selected.")
        return 0

    command = [str(resolve_runfile(arguments.tool)), "--style=file"]
    if arguments.check:
        command.extend(["--dry-run", "--Werror"])
    else:
        command.append("-i")
    command.extend(str(path) for path in files)
    result = subprocess.run(command, cwd=root, check=False)
    if result.returncode == 0:
        action = "Checked" if arguments.check else "Formatted"
        print(f"{action} {len(files)} C++ file(s).")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
