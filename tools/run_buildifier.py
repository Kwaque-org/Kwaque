from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

STARLARK_SUFFIXES = frozenset({".bazel", ".bzl", ".BUILD", ".sky"})
WORKSPACE_NAMES = frozenset({"WORKSPACE", "WORKSPACE.bzlmod", "WORKSPACE.oss"})


def workspace_root() -> Path:
    configured = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    return Path(configured).resolve() if configured else Path.cwd().resolve()


def resolve_runfile(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    roots = []
    if configured := os.environ.get("RUNFILES_DIR"):
        roots.append(Path(configured))
    roots.extend(
        parent
        for parent in Path(__file__).absolute().parents
        if parent.name.endswith(".runfiles")
    )
    for root in roots:
        for base in (root, root.parent):
            resolved = base / path
            if resolved.exists():
                return resolved
    return candidate.resolve()


def is_starlark_path(path: Path) -> bool:
    name = path.name
    return (
        name == "BUILD"
        or name in WORKSPACE_NAMES
        or name.startswith("BUILD.")
        and name.endswith(".oss")
        or name.startswith("WORKSPACE.")
        and name.endswith(".oss")
        or path.suffix in STARLARK_SUFFIXES
    )


def selected_files(root: Path) -> list[Path]:
    output = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    return sorted(
        path
        for value in output.split(b"\0")
        if value
        for path in [Path(value.decode())]
        if is_starlark_path(path) and (root / path).is_file()
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Format tracked Bazel and Starlark files"
    )
    parser.add_argument("--tool", required=True)
    parser.add_argument("--mode", choices=("diff", "fix"), required=True)
    arguments = parser.parse_args()

    root = workspace_root()
    files = selected_files(root)
    if not files:
        print("No Bazel or Starlark files selected.")
        return 0
    result = subprocess.run(
        [
            str(resolve_runfile(arguments.tool)),
            f"-mode={arguments.mode}",
            *map(str, files),
        ],
        cwd=root,
        check=False,
    )
    if result.returncode == 0:
        action = "Checked" if arguments.mode == "diff" else "Formatted"
        print(f"{action} {len(files)} Bazel/Starlark file(s).")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
