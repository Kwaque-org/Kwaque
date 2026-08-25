from __future__ import annotations

import subprocess
import sys
from pathlib import PurePosixPath

FORBIDDEN_DIRECTORIES = frozenset({"bazel-bin", "bazel-out", "bazel-testlogs"})
FORBIDDEN_NAMES = frozenset({"compile_commands.json"})
FORBIDDEN_SUFFIXES = (".o", ".pyc", ".pb.cc", ".pb.h", "_generated.h")


def is_forbidden(path: str) -> bool:
    parsed = PurePosixPath(path)
    return (
        parsed.name in FORBIDDEN_NAMES
        or bool(FORBIDDEN_DIRECTORIES.intersection(parsed.parts))
        or parsed.name.endswith(FORBIDDEN_SUFFIXES)
        or (
            parsed.parts
            and parsed.parts[0].startswith("bazel-")
            and parsed.parts[0] != "bazel-thirdparty"
        )
    )


def main() -> int:
    output = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    tracked = sorted(value.decode() for value in output.split(b"\0") if value)
    forbidden = [path for path in tracked if is_forbidden(path)]
    if not forbidden:
        return 0
    print("Generated artifacts must not be committed:", file=sys.stderr)
    for path in forbidden:
        print(f"  {path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
