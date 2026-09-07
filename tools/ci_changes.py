"""Skip native CI only when a complete Git diff proves documentation-only changes."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path, PurePosixPath

ROOT_DOCUMENTS = frozenset({"CODE_OF_CONDUCT.md", "CONTRIBUTING.md", "SECURITY.md"})


def documentation_path(path: str) -> bool:
    value = PurePosixPath(path)
    return (
        value.name == "README.md"
        or path in ROOT_DOCUMENTS
        or (value.parts[0] == "docs" and value.suffix in {".md", ".rst", ".adoc"})
    )


def documentation_only(diff: bytes) -> bool:
    fields = diff.split(b"\0")
    if fields.pop() != b"" or len(fields) % 2 or any(not path for path in fields[1::2]):
        raise ValueError("incomplete Git name-status output")
    return all(
        status in {b"A", b"M"} and documentation_path(os.fsdecode(path))
        for status, path in zip(fields[::2], fields[1::2])
    )


def comparison_base(event_name: str, event: dict) -> str | None:
    if event_name == "pull_request":
        return event["pull_request"]["base"]["sha"]
    if event_name == "push":
        return event["before"]
    if event_name == "merge_group":
        return event["merge_group"]["base_sha"]
    # Manual workflow dispatches and unknown events receive the complete checks.
    return None


def requires_checks(event_name: str, event: dict, head: str, root: Path) -> bool:
    try:
        base = comparison_base(event_name, event)
        if base is None:
            return True
        for revision in (base, head):
            if not re.fullmatch(
                r"[0-9a-f]{40}|[0-9a-f]{64}", revision
            ) or not revision.strip("0"):
                raise ValueError("missing comparison revision")
        actual_head = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        if actual_head != head:
            raise ValueError("checkout does not match the workflow revision")
        # Compare the tested merge tree for PRs/merge queues and the complete
        # before/after range for pushes. Disabling rename detection retains both
        # sides of a source-to-document rename. NUL delimiters preserve filenames.
        diff = subprocess.run(
            [
                "git",
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                "--name-status",
                "-z",
                base,
                head,
                "--",
            ],
            cwd=root,
            check=True,
            capture_output=True,
            timeout=30,
        ).stdout
        return not documentation_only(diff)
    except (
        KeyError,
        TypeError,
        ValueError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"Cannot prove documentation-only changes; running all checks: {error}")
        return True


def main() -> None:
    try:
        event = json.loads(
            Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8")
        )
        run_checks = requires_checks(
            os.environ["GITHUB_EVENT_NAME"],
            event,
            os.environ["GITHUB_SHA"],
            Path.cwd(),
        )
    except (KeyError, ValueError, OSError) as error:
        print(f"Cannot read workflow event; running all checks: {error}")
        run_checks = True
    value = str(run_checks).lower()
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        output.write(f"run_checks={value}\n")
    print(
        "Full CI checks selected"
        if run_checks
        else "Documentation-only changes; native CI skipped"
    )


if __name__ == "__main__":
    main()
