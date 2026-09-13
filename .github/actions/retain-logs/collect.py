"""Select diagnostics belonging to failed Bazel tests."""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from pathlib import Path


def failed_report(report: Path) -> bool:
    try:
        root = ET.parse(report).getroot()
        return any(
            node.tag.rsplit("}", 1)[-1] in {"failure", "error"}
            or int(node.get("failures", "0")) > 0
            or int(node.get("errors", "0")) > 0
            for node in root.iter()
        )
    except (ET.ParseError, ValueError):
        # A broken test report is itself diagnostic evidence.
        return True


def failure_paths(
    logs: Path = Path("bazel-testlogs"), binaries: Path = Path("bazel-bin")
) -> list[str]:
    paths = set()
    for report in logs.rglob("test.xml"):
        relative = report.parent.relative_to(logs)
        if "test.outputs" in relative.parts or not failed_report(report):
            continue
        for pattern in ("test.log", "test.xml", "test.outputs/**", "test_attempts/**"):
            paths.add(str(report.parent / pattern))
        # Repeated and sharded runs can nest below the owning target directory.
        while relative != Path("."):
            binary = binaries / relative
            if binary.is_file():
                paths.add(str(binary))
                break
            relative = relative.parent
    return sorted(paths)


def main() -> None:
    paths = failure_paths()
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        output.write(f"has_failures={'true' if paths else 'false'}\n")
        if paths:
            output.write("paths<<FAILED_TEST_PATHS\n")
            output.write("\n".join(paths) + "\nFAILED_TEST_PATHS\n")
    if not paths:
        print("No failed test reports; skipping artifact upload.")


if __name__ == "__main__":
    main()
