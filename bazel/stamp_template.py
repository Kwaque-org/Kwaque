"""Expand a template with defaults overridden by Bazel stable-status values."""

from __future__ import annotations

import argparse
from pathlib import Path
from string import Template


def read_variables(path: Path) -> dict[str, str]:
    variables: dict[str, str] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        key, separator, value = line.partition(" ")
        if not separator:
            raise ValueError(f"{path}:{line_number}: expected KEY VALUE")
        variables[key] = value
    return variables


def merge_variables(
    defaults: dict[str, str], stamped: dict[str, str] | None
) -> dict[str, str]:
    """Return the defaults, replaced entirely by stamped values when given.

    A stamped build must override every default, so a key renamed on one side
    fails instead of silently reporting its unstamped default.
    """
    if stamped is None:
        return dict(defaults)
    missing = sorted(key for key in defaults if key not in stamped)
    if missing:
        raise ValueError(f"stamped status lacks {', '.join(missing)}")
    return {**defaults, **stamped}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--defaults", type=Path, required=True)
    parser.add_argument("--stamped", type=Path)
    args = parser.parse_args()

    variables = merge_variables(
        read_variables(args.defaults),
        read_variables(args.stamped) if args.stamped else None,
    )
    template = Template(args.template.read_text(encoding="utf-8"))
    args.output.write_text(template.substitute(variables), encoding="utf-8")


if __name__ == "__main__":
    main()
