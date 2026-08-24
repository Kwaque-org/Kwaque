"""Expand a template with defaults overridden by Bazel stable-status values."""

from __future__ import annotations

import argparse
from pathlib import Path
from string import Template


def read_variables(paths: list[Path]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for path in paths:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--variables", type=Path, nargs="+", required=True)
    args = parser.parse_args()

    variables = read_variables(args.variables)
    template = Template(args.template.read_text(encoding="utf-8"))
    args.output.write_text(template.substitute(variables), encoding="utf-8")


if __name__ == "__main__":
    main()
