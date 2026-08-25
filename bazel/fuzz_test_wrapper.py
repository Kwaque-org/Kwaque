from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path


def resolve_runfile(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path

    candidates = [Path.cwd() / path]
    if runfiles := os.environ.get("RUNFILES_DIR"):
        runfiles_root = Path(runfiles)
        candidates.append(runfiles_root / path)
        if workspace := os.environ.get("TEST_WORKSPACE"):
            candidates.append(runfiles_root / workspace / path)
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(f"runfile not found: {value}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--seed", action="append", default=[])
    parser.add_argument("fuzzer_args", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()

    temporary_root = Path(
        os.environ.get("TEST_TMPDIR") or tempfile.mkdtemp(prefix="kwaque-fuzz-")
    )
    corpus = temporary_root / "corpus"
    corpus.mkdir(parents=True, exist_ok=True)
    for index, value in enumerate(arguments.seed):
        source = resolve_runfile(value)
        shutil.copyfile(source, corpus / f"{index:03d}-{source.name}")

    fuzzer_args = arguments.fuzzer_args
    if fuzzer_args[:1] == ["--"]:
        fuzzer_args = fuzzer_args[1:]
    binary = resolve_runfile(arguments.binary)
    os.execv(binary, [str(binary), *fuzzer_args, str(corpus)])


if __name__ == "__main__":
    main()
