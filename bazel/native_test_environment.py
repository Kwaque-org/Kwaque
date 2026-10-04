"""Environment for native processes that a test harness starts.

Bazel passes the sanitizer suppression files and the symbolizer as paths
relative to the test's working directory. A child that runs in another
directory cannot open them and aborts before it reaches its own code, so a
harness starts every native process with `normalized_environment()`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

_SANITIZER_OPTIONS = ("ASAN_OPTIONS", "LSAN_OPTIONS", "UBSAN_OPTIONS")
_PATH_OPTIONS = frozenset({"suppressions", "external_symbolizer_path"})


def resolve_runfile(value: str) -> Path:
    path = Path(value)
    candidates = [path] if path.is_absolute() else [Path.cwd() / path]
    if not path.is_absolute() and (runfiles := os.environ.get("RUNFILES_DIR")):
        candidates.append(Path(runfiles) / path)
        if workspace := os.environ.get("TEST_WORKSPACE"):
            candidates.append(Path(runfiles) / workspace / path)
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(f"runfile not found: {value}")


def normalized_environment(
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Returns the environment with every sanitizer path made absolute.

    The process environment is the default. An already absolute path is kept,
    so the result can be normalized again.
    """
    result = dict(os.environ if environment is None else environment)
    if symbolizer := result.get("ASAN_SYMBOLIZER_PATH"):
        result["ASAN_SYMBOLIZER_PATH"] = str(resolve_runfile(symbolizer))
    for variable in _SANITIZER_OPTIONS:
        if variable not in result:
            continue
        options = result[variable].split(":")
        for index, option in enumerate(options):
            key, separator, value = option.partition("=")
            if separator and key in _PATH_OPTIONS and value:
                options[index] = f"{key}={resolve_runfile(value)}"
        result[variable] = ":".join(options)
    return result
