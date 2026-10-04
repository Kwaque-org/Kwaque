"""Run bounded fuzzing in writable test space and retain failure diagnostics."""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

from bazel.native_test_environment import normalized_environment, resolve_runfile


# Each target bounds its own input with -max_len, sized to what it parses.
# This ceiling only rejects a campaign whose inputs would not be bounded.
MAXIMUM_INPUT_BYTES = 1 << 20


def normalized_arguments(arguments: list[str]) -> list[str]:
    normalized = []
    for argument in arguments:
        key, _, value = argument.partition("=")
        if key in {"-artifact_prefix", "-exact_artifact_path", "-merge_control_file"}:
            raise ValueError("artifact paths are owned by the test wrapper")
        if (
            key in {"-jobs", "-workers", "-fork", "-merge", "-minimize_crash"}
            and value != "0"
        ):
            raise ValueError("the test wrapper runs one bounded fuzz campaign")
        if key == "-dict":
            argument = f"-dict={resolve_runfile(value)}"
        elif not argument.startswith("-"):
            argument = str(resolve_runfile(argument))
        normalized.append(argument)
    return normalized


def positive_limit(arguments: list[str], name: str, default: int, maximum: int) -> int:
    value = default
    for argument in arguments:
        if argument.startswith(f"-{name}="):
            value = int(argument.split("=", 1)[1])
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def run_logged(
    command: list[str], work: Path, environment: dict[str, str], log: Path, timeout: int
) -> int:
    with log.open("w+b") as output:
        with subprocess.Popen(
            command,
            cwd=work,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as child:
            try:
                status = child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
                output.write(b"\nfuzz wrapper: external watchdog expired\n")
                status = 124
        output.seek(0)
        for raw in output:
            sys.stdout.write(raw.decode("utf-8", errors="replace"))
    sys.stdout.flush()
    return status if status >= 0 else 128 - status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--seed", action="append", default=[])
    parser.add_argument("fuzzer_args", nargs=argparse.REMAINDER)
    arguments = parser.parse_args(argv)
    raw_args = arguments.fuzzer_args
    if raw_args[:1] == ["--"]:
        raw_args = raw_args[1:]

    # Resolve every runfile while still in the caller's runfiles directory.
    binary = resolve_runfile(arguments.binary)
    seeds = [resolve_runfile(value) for value in arguments.seed]
    fuzzer_args = normalized_arguments(raw_args)
    environment = normalized_environment()
    seconds = positive_limit(fuzzer_args, "max_total_time", 2, 600)
    input_timeout = positive_limit(fuzzer_args, "timeout", 15, 60)
    maximum_length = positive_limit(fuzzer_args, "max_len", 4096, MAXIMUM_INPUT_BYTES)
    minimize_seconds = int(environment.get("KWAQUE_FUZZ_MINIMIZE_SECONDS", "0"))
    if not 0 <= minimize_seconds <= 60:
        raise ValueError("KWAQUE_FUZZ_MINIMIZE_SECONDS must be between 0 and 60")

    for source in seeds:
        if source.stat().st_size > maximum_length:
            raise ValueError("seed exceeds the target input limit")

    temporary_root = Path(
        environment.get("TEST_TMPDIR") or tempfile.gettempdir()
    ).resolve()
    temporary_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="kwaque-fuzz-", dir=temporary_root))
    output_root = Path(environment.get("TEST_UNDECLARED_OUTPUTS_DIR") or work).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix="fuzz-", dir=output_root))
    if persistent := environment.get("KWAQUE_FUZZ_CORPUS_DIR"):
        # A scheduled campaign continues from the corpus that earlier campaigns
        # grew. The caller makes the directory writable and keeps it.
        corpus = Path(persistent)
        if not corpus.is_absolute():
            raise ValueError("KWAQUE_FUZZ_CORPUS_DIR must be an absolute path")
        corpus.mkdir(parents=True, exist_ok=True)
    else:
        corpus = work / "corpus"
        corpus.mkdir()
    for index, source in enumerate(seeds):
        shutil.copyfile(source, corpus / f"{index:03d}-{source.name}")

    environment["TMPDIR"] = str(work)
    environment["LLVM_PROFILE_FILE"] = str(work / "coverage-%p.profraw")
    options = [
        str(binary),
        *fuzzer_args,
        f"-max_total_time={seconds}",
        f"-timeout={input_timeout}",
        f"-max_len={maximum_length}",
        f"-artifact_prefix={artifacts}/",
    ]
    inputs = (
        [] if any(not arg.startswith("-") for arg in fuzzer_args) else [str(corpus)]
    )
    print(
        f"fuzz target: {binary.name}; campaign seconds: {seconds}; max input: {maximum_length}",
        flush=True,
    )
    status = run_logged(
        [*options, *inputs],
        work,
        environment,
        artifacts / "campaign.log",
        seconds + input_timeout + 5,
    )
    if status and minimize_seconds:
        # libFuzzer owns crash reduction; keep the original even if reduction times out.
        failures = sorted(artifacts.glob("crash-*")) + sorted(
            artifacts.glob("timeout-*")
        )
        for original in failures[:1]:
            minimized = artifacts / f"minimized-{original.name}"
            shutil.copyfile(original, minimized)
            reduction_options = [
                str(binary),
                *[
                    arg
                    for arg in options[1:]
                    if arg.startswith("-")
                    and not arg.startswith(("-runs=", "-max_total_time="))
                ],
            ]
            run_logged(
                [
                    *reduction_options,
                    "-minimize_crash=1",
                    f"-max_total_time={minimize_seconds}",
                    f"-exact_artifact_path={minimized}",
                    str(original),
                ],
                work,
                environment,
                artifacts / "minimize.log",
                minimize_seconds + input_timeout + 5,
            )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
