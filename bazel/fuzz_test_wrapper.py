"""Run bounded fuzzing in writable test space and retain failure diagnostics."""

from __future__ import annotations

import argparse
import os
import random
import re
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
# A target that knows why an input failed says so on a line of its own before
# it traps. A reduction then keeps only inputs that fail the same way: the
# target, given the verdict in REQUIRED_VERDICT, returns from any other
# failure as from an input that passed.
VERDICT_LINE = re.compile(rb"^kwaque-fuzz-verdict ([a-z_]{1,32})$", re.MULTILINE)
REQUIRED_VERDICT = "KWAQUE_FUZZ_REQUIRED_VERDICT"
# libFuzzer runs every input it is given before it first looks at the clock,
# so a campaign that continues a kept corpus starts from the seeds and a sample
# of the kept inputs, its size one of these; the campaign's seed chooses both.
# The time it spends before it mutates anything then stays bounded however
# large the corpus is.
CORPUS_SUBSET_SIZES = (10, 20, 50, 75, 75, 100, 100, 100, 125, 125, 150)
# libFuzzer never deletes an input it loaded, so the kept corpus is replaced by
# a merge of itself, the seeds and the new inputs after each campaign. The
# merge runs every kept input once; this bounds it.
MERGE_SECONDS = 30 * 60
FAILURE_ARTIFACTS = ("crash-*", "timeout-*", "oom-*", "leak-*")
# Before it stops, libFuzzer may run the empty input, every input it is
# given, and the round of mutations it began before its deadline (its default
# -mutate_depth), each within -timeout. The watchdog waits for all of them.
MUTATIONS_PER_ROUND = 5


def failure_verdict(log: Path) -> str | None:
    """The verdict the last failure in a campaign's log ended in, if it said."""
    try:
        found = VERDICT_LINE.findall(log.read_bytes())
    except OSError:
        return None
    return found[-1].decode("ascii") if found else None


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


def last_value(arguments: list[str], name: str) -> str | None:
    value = None
    for argument in arguments:
        if argument.startswith(f"-{name}="):
            value = argument.split("=", 1)[1]
    return value


def positive_limit(arguments: list[str], name: str, default: int, maximum: int) -> int:
    given = last_value(arguments, name)
    value = default if given is None else int(given)
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def copy_subset(kept: Path, subset: Path, chooser: random.Random) -> None:
    inputs = sorted(path for path in kept.iterdir() if path.is_file())
    size = chooser.choice(CORPUS_SUBSET_SIZES)
    if len(inputs) > size:
        inputs = chooser.sample(inputs, size)
    for source in inputs:
        shutil.copyfile(source, subset / source.name)


def failure_artifacts(artifacts: Path) -> set[str]:
    return {
        path.name for pattern in FAILURE_ARTIFACTS for path in artifacts.glob(pattern)
    }


def merge_kept_corpus(
    options: list[str],
    kept: Path,
    inputs: list[Path],
    work: Path,
    environment: dict[str, str],
    artifacts: Path,
) -> int:
    """Replace the kept corpus with the inputs of it and `inputs` that add coverage.

    The merge runs each input in a child it restarts after a failure, and
    writes the failing input as an artifact: an input that fails here fails
    the campaign. A merge that does not finish leaves the corpus as it was.
    """
    merged = work / "merged"
    merged.mkdir()
    before = failure_artifacts(artifacts)
    status = run_logged(
        [
            options[0],
            *[
                arg
                for arg in options[1:]
                if not arg.startswith(("-runs=", "-max_total_time="))
            ],
            "-merge=1",
            str(merged),
            str(kept),
            *[str(directory) for directory in inputs],
        ],
        work,
        environment,
        artifacts / "merge.log",
        MERGE_SECONDS,
    )
    if failure_artifacts(artifacts) != before:
        return 1
    if status:
        print(
            f"fuzz wrapper: the corpus merge ended with status {status}; "
            "the kept corpus is unchanged",
            flush=True,
        )
        return 0
    names = {path.name for path in merged.iterdir()}
    for path in merged.iterdir():
        shutil.copyfile(path, kept / path.name)
    for path in kept.iterdir():
        if path.is_file() and path.name not in names:
            path.unlink()
    return 0


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
    corpus = work / "corpus"
    corpus.mkdir()
    for index, source in enumerate(seeds):
        shutil.copyfile(source, corpus / f"{index:03d}-{source.name}")
    directories = [corpus]
    kept = None
    if persistent := environment.get("KWAQUE_FUZZ_CORPUS_DIR"):
        # A scheduled campaign continues from the corpus that earlier campaigns
        # kept. The caller makes the directory writable and keeps it. libFuzzer
        # writes what it finds only to the first directory it is given.
        kept = Path(persistent)
        if not kept.is_absolute():
            raise ValueError("KWAQUE_FUZZ_CORPUS_DIR must be an absolute path")
        kept.mkdir(parents=True, exist_ok=True)
        found = work / "new"
        subset = work / "subset"
        found.mkdir()
        subset.mkdir()
        copy_subset(kept, subset, random.Random(last_value(fuzzer_args, "seed")))
        directories = [found, corpus, subset]

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
    explicit = [arg for arg in fuzzer_args if not arg.startswith("-")]
    inputs = [] if explicit else [str(directory) for directory in directories]
    given = (
        len(explicit)
        if explicit
        else sum(1 for directory in directories for _ in directory.iterdir())
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
        seconds + input_timeout * (1 + given + MUTATIONS_PER_ROUND) + 5,
    )
    if kept is not None and not explicit and status == 0:
        # The subset is a copy of kept inputs, which the merge reads in place.
        status = merge_kept_corpus(
            options, kept, directories[:2], work, environment, artifacts
        )
    if status and minimize_seconds:
        # libFuzzer owns crash reduction; keep the original even if reduction times out.
        failures = sorted(artifacts.glob("crash-*")) + sorted(
            artifacts.glob("timeout-*")
        )
        reduction_environment = dict(environment)
        if verdict := failure_verdict(artifacts / "campaign.log") or failure_verdict(
            artifacts / "merge.log"
        ):
            reduction_environment[REQUIRED_VERDICT] = verdict
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
                reduction_environment,
                artifacts / "minimize.log",
                minimize_seconds + input_timeout + 5,
            )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
