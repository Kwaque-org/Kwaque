"""Compare prebuilt native benchmark cases with independent paired invocations.

The caller supplies release-build evidence and reviews equal work. The native
pre-run hook reports its build capabilities and effective OOM policy. Each of three rounds
runs each case in a fresh native process, with at least seven native samples.
Hardware instruction/cycle counters are disabled; native allocator and reactor
task counters remain active. This keeps the required measurements independent
of host perf-event permissions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROUNDS = 3
MINIMUM_RUNS = 7
REGRESSION_RATIO = 1.05
CASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+")
MAXIMUM_RESULT_BYTES = 2 * 1024 * 1024
MAXIMUM_PROFILE_LOG_BYTES = 2 * 1024 * 1024
PROFILE_PREFIX = b"kwaque-benchmark-profile-v1 "
PRODUCTION_PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}
NATIVE_ARGUMENTS = (
    "--smp=1",
    "--memory=512MiB",
    "--overprovisioned",
    "--reactor-backend=epoll",
    "--abort-on-seastar-bad-alloc",
    "--unsafe-bypass-fsync=false",
    "--kernel-page-cache=false",
    "--blocked-reactor-notify-ms=2000000",
    "--no-perf-counters",
    "--overhead-threshold=0.1",
)


class ComparisonError(ValueError):
    pass


def read_runtime_profile(path: Path) -> dict[str, str]:
    try:
        with path.open("rb") as source:
            data = source.read(MAXIMUM_PROFILE_LOG_BYTES + 1)
    except OSError as error:
        raise ComparisonError("cannot read benchmark profile log") from error
    if len(data) > MAXIMUM_PROFILE_LOG_BYTES:
        raise ComparisonError("benchmark profile log exceeds size limit")
    lines = [line[len(PROFILE_PREFIX):] for line in data.splitlines()
             if line.startswith(PROFILE_PREFIX)]
    if len(lines) != 1:
        raise ComparisonError("native benchmark must report exactly one runtime profile")
    fields = {}
    try:
        for field in lines[0].decode("ascii").split():
            key, value = field.split("=")
            if key in fields:
                raise ValueError("duplicate profile field")
            fields[key] = value
    except (UnicodeError, ValueError) as error:
        raise ComparisonError("native benchmark reported a malformed runtime profile") from error
    if fields != PRODUCTION_PROFILE:
        raise ComparisonError("native benchmark does not match the production runtime profile")
    return fields


@dataclass(frozen=True)
class Pair:
    baseline: str
    candidate: str


@dataclass(frozen=True)
class Measurement:
    runs: int
    total_iterations: int
    median_ns: float
    allocations: float
    tasks: float
    overhead_ratio: float | None = None


@dataclass(frozen=True)
class Invocation:
    round: int
    pair: int
    role: str
    case: str


def parse_pair(value: str) -> Pair:
    names = value.split("=")
    if len(names) != 2 or any(
        len(name) > 200 or not CASE_NAME.fullmatch(name) for name in names
    ):
        raise ComparisonError(
            "pair must contain two exact group.case names separated by '='"
        )
    if names[0] == names[1]:
        raise ComparisonError("baseline and candidate must be distinct cases")
    return Pair(*names)


def invocation_order(pairs: list[Pair], seed: int) -> list[Invocation]:
    chooser = random.Random(seed)
    order = []
    for round_number in range(1, ROUNDS + 1):
        indices = list(range(len(pairs)))
        chooser.shuffle(indices)
        for pair_index in indices:
            roles = ["baseline", "candidate"]
            chooser.shuffle(roles)
            for role in roles:
                order.append(
                    Invocation(
                        round_number, pair_index, role, getattr(pairs[pair_index], role)
                    )
                )
    return order


def finite_number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ComparisonError(f"native field {name} must be numeric")
    try:
        number = float(value)
    except (OverflowError, ValueError) as error:
        raise ComparisonError(
            f"native field {name} is outside the finite range"
        ) from error
    if not math.isfinite(number) or number < 0 or positive and number == 0:
        raise ComparisonError(
            f"native field {name} must be finite and {'positive' if positive else 'nonnegative'}"
        )
    return number


def count(value: Any, name: str) -> int:
    number = finite_number(value, name, positive=True)
    if not number.is_integer():
        raise ComparisonError(f"native field {name} must be an integer")
    return int(number)


def parse_measurement(document: Any, case: str, runs: int) -> Measurement:
    if not isinstance(document, dict) or not isinstance(document.get("summary"), dict):
        raise ComparisonError("native JSON requires results and summary objects")
    results = document.get("results")
    if not isinstance(results, dict) or set(results) != {case}:
        raise ComparisonError(
            f"native JSON must contain exactly the requested case {case}"
        )
    result = results[case]
    if not isinstance(result, dict):
        raise ComparisonError("native case result must be an object")
    try:
        actual_runs = count(result["runs"], "runs")
        iterations = count(result["total_iterations"], "total_iterations")
        median = finite_number(result["median"], "median", positive=True)
        allocations = finite_number(result["allocs"], "allocs")
        tasks = finite_number(result["tasks"], "tasks")
    except KeyError as error:
        raise ComparisonError(
            f"native result is missing field {error.args[0]}"
        ) from error
    if actual_runs < MINIMUM_RUNS or actual_runs != runs:
        raise ComparisonError(
            f"native result must contain the requested {runs} runs (at least seven)"
        )
    overhead = (
        finite_number(result["overhead"], "overhead") if "overhead" in result else None
    )
    return Measurement(actual_runs, iterations, median, allocations, tasks, overhead)


def _unique_object(items):
    result = {}
    for name, value in items:
        if name in result:
            raise ComparisonError("native JSON contains a duplicate object key")
        result[name] = value
    return result


def _invalid_constant(value: str):
    raise ComparisonError(f"native JSON contains non-finite constant {value}")


def read_measurement(path: Path, case: str, runs: int) -> Measurement:
    try:
        with path.open("rb") as source:
            encoded = source.read(MAXIMUM_RESULT_BYTES + 1)
        if len(encoded) > MAXIMUM_RESULT_BYTES:
            raise ComparisonError("native JSON exceeds the result size limit")
        document = json.loads(
            encoded, object_pairs_hook=_unique_object, parse_constant=_invalid_constant
        )
    except OSError as error:
        raise ComparisonError(
            f"native JSON is unavailable ({error.strerror})"
        ) from error
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ComparisonError("native JSON is malformed") from error
    return parse_measurement(document, case, runs)


def compare_pair(
    pair: Pair, rounds: list[tuple[Measurement, Measurement]]
) -> dict[str, Any]:
    if len(rounds) != ROUNDS:
        raise ComparisonError(
            "comparison requires exactly three complete paired rounds"
        )
    details = []
    counters_pass = True
    for number, (baseline, candidate) in enumerate(rounds, 1):
        ratio = candidate.median_ns / baseline.median_ns
        if not math.isfinite(ratio) or ratio <= 0:
            raise ComparisonError(
                "paired median ratio is outside the finite positive range"
            )
        counter_pass = (
            candidate.allocations <= baseline.allocations
            and candidate.tasks <= baseline.tasks
        )
        counters_pass = counters_pass and counter_pass
        details.append(
            {
                "round": number,
                "baseline": asdict(baseline),
                "candidate": asdict(candidate),
                "median_ratio": ratio,
                "counters_pass": counter_pass,
            }
        )
    median_ratio = statistics.median(item["median_ratio"] for item in details)
    if not counters_pass:
        status = "counter_failure"
    elif median_ratio > REGRESSION_RATIO:
        status = "timing_regression"
    elif all(item["median_ratio"] < 1 for item in details):
        status = "win"
    else:
        status = "parity"
    return {
        **asdict(pair),
        "status": status,
        "median_ratio": median_ratio,
        "counters_pass": counters_pass,
        "rounds": details,
    }


def binary_digest(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(128 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise ComparisonError(
            f"cannot read benchmark binary ({error.strerror})"
        ) from error
    return digest.hexdigest()


def write_manifest(directory: Path, manifest: dict[str, Any]) -> None:
    temporary = directory / "manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(directory / "manifest.json")


def select_cpu(cpu: int | None) -> tuple[int, list[int]]:
    if cpu is not None and (
        isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 0
    ):
        raise ComparisonError("CPU must be a nonnegative integer")
    try:
        allowed = sorted(os.sched_getaffinity(0))
    except (AttributeError, NotImplementedError, OSError) as error:
        raise ComparisonError("cannot read process CPU affinity") from error
    if not allowed:
        raise ComparisonError("process CPU affinity contains no allowed CPUs")
    selected = allowed[0] if cpu is None else cpu
    if selected not in allowed:
        raise ComparisonError(f"CPU {selected} is outside process CPU affinity")
    return selected, allowed


def run_comparison(
    binary: Path,
    pairs: list[Pair],
    output_dir: Path,
    *,
    runs: int = 7,
    duration: float = 1.0,
    seed: int = 1,
    rounds: int = 3,
    timeout: float | None = None,
    cpu: int | None = None,
    task_quota_ms: float | None = None,
) -> dict[str, Any]:
    if isinstance(runs, bool) or not isinstance(runs, int) or runs < MINIMUM_RUNS:
        raise ComparisonError("runs must be an integer of at least seven")
    if isinstance(rounds, bool) or rounds != ROUNDS:
        raise ComparisonError("exactly three independent paired rounds are required")
    if (
        isinstance(seed, bool)
        or not isinstance(seed, int)
        or not 0 < seed <= 0xFFFFFFFF
    ):
        raise ComparisonError("seed must be an integer from 1 through 4294967295")
    duration = finite_number(duration, "duration", positive=True)
    if task_quota_ms is not None:
        task_quota_ms = finite_number(task_quota_ms, "task_quota_ms", positive=True)
    if not pairs or any(
        parse_pair(f"{pair.baseline}={pair.candidate}") != pair for pair in pairs
    ):
        raise ComparisonError("at least one valid comparison pair is required")
    if len(set(pairs)) != len(pairs):
        raise ComparisonError("duplicate comparison pairs are not permitted")
    timeout = finite_number(
        timeout if timeout is not None else max(60.0, runs * duration * 3 + 30),
        "timeout",
        positive=True,
    )
    selected_cpu, allowed_cpus = select_cpu(cpu)
    # Resolve both before setting a subprocess working directory. Never chdir
    # the parent, invoke a shell, build a target, or reuse old result files.
    try:
        binary = binary.expanduser().resolve(strict=True)
    except OSError as error:
        raise ComparisonError(
            f"benchmark binary is unavailable ({error.strerror})"
        ) from error
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ComparisonError("benchmark binary must be an executable regular file")
    output_dir = output_dir.expanduser().resolve()
    digest = binary_digest(binary)
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise ComparisonError(
            f"output directory must be new and writable ({error.strerror})"
        ) from error
    order = invocation_order(pairs, seed)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "binary": {"name": binary.name, "sha256": digest},
        "build_mode": "caller must provide release-build evidence",
        "configuration": {
            "runs": runs,
            "rounds": ROUNDS,
            "duration_seconds": duration,
            "order_seed": seed,
            "native_seed": seed,
            "timeout_seconds": timeout,
            "cores": 1,
            "memory_bytes": 512 * 1024 * 1024,
            "selected_cpu": selected_cpu,
            "allowed_cpus": allowed_cpus,
            "thread_affinity": True,
            "task_quota_ms": task_quota_ms,
            "hardware_perf_counters": False,
            "regression_ratio": REGRESSION_RATIO,
            "native_overhead_warning_ratio": 0.1,
            "required_runtime_profile": dict(PRODUCTION_PROFILE),
        },
        "pairs": [asdict(pair) for pair in pairs],
        "order": [asdict(invocation) for invocation in order],
        "invocations": [],
        "comparisons": [],
    }
    measurements: dict[tuple[int, int, str], Measurement] = {}
    write_manifest(output_dir, manifest)
    try:
        for sequence, invocation in enumerate(order, 1):
            stem = f"{sequence:04d}-round-{invocation.round}-pair-{invocation.pair + 1}-{invocation.role}"
            json_name = stem + ".json"
            log_name = stem + ".log"
            arguments = [
                *NATIVE_ARGUMENTS,
                f"--cpuset={selected_cpu}",
                "--thread-affinity=1",
                "--mbind=0",
                f"--runs={runs}",
                f"--duration={duration:.17g}",
                f"--random-seed={seed}",
                f"--test=^{re.escape(invocation.case)}$",
                f"--json-output={json_name}",
            ]
            if task_quota_ms is not None:
                arguments.append(f"--task-quota-ms={task_quota_ms:.17g}")
            record = {
                **asdict(invocation),
                "json": json_name,
                "log": log_name,
                "arguments": arguments,
                "status": "running",
            }
            manifest["invocations"].append(record)
            write_manifest(output_dir, manifest)
            if binary_digest(binary) != digest:
                raise ComparisonError(
                    "benchmark binary changed before a native invocation"
                )
            try:
                with (output_dir / log_name).open("xb") as log:
                    result = subprocess.run(
                        [str(binary), *arguments],
                        cwd=output_dir,
                        stdin=subprocess.DEVNULL,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=timeout,
                        check=False,
                        shell=False,
                        env={**os.environ, "KWAQUE_REQUIRE_BENCHMARK_PROFILE": "production"},
                    )
            except subprocess.TimeoutExpired as error:
                raise ComparisonError(
                    f"native case {invocation.case} timed out"
                ) from error
            except OSError as error:
                raise ComparisonError(
                    f"native case {invocation.case} could not execute ({error.strerror})"
                ) from error
            record["returncode"] = result.returncode
            if result.returncode != 0:
                raise ComparisonError(
                    f"native case {invocation.case} exited with status {result.returncode}"
                )
            if binary_digest(binary) != digest:
                raise ComparisonError(
                    "benchmark binary changed during a native invocation"
                )
            record["runtime_profile"] = read_runtime_profile(output_dir / log_name)
            measured = read_measurement(output_dir / json_name, invocation.case, runs)
            measurements[(invocation.pair, invocation.round, invocation.role)] = (
                measured
            )
            record.update(status="complete", measurement=asdict(measured))
            write_manifest(output_dir, manifest)
        for index, pair in enumerate(pairs):
            manifest["comparisons"].append(
                compare_pair(
                    pair,
                    [
                        (
                            measurements[index, round_number, "baseline"],
                            measurements[index, round_number, "candidate"],
                        )
                        for round_number in range(1, ROUNDS + 1)
                    ],
                )
            )
        manifest["status"] = (
            "passed"
            if all(
                item["status"] in {"win", "parity"} for item in manifest["comparisons"]
            )
            else "failed"
        )
    except ComparisonError as error:
        manifest["status"] = "failed"
        manifest["error"] = str(error)
        if (
            manifest["invocations"]
            and manifest["invocations"][-1]["status"] == "running"
        ):
            manifest["invocations"][-1].update(status="failed", error=str(error))
        write_manifest(output_dir, manifest)
        raise
    write_manifest(output_dir, manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", required=True, type=Path, help="prebuilt release benchmark binary"
    )
    parser.add_argument(
        "--pair",
        required=True,
        action="append",
        type=parse_pair,
        help="exact group.baseline=group.candidate names; repeat for additional pairs",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="new result directory on disk-backed storage",
    )
    parser.add_argument("--runs", type=int, default=MINIMUM_RUNS)
    parser.add_argument("--rounds", type=int, choices=[ROUNDS], default=ROUNDS)
    parser.add_argument(
        "--duration", type=float, default=1.0, help="seconds per native sample"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="positive recorded ordering and native RNG seed",
    )
    parser.add_argument(
        "--timeout", type=float, help="maximum seconds for each native invocation"
    )
    parser.add_argument(
        "--cpu",
        type=int,
        help="allowed CPU for every native process (default: lowest allowed CPU)",
    )
    parser.add_argument(
        "--task-quota-ms",
        type=float,
        help="override the native reactor task quota in milliseconds",
    )
    arguments = parser.parse_args()
    try:
        manifest = run_comparison(
            arguments.binary,
            arguments.pair,
            arguments.output_dir,
            runs=arguments.runs,
            duration=arguments.duration,
            seed=arguments.seed,
            rounds=arguments.rounds,
            timeout=arguments.timeout,
            cpu=arguments.cpu,
            task_quota_ms=arguments.task_quota_ms,
        )
    except (ComparisonError, OSError) as error:
        message = error.strerror if isinstance(error, OSError) else str(error)
        print(f"Benchmark comparison failed: {message}", file=sys.stderr)
        return 1
    for comparison in manifest["comparisons"]:
        print(
            f"{comparison['baseline']} -> {comparison['candidate']}: "
            f"{comparison['status']} (median ratio {comparison['median_ratio']:.6f})"
        )
    return 0 if manifest["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
