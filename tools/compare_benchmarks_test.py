from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

try:
    from tools import compare_benchmarks as driver
except ModuleNotFoundError:
    import compare_benchmarks as driver


PAIR = driver.Pair("group.native", "group.kwaque")
OTHER = driver.Pair("other.native", "other.kwaque")
PROFILE_LINE = (
    b"kwaque-benchmark-profile-v1 allocator=native injection=false optimized=true "
    b"asan=false ubsan=false oom_abort=true\n"
)
# The cases the mocked binary lists, as the native harness prints them.
LISTED = (
    PAIR.baseline,
    PAIR.candidate,
    OTHER.baseline,
    OTHER.candidate,
    "maximum_noise.cold_decompress",
)


def listing(names) -> bytes:
    return b"startup diagnostics\navailable tests:\n" + b"".join(
        f"\t{name}\n".encode() for name in names
    )


def native_document(case: str, median: float = 10.0) -> dict:
    return {
        "results": {
            case: {
                "runs": 7,
                "total_iterations": 1000,
                "median": median,
                "allocs": 0,
                "tasks": 0,
                "overhead": 0.02,
            }
        },
        "summary": {"total_runtime_s": 7.1},
    }


def measurement(median: float, allocations: float = 0, tasks: float = 0):
    return driver.Measurement(7, 1000, median, allocations, tasks)


class BenchmarkComparisonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = self.root / "native benchmark"
        self.binary.write_bytes(b"prebuilt-test-fixture")
        self.binary.chmod(0o755)
        self.output = self.root / "results with spaces"
        affinity = mock.patch.object(
            driver.os, "sched_getaffinity", return_value={3, 7}, create=True
        )
        self.affinity = affinity.start()
        self.addCleanup(affinity.stop)
        self.expected_cpu = 3
        self.reactor_backend = "epoll"
        self.listed = LISTED

    def fake_native(
        self, arguments, *, cwd, stdin, stdout, stderr, timeout, check, shell, env
    ):
        self.assertTrue(Path(arguments[0]).is_absolute())
        self.assertIn("--no-perf-counters", arguments)
        self.assertIn("--abort-on-seastar-bad-alloc", arguments)
        self.assertIn("--unsafe-bypass-fsync=false", arguments)
        self.assertIn("--kernel-page-cache=false", arguments)
        self.assertEqual(env["KWAQUE_REQUIRE_BENCHMARK_PROFILE"], "production")
        self.assertEqual(Path(arguments[0]), self.binary)
        self.assertEqual(cwd, self.output)
        self.assertEqual(stderr, subprocess.STDOUT)
        self.assertEqual(stdin, subprocess.DEVNULL)
        self.assertFalse(shell)
        self.assertFalse(check)
        self.assertGreater(timeout, 0)
        self.assertIn(f"--reactor-backend={self.reactor_backend}", arguments)
        self.assertIn("--smp=1", arguments)
        self.assertEqual(
            [arg for arg in arguments if arg.startswith("--cpuset=")],
            [f"--cpuset={self.expected_cpu}"],
        )
        self.assertIn("--thread-affinity=1", arguments)
        self.assertIn("--mbind=0", arguments)
        self.assertIn("--overprovisioned", arguments)
        if "--list" in arguments:
            # The listing starts a reactor and selects, runs and writes nothing.
            self.assertFalse(
                [
                    arg
                    for arg in arguments
                    if arg.startswith(("--test=", "--runs=", "--json-output="))
                ]
            )
            stdout.write(listing(self.listed))
            return subprocess.CompletedProcess(arguments, 0)
        self.assertIn("--runs=7", arguments)
        selector = next(
            value.removeprefix("--test=")
            for value in arguments
            if value.startswith("--test=")
        )
        self.assertTrue(selector.startswith("^") and selector.endswith("$"))
        case = selector[1:-1].replace("\\.", ".")
        filename = next(
            value.removeprefix("--json-output=")
            for value in arguments
            if value.startswith("--json-output=")
        )
        self.assertFalse(Path(filename).is_absolute())
        median = 9 if case == PAIR.candidate else 10
        (cwd / filename).write_text(
            json.dumps(native_document(case, median)), encoding="utf-8"
        )
        stdout.write(b"native stdout and warnings retained\n")
        stdout.write(PROFILE_LINE)
        return subprocess.CompletedProcess(arguments, 0)

    def run_mocked(self, behavior=None, **kwargs):
        with mock.patch.object(
            driver.subprocess, "run", side_effect=behavior or self.fake_native
        ) as child:
            result = driver.run_comparison(
                self.binary, [PAIR], self.output, seed=41, **kwargs
            )
        return result, child

    def test_same_case_across_saved_binaries_records_both_identities(self):
        baseline = self.root / "saved baseline"
        baseline.write_bytes(b"saved-release-fixture")
        baseline.chmod(0o755)
        case = "maximum_noise.cold_decompress"
        invoked = []

        def run(arguments, **kwargs):
            invoked.append(Path(arguments[0]))
            with mock.patch.object(self, "binary", Path(arguments[0])):
                return self.fake_native(arguments, **kwargs)

        with mock.patch.object(driver.subprocess, "run", side_effect=run):
            result = driver.run_comparison(
                self.binary,
                [driver.Pair(case, case)],
                self.output,
                baseline_binary=baseline,
            )
        # Each binary lists its cases once and then runs one side a round.
        self.assertEqual(invoked.count(baseline), 4)
        self.assertEqual(invoked.count(self.binary), 4)
        self.assertEqual(invoked[:2], [baseline, self.binary])
        self.assertEqual(
            [item["role"] for item in result["listings"]], ["baseline", "candidate"]
        )
        self.assertEqual(
            result["baseline_binary"]["sha256"], driver.binary_digest(baseline)
        )
        self.assertEqual(result["binary"]["sha256"], driver.binary_digest(self.binary))
        self.assertEqual(result["comparisons"][0]["status"], "parity")

    def test_saved_binary_must_be_distinct_and_immutable(self):
        with self.assertRaisesRegex(driver.ComparisonError, "separate binaries"):
            driver.run_comparison(
                self.binary, [PAIR], self.output, baseline_binary=self.binary
            )
        self.assertFalse(self.output.exists())
        baseline = self.root / "saved baseline"
        baseline.write_bytes(b"saved-release-fixture")
        baseline.chmod(0o755)

        def mutate(arguments, **kwargs):
            with mock.patch.object(self, "binary", Path(arguments[0])):
                result = self.fake_native(arguments, **kwargs)
            if Path(arguments[0]) == baseline and "--list" not in arguments:
                baseline.write_bytes(b"changed during measurement")
            return result

        with (
            mock.patch.object(driver.subprocess, "run", side_effect=mutate),
            self.assertRaisesRegex(
                driver.ComparisonError, "changed during a native invocation"
            ),
        ):
            driver.run_comparison(
                self.binary, [PAIR], self.output, baseline_binary=baseline
            )

    def test_same_case_requires_saved_binary(self):
        with self.assertRaises(driver.ComparisonError):
            driver.run_comparison(
                self.binary, [driver.Pair("group.case", "group.case")], self.output
            )
        self.assertFalse(self.output.exists())

    def test_copy_of_candidate_is_not_a_before_after_baseline(self):
        baseline = self.root / "copied baseline"
        baseline.write_bytes(self.binary.read_bytes())
        baseline.chmod(0o755)
        with (
            mock.patch.object(driver.subprocess, "run") as child,
            self.assertRaisesRegex(driver.ComparisonError, "different binary contents"),
        ):
            driver.run_comparison(
                self.binary,
                [driver.Pair("group.case", "group.case")],
                self.output,
                baseline_binary=baseline,
            )
        child.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_runtime_profile_requires_native_optimized_oom_abort_without_instrumentation(
        self,
    ) -> None:
        path = self.root / "profile.log"
        path.write_bytes(b"startup diagnostics\n" + PROFILE_LINE)
        self.assertEqual(driver.read_runtime_profile(path), driver.PRODUCTION_PROFILE)
        rejected = [
            b"startup diagnostics only\n",
            PROFILE_LINE + PROFILE_LINE,
            PROFILE_LINE.replace(b"allocator=native", b"allocator=system"),
            PROFILE_LINE.replace(b"injection=false", b"injection=true"),
            PROFILE_LINE.replace(b"optimized=true", b"optimized=false"),
            PROFILE_LINE.replace(b"asan=false", b"asan=true"),
            PROFILE_LINE.replace(b"ubsan=false", b"ubsan=true"),
            PROFILE_LINE.replace(b"oom_abort=true", b"oom_abort=false"),
            PROFILE_LINE.rstrip() + b" oom_abort=true\n",
            PROFILE_LINE.replace(b"oom_abort=true", b"broken"),
            PROFILE_LINE.replace(b"allocator=native", b"allocator=\xff"),
        ]
        for index, encoded in enumerate(rejected):
            with self.subTest(case=index):
                path.write_bytes(encoded)
                with self.assertRaises(driver.ComparisonError):
                    driver.read_runtime_profile(path)
        path.write_bytes(PROFILE_LINE)
        with (
            mock.patch.object(driver, "MAXIMUM_PROFILE_LOG_BYTES", 16),
            self.assertRaisesRegex(driver.ComparisonError, "size limit"),
        ):
            driver.read_runtime_profile(path)

    def test_valid_measurements_with_wrong_runtime_profile_cannot_pass(self) -> None:
        def wrong_profile(arguments, **kwargs):
            completed = self.fake_native(arguments, **kwargs)
            if "--list" in arguments:
                return completed
            output = kwargs["stdout"]
            output.seek(0)
            output.truncate()
            output.write(PROFILE_LINE.replace(b"oom_abort=true", b"oom_abort=false"))
            return completed

        with self.assertRaisesRegex(driver.ComparisonError, "runtime profile"):
            self.run_mocked(wrong_profile)
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["comparisons"], [])
        self.assertEqual(manifest["invocations"][0]["status"], "failed")

    def test_native_schema_accepts_float_encoded_counts_and_preserves_overhead(
        self,
    ) -> None:
        document = native_document(PAIR.baseline)
        document["results"][PAIR.baseline]["runs"] = 7.0
        measured = driver.parse_measurement(document, PAIR.baseline, 7)
        self.assertEqual(measured.runs, 7)
        self.assertEqual(measured.total_iterations, 1000)
        self.assertEqual(measured.median_ns, 10)
        self.assertEqual(measured.overhead_ratio, 0.02)

    def test_native_schema_rejects_missing_wrong_extra_or_malformed_results(
        self,
    ) -> None:
        for document in (
            {},
            {"results": {}, "summary": {}},
            {"results": [], "summary": {}},
            {"results": {PAIR.candidate: {}}, "summary": {}},
            {"results": {PAIR.baseline: {}, PAIR.candidate: {}}, "summary": {}},
            {"results": {PAIR.baseline: []}, "summary": {}},
            {"results": {PAIR.baseline: {}}, "summary": {}},
        ):
            with (
                self.subTest(document=document),
                self.assertRaises(driver.ComparisonError),
            ):
                driver.parse_measurement(document, PAIR.baseline, 7)

    def test_native_numeric_fields_require_finite_valid_measurements(self) -> None:
        for field, values in {
            "runs": [6, 8, 7.5, 0, True, "7"],
            "total_iterations": [0, -1, 0.5, False],
            "median": [0, -1, math.nan, math.inf, "10"],
            "allocs": [-1, math.nan, math.inf, True],
            "tasks": [-1, math.nan, math.inf, None],
            "overhead": [-1, math.nan, math.inf],
        }.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    document = native_document(PAIR.baseline)
                    document["results"][PAIR.baseline][field] = value
                    with self.assertRaises(driver.ComparisonError):
                        driver.parse_measurement(document, PAIR.baseline, 7)

    def test_json_duplicate_keys_nonfinite_constants_and_size_limit_fail(self) -> None:
        path = self.root / "result.json"
        for text in (
            '{"results":{},"results":{},"summary":{}}',
            '{"bad":NaN}',
            "{broken",
        ):
            path.write_text(text)
            with self.assertRaises(driver.ComparisonError):
                driver.read_measurement(path, PAIR.baseline, 7)
        path.write_bytes(b" " * 20)
        with (
            mock.patch.object(driver, "MAXIMUM_RESULT_BYTES", 10),
            self.assertRaisesRegex(driver.ComparisonError, "size limit"),
        ):
            driver.read_measurement(path, PAIR.baseline, 7)

    def test_thresholds_use_median_of_paired_ratios_and_all_rounds_for_a_win(
        self,
    ) -> None:
        for medians, expected in (
            ([90, 95, 99], "win"),
            ([90, 90, 101], "parity"),
            ([100, 100, 100], "parity"),
            ([105, 105, 105], "parity"),
            ([106, 106, 99], "timing_regression"),
            ([106, 99, 99], "parity"),
        ):
            with self.subTest(medians=medians):
                result = driver.compare_pair(
                    PAIR, [(measurement(100), measurement(value)) for value in medians]
                )
                self.assertEqual(result["status"], expected)
                self.assertEqual(
                    [r["median_ratio"] for r in result["rounds"]],
                    [v / 100 for v in medians],
                )
        # Medians of the raw baseline/candidate series would imply a regression;
        # the defined comparison is the median of independent paired ratios.
        result = driver.compare_pair(
            PAIR,
            [
                (measurement(1), measurement(0.9)),
                (measurement(100), measurement(150)),
                (measurement(1000), measurement(900)),
            ],
        )
        self.assertAlmostEqual(result["median_ratio"], 0.9)
        self.assertEqual(result["status"], "parity")

    def test_counters_are_hard_gates_in_every_round(self) -> None:
        for field in ("allocations", "tasks"):
            for failed_round in range(3):
                rounds = [(measurement(100), measurement(90)) for _ in range(3)]
                candidate = measurement(90, **{field: 0.001})
                rounds[failed_round] = measurement(100), candidate
                result = driver.compare_pair(PAIR, rounds)
                self.assertEqual(result["status"], "counter_failure")
                self.assertFalse(result["rounds"][failed_round]["counters_pass"])
        with self.assertRaises(driver.ComparisonError):
            driver.compare_pair(PAIR, [(measurement(1), measurement(1))])

    def test_extreme_finite_medians_cannot_produce_an_invalid_ratio(self) -> None:
        for baseline, candidate in ((1e-320, 1e308), (1e308, 1e-320)):
            with (
                self.subTest(baseline=baseline, candidate=candidate),
                self.assertRaisesRegex(driver.ComparisonError, "ratio"),
            ):
                driver.compare_pair(
                    PAIR, [(measurement(baseline), measurement(candidate))] * 3
                )

    def test_order_is_recorded_reproducible_and_pairs_every_round(self) -> None:
        pairs = [PAIR, driver.Pair("other.native", "other.kwaque")]
        first = driver.invocation_order(pairs, 42)
        self.assertEqual(first, driver.invocation_order(pairs, 42))
        self.assertNotEqual(first, driver.invocation_order(pairs, 43))
        self.assertEqual(len(first), 12)
        for round_number in range(1, 4):
            for pair in range(2):
                selected = [
                    i for i in first if i.round == round_number and i.pair == pair
                ]
                self.assertEqual({i.role for i in selected}, {"baseline", "candidate"})

    def test_names_are_safe_exact_literals(self) -> None:
        self.assertEqual(driver.parse_pair("group.native=group.kwaque"), PAIR)
        for pair in (
            "a=b",
            "group.case=group.case",
            "group.*=other.case",
            "../group.case=other.case",
            "group.case=other.case=third.case",
            "group.case;bad=other.case",
            "group.case=other.$(bad)",
        ):
            with (
                self.subTest(pair=pair),
                self.assertRaises(driver.ComparisonError),
            ):
                driver.parse_pair(pair)

    def pairs_file(self, text: str) -> Path:
        path = self.root / "pairs.txt"
        path.write_text(text, encoding="ascii")
        return path

    def test_pairs_file_names_pairs_and_their_kinds(self) -> None:
        path = self.pairs_file(
            "# Owner-cost pairs.\n"
            "\n"
            f"{PAIR.baseline}={PAIR.candidate} device\n"
            "   \n"
            "  # An indented comment, then trailing space.\n"
            f"{OTHER.baseline}={OTHER.candidate}\tprocessor  \n"
        )
        self.assertEqual(
            driver.read_pairs_file(path), [(PAIR, "device"), (OTHER, "processor")]
        )

    def test_pairs_file_rejects_bad_lines_kinds_and_duplicates(self) -> None:
        good = f"{PAIR.baseline}={PAIR.candidate} device\n"
        for text, message in (
            (good + "other.native=other.kwaque\n", "line 2 must be"),
            (good + "other.native=other.kwaque device extra\n", "line 2 must be"),
            (good + "other.native other.kwaque device\n", "line 2 must be"),
            ("# only\n" + good + "other.native=other.kwaque disk\n", "line 3: kind"),
            (good + "other.native=other.kwaque Device\n", "line 2: kind"),
            (good + "other.*=other.kwaque device\n", "line 2: pair must contain"),
            (good + "other.case=other.case device\n", "line 2: baseline and"),
            (good + "\n" + good, "line 3 repeats a pair"),
            (good + f"{PAIR.baseline}={PAIR.candidate} processor\n", "repeats a pair"),
            ("# nothing but a comment\n\n", "names no pair"),
            ("", "names no pair"),
        ):
            with (
                self.subTest(text=text),
                self.assertRaisesRegex(driver.ComparisonError, message),
            ):
                driver.read_pairs_file(self.pairs_file(text))
        path = self.root / "pairs.txt"
        path.write_bytes(good.encode() + b"caf\xc3\xa9.one=other.two device\n")
        with self.assertRaisesRegex(driver.ComparisonError, "ASCII"):
            driver.read_pairs_file(path)
        path.write_text(good)
        with (
            mock.patch.object(driver, "MAXIMUM_PAIRS_FILE_BYTES", 16),
            self.assertRaisesRegex(driver.ComparisonError, "size limit"),
        ):
            driver.read_pairs_file(path)
        with self.assertRaisesRegex(driver.ComparisonError, "cannot read pairs file"):
            driver.read_pairs_file(self.root / "absent.txt")

    def cli(self, *selection: str) -> list[str]:
        return [
            "compare_benchmarks",
            "--binary",
            str(self.binary),
            *selection,
            "--output-dir",
            str(self.output),
        ]

    def test_cli_pairs_file_follows_pairs_given_directly(self) -> None:
        path = self.pairs_file(f"{OTHER.baseline}={OTHER.candidate} device\n")
        pair = f"{PAIR.baseline}={PAIR.candidate}"
        arguments = self.cli("--pair", pair, "--pairs-file", str(path))
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            mock.patch.object(driver.sys, "argv", arguments),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 13)
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(manifest["pairs"], [driver.asdict(PAIR), driver.asdict(OTHER)])
        # The kind is recorded; the gate's answer does not depend on it.
        self.assertEqual(manifest["configuration"]["pair_kinds"], [None, "device"])
        self.assertEqual(
            [item["status"] for item in manifest["comparisons"]], ["win", "parity"]
        )

    def test_cli_pairs_file_alone_selects_its_pairs(self) -> None:
        path = self.pairs_file(
            f"{PAIR.baseline}={PAIR.candidate} processor\n"
            f"{OTHER.baseline}={OTHER.candidate} device\n"
        )
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            mock.patch.object(driver.sys, "argv", self.cli("--pairs-file", str(path))),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 13)
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(
            manifest["configuration"]["pair_kinds"], ["processor", "device"]
        )

    def test_cli_rejects_a_bad_or_conflicting_pairs_file_before_any_child(self) -> None:
        pair = f"{PAIR.baseline}={PAIR.candidate}"
        repeated = self.pairs_file(f"{pair} device\n")
        for selection, status in (
            # The same pair given both ways is a duplicate.
            (("--pair", pair, "--pairs-file", str(repeated)), 1),
            (("--pairs-file", str(self.root / "absent.txt")), 1),
            (("--case", PAIR.baseline, "--pairs-file", str(repeated)), 2),
            ((), 2),
        ):
            with (
                self.subTest(selection=selection),
                mock.patch.object(driver.subprocess, "run") as child,
                mock.patch.object(driver.sys, "argv", self.cli(*selection)),
                mock.patch("builtins.print"),
                mock.patch.object(driver.sys, "stderr"),
            ):
                if status == 2:
                    with self.assertRaises(SystemExit) as stopped:
                        driver.main()
                    self.assertEqual(stopped.exception.code, 2)
                else:
                    self.assertEqual(driver.main(), 1)
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_pair_kinds_must_match_the_pairs(self) -> None:
        for kinds in (["disk"], ["device", "device"], [], [True]):
            with (
                self.subTest(kinds=kinds),
                mock.patch.object(driver.subprocess, "run") as child,
            ):
                with self.assertRaisesRegex(driver.ComparisonError, "kind"):
                    driver.run_comparison(
                        self.binary, [PAIR], self.output, pair_kinds=kinds
                    )
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_unlisted_names_are_refused_before_any_measurement(self) -> None:
        self.listed = (PAIR.baseline, "other.unrelated")
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            self.assertRaises(driver.ComparisonError) as refused,
        ):
            driver.run_comparison(self.binary, [OTHER, PAIR], self.output)
        # Every missing name is given, not only the first.
        self.assertEqual(
            str(refused.exception),
            "benchmark binary does not list "
            f"{PAIR.candidate}, {OTHER.candidate}, {OTHER.baseline}",
        )
        self.assertEqual(child.call_count, 1)
        self.assertIn("--list", child.call_args.args[0])
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["error"], str(refused.exception))
        self.assertEqual(manifest["invocations"], [])
        self.assertEqual(manifest["comparisons"], [])
        self.assertEqual(
            sorted(path.name for path in self.output.iterdir()),
            ["0000-list-candidate.log", "manifest.json"],
        )

    def test_each_saved_binary_is_asked_for_its_own_side(self) -> None:
        baseline = self.root / "saved baseline"
        baseline.write_bytes(b"saved-release-fixture")
        baseline.chmod(0o755)
        case = "maximum_noise.cold_decompress"

        def run(arguments, **kwargs):
            with (
                mock.patch.object(self, "binary", Path(arguments[0])),
                mock.patch.object(
                    self, "listed", () if Path(arguments[0]) == baseline else LISTED
                ),
            ):
                return self.fake_native(arguments, **kwargs)

        with (
            mock.patch.object(driver.subprocess, "run", side_effect=run) as child,
            self.assertRaisesRegex(
                driver.ComparisonError, f"^baseline binary does not list {case}$"
            ),
        ):
            driver.run_comparison(
                self.binary,
                [driver.Pair(case, case)],
                self.output,
                baseline_binary=baseline,
            )
        # Both binaries are listed before the refusal, and neither is measured.
        self.assertEqual(child.call_count, 2)

    def test_a_listing_that_fails_or_lists_nothing_measures_nothing(self) -> None:
        for failure, message in (
            ("exit", "listing exited with status 2"),
            ("timeout", "listing timed out"),
            ("silent", "did not list its cases"),
            ("changed", "changed during its case listing"),
        ):

            def failed(arguments, *, failure=failure, **kwargs):
                self.assertIn("--list", arguments)
                if failure == "exit":
                    return subprocess.CompletedProcess(arguments, 2)
                if failure == "timeout":
                    raise subprocess.TimeoutExpired(arguments, kwargs["timeout"])
                if failure == "silent":
                    kwargs["stdout"].write(f"\t{PAIR.baseline}\n".encode())
                    return subprocess.CompletedProcess(arguments, 0)
                completed = self.fake_native(arguments, **kwargs)
                self.binary.write_bytes(b"changed binary")
                return completed

            with self.subTest(failure=failure):
                self.binary.write_bytes(b"prebuilt-test-fixture")
                self.output = self.root / failure
                with self.assertRaisesRegex(driver.ComparisonError, message):
                    _, child = self.run_mocked(failed)
                manifest = json.loads((self.output / "manifest.json").read_text())
                self.assertEqual(manifest["status"], "failed")
                self.assertEqual(manifest["invocations"], [])
                self.assertEqual(manifest["comparisons"], [])

    def test_listing_reads_only_exact_names_after_its_header(self) -> None:
        path = self.root / "list.log"
        path.write_bytes(
            b"\tbefore.header\n"
            b"available tests:\n"
            b"\tgroup.native\n"
            b"\tgroup.kwaque\r\n"
            b"group.untabbed\n"
            b"\t group.spaced\n"
            b"\tnot a name\n"
            b"\t\xff.bad\n"
        )
        self.assertEqual(driver.read_listing(path), {"group.native", "group.kwaque"})
        with (
            mock.patch.object(driver, "MAXIMUM_PROFILE_LOG_BYTES", 16),
            self.assertRaisesRegex(driver.ComparisonError, "size limit"),
        ):
            driver.read_listing(path)
        with self.assertRaisesRegex(driver.ComparisonError, "cannot read"):
            driver.read_listing(self.root / "absent.log")

    def test_without_a_pairs_file_only_one_listing_is_added(self) -> None:
        result, child = self.run_mocked()
        calls = [call.args[0] for call in child.call_args_list]
        self.assertEqual(len(calls), 7)
        self.assertEqual(
            calls[0][1:],
            [
                *driver.NATIVE_ARGUMENTS,
                "--reactor-backend=epoll",
                "--cpuset=3",
                "--thread-affinity=1",
                "--mbind=0",
                "--list",
            ],
        )
        self.assertEqual(
            result["listings"],
            [
                {
                    "role": "candidate",
                    "log": "0000-list-candidate.log",
                    "arguments": calls[0][1:],
                    "returncode": 0,
                    "cases": len(LISTED),
                }
            ],
        )
        # The six measured invocations are what they were before the listing.
        self.assertFalse(any("--list" in call for call in calls[1:]))
        self.assertEqual(len(result["invocations"]), 6)
        for call, invocation in zip(calls[1:], result["invocations"], strict=True):
            self.assertEqual(call[1:], invocation["arguments"])
            self.assertEqual(
                call[1:],
                [
                    *driver.NATIVE_ARGUMENTS,
                    "--reactor-backend=epoll",
                    "--cpuset=3",
                    "--thread-affinity=1",
                    "--mbind=0",
                    "--runs=7",
                    "--duration=1",
                    "--random-seed=41",
                    f"--test=^{invocation['case'].replace('.', chr(92) + '.')}$",
                    f"--json-output={invocation['json']}",
                ],
            )
            self.assertRegex(
                invocation["json"], r"^000[1-6]-round-[123]-pair-1-\w+\.json$"
            )
        self.assertNotIn("pair_kinds", result["configuration"])
        self.assertEqual(result["pairs"], [driver.asdict(PAIR)])
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["comparisons"][0]["status"], "win")

    def test_selected_reactor_backend_reaches_every_invocation(self) -> None:
        self.reactor_backend = "linux-aio"
        with mock.patch.object(driver.subprocess, "run", side_effect=self.fake_native):
            result = driver.run_comparison(
                self.binary, [PAIR], self.output, reactor_backend="linux-aio"
            )
        self.assertEqual(result["configuration"]["reactor_backend"], "linux-aio")
        for invocation in result["invocations"]:
            self.assertIn("--reactor-backend=linux-aio", invocation["arguments"])
        with self.assertRaises(driver.ComparisonError):
            driver.run_comparison(
                self.binary, [PAIR], self.root / "other", reactor_backend="dpdk"
            )

    def test_every_case_uses_a_fresh_process_with_relative_artifact_paths(self) -> None:
        parent_cwd = Path.cwd()
        with mock.patch.object(
            driver.subprocess, "run", side_effect=self.fake_native
        ) as child:
            result = driver.run_comparison(
                Path(os.path.relpath(self.binary, parent_cwd)),
                [PAIR],
                Path(os.path.relpath(self.output, parent_cwd)),
                seed=41,
            )
        self.assertEqual(Path.cwd(), parent_cwd)
        self.assertEqual(child.call_count, 7)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["comparisons"][0]["status"], "win")
        self.assertEqual(result["binary"]["name"], self.binary.name)
        self.assertEqual(result["binary"]["sha256"], driver.binary_digest(self.binary))
        self.assertEqual(result["configuration"]["runs"], 7)
        self.assertEqual(result["configuration"]["rounds"], 3)
        self.assertEqual(result["configuration"]["selected_cpu"], 3)
        self.assertEqual(result["configuration"]["allowed_cpus"], [3, 7])
        self.assertTrue(result["configuration"]["thread_affinity"])
        self.assertEqual(
            result["configuration"]["required_runtime_profile"],
            driver.PRODUCTION_PROFILE,
        )
        self.assertIsNone(result["configuration"]["task_quota_ms"])
        self.assertEqual(result["configuration"]["reactor_backend"], "epoll")
        self.affinity.assert_called_once_with(0)
        self.assertNotIn(str(self.root), json.dumps(result))
        for invocation in result["invocations"]:
            self.assertEqual(invocation["runtime_profile"], driver.PRODUCTION_PROFILE)
            self.assertFalse(
                any(
                    arg.startswith("--task-quota-ms=")
                    for arg in invocation["arguments"]
                )
            )
            self.assertFalse(Path(invocation["json"]).is_absolute())
            self.assertTrue((self.output / invocation["json"]).is_file())
            self.assertIn(
                "warnings retained", (self.output / invocation["log"]).read_text()
            )

    def test_cpu_override_is_fixed_for_all_pairs_roles_and_rounds(self) -> None:
        self.expected_cpu = 7
        pairs = [PAIR, driver.Pair("other.native", "other.kwaque")]
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            mock.patch.object(
                driver.os, "sched_setaffinity", create=True
            ) as set_affinity,
        ):
            result = driver.run_comparison(self.binary, pairs, self.output, cpu=7)
        self.affinity.assert_called_once_with(0)
        set_affinity.assert_not_called()
        self.assertEqual(child.call_count, 13)
        self.assertEqual(result["configuration"]["selected_cpu"], 7)
        self.assertEqual(result["configuration"]["allowed_cpus"], [3, 7])
        self.assertTrue(result["configuration"]["thread_affinity"])
        for round_number in range(1, 4):
            for pair in range(2):
                invocations = [
                    item
                    for item in result["invocations"]
                    if item["round"] == round_number and item["pair"] == pair
                ]
                self.assertEqual(
                    {item["role"] for item in invocations}, {"baseline", "candidate"}
                )
                for invocation in invocations:
                    self.assertIn("--cpuset=7", invocation["arguments"])

    def test_invalid_cpu_is_rejected_before_output_or_children(self) -> None:
        for cpu in (-1, True, False, 3.0, "3", 4):
            with (
                self.subTest(cpu=cpu),
                mock.patch.object(driver.subprocess, "run") as child,
            ):
                with self.assertRaises(driver.ComparisonError):
                    driver.run_comparison(self.binary, [PAIR], self.output, cpu=cpu)
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_task_quota_override_reaches_every_pair_role_and_round(self) -> None:
        pairs = [PAIR, driver.Pair("other.native", "other.kwaque")]
        with mock.patch.object(
            driver.subprocess, "run", side_effect=self.fake_native
        ) as child:
            result = driver.run_comparison(
                self.binary, pairs, self.output, task_quota_ms=0.125
            )
        self.assertEqual(child.call_count, 13)
        self.assertEqual(result["configuration"]["task_quota_ms"], 0.125)
        recorded = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(recorded["configuration"]["task_quota_ms"], 0.125)
        # The listing starts its reactor under the same quota.
        self.assertIn("--task-quota-ms=0.125", child.call_args_list[0].args[0])
        self.assertIn("--task-quota-ms=0.125", result["listings"][0]["arguments"])
        for call, invocation in zip(
            child.call_args_list[1:], result["invocations"], strict=True
        ):
            actual = [arg for arg in call.args[0] if arg.startswith("--task-quota-ms=")]
            self.assertEqual(actual, ["--task-quota-ms=0.125"])
            self.assertEqual(
                [
                    arg
                    for arg in invocation["arguments"]
                    if arg.startswith("--task-quota-ms=")
                ],
                actual,
            )

    def test_invalid_task_quota_is_rejected_before_output_or_children(self) -> None:
        for quota in (0, -0.0, -1, math.nan, math.inf, -math.inf, True, False, "1"):
            with (
                self.subTest(quota=quota),
                mock.patch.object(driver.subprocess, "run") as child,
            ):
                with self.assertRaisesRegex(driver.ComparisonError, "task_quota_ms"):
                    driver.run_comparison(
                        self.binary, [PAIR], self.output, task_quota_ms=quota
                    )
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_cli_task_quota_is_recorded_and_passed_once_to_each_child(self) -> None:
        arguments = [
            "compare_benchmarks",
            "--binary",
            str(self.binary),
            "--pair",
            f"{PAIR.baseline}={PAIR.candidate}",
            "--output-dir",
            str(self.output),
            "--task-quota-ms",
            "60000",
        ]
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            mock.patch.object(driver.sys, "argv", arguments),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 7)
        for call in child.call_args_list:
            self.assertEqual(
                [arg for arg in call.args[0] if arg.startswith("--task-quota-ms=")],
                ["--task-quota-ms=60000"],
            )
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(manifest["configuration"]["task_quota_ms"], 60000)

    def test_cli_invalid_task_quota_never_starts_a_child(self) -> None:
        for quota in ("0", "-1", "nan", "inf", "-inf"):
            arguments = [
                "compare_benchmarks",
                "--binary",
                str(self.binary),
                "--pair",
                f"{PAIR.baseline}={PAIR.candidate}",
                "--output-dir",
                str(self.output),
                f"--task-quota-ms={quota}",
            ]
            with (
                self.subTest(quota=quota),
                mock.patch.object(driver.subprocess, "run") as child,
                mock.patch.object(driver.sys, "argv", arguments),
                mock.patch("builtins.print"),
            ):
                self.assertEqual(driver.main(), 1)
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_missing_or_empty_affinity_never_runs_unpinned(self) -> None:
        for error in (
            AttributeError(),
            NotImplementedError(),
            OSError("unavailable"),
            None,
        ):
            self.affinity.side_effect = error
            self.affinity.return_value = set()
            with (
                self.subTest(error=error),
                mock.patch.object(driver.subprocess, "run") as child,
            ):
                with self.assertRaisesRegex(driver.ComparisonError, "affinity"):
                    driver.run_comparison(self.binary, [PAIR], self.output)
                child.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_cli_passes_cpu_override_to_every_child(self) -> None:
        self.expected_cpu = 7
        arguments = [
            "compare_benchmarks",
            "--binary",
            str(self.binary),
            "--pair",
            f"{PAIR.baseline}={PAIR.candidate}",
            "--output-dir",
            str(self.output),
            "--cpu",
            "7",
        ]
        with (
            mock.patch.object(
                driver.subprocess, "run", side_effect=self.fake_native
            ) as child,
            mock.patch.object(driver.sys, "argv", arguments),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 7)

    def test_child_failure_timeout_missing_and_malformed_json_are_not_parity(
        self,
    ) -> None:
        for failure in ("exit", "timeout", "missing", "malformed"):

            def failed(arguments, *, failure=failure, **kwargs):
                if "--list" in arguments:
                    return self.fake_native(arguments, **kwargs)
                kwargs["stdout"].write(PROFILE_LINE)
                if failure == "exit":
                    return subprocess.CompletedProcess(arguments, 2)
                if failure == "timeout":
                    raise subprocess.TimeoutExpired(arguments, kwargs["timeout"])
                if failure == "malformed":
                    filename = next(
                        a.removeprefix("--json-output=")
                        for a in arguments
                        if a.startswith("--json-output=")
                    )
                    (kwargs["cwd"] / filename).write_text("{invalid")
                return subprocess.CompletedProcess(arguments, 0)

            with self.subTest(failure=failure):
                self.output = self.root / failure
                with self.assertRaises(driver.ComparisonError):
                    self.run_mocked(failed)
                manifest = json.loads((self.output / "manifest.json").read_text())
                self.assertEqual(manifest["status"], "failed")
                self.assertEqual(manifest["comparisons"], [])
                self.assertEqual(manifest["invocations"][-1]["status"], "failed")

    def test_binary_change_is_rejected_and_original_results_are_not_overwritten(
        self,
    ) -> None:
        def changed(arguments, **kwargs):
            completed = self.fake_native(arguments, **kwargs)
            if "--list" not in arguments:
                self.binary.write_bytes(b"changed binary")
            return completed

        with self.assertRaisesRegex(
            driver.ComparisonError, "changed during a native invocation"
        ):
            self.run_mocked(changed)
        manifest = (self.output / "manifest.json").read_bytes()
        with self.assertRaisesRegex(driver.ComparisonError, "must be new"):
            self.run_mocked()
        self.assertEqual((self.output / "manifest.json").read_bytes(), manifest)

    def test_valid_native_results_can_fail_the_complete_cli_gate(self) -> None:
        for reason in ("counter_failure", "timing_regression"):
            self.output = self.root / reason

            def regressed(arguments, *, reason=reason, **kwargs):
                result = self.fake_native(arguments, **kwargs)
                if "--list" in arguments:
                    return result
                filename = next(
                    a.removeprefix("--json-output=")
                    for a in arguments
                    if a.startswith("--json-output=")
                )
                path = kwargs["cwd"] / filename
                document = json.loads(path.read_text())
                if PAIR.candidate in document["results"]:
                    row = document["results"][PAIR.candidate]
                    row["allocs" if reason == "counter_failure" else "median"] = (
                        1 if reason == "counter_failure" else 11
                    )
                    path.write_text(json.dumps(document))
                return result

            arguments = [
                "compare_benchmarks",
                "--binary",
                str(self.binary),
                "--pair",
                f"{PAIR.baseline}={PAIR.candidate}",
                "--output-dir",
                str(self.output),
                "--seed",
                "41",
            ]
            with self.subTest(reason=reason):
                with (
                    mock.patch.object(
                        driver.subprocess, "run", side_effect=regressed
                    ) as child,
                    mock.patch.object(driver.sys, "argv", arguments),
                    mock.patch("builtins.print"),
                ):
                    self.assertEqual(driver.main(), 1)
                self.assertEqual(child.call_count, 7)
                manifest = json.loads((self.output / "manifest.json").read_text())
                self.assertEqual(manifest["status"], "failed")
                self.assertEqual(manifest["comparisons"][0]["status"], reason)
                self.assertTrue(
                    all(
                        item["status"] == "complete" for item in manifest["invocations"]
                    )
                )

    def test_invalid_configuration_never_starts_a_native_process(self) -> None:
        for options in (
            {"runs": 6},
            {"runs": True},
            {"rounds": 2},
            {"duration": 0},
            {"duration": math.inf},
            {"seed": 0},
            {"seed": 2**32},
            {"timeout": -1},
        ):
            with self.subTest(options=options):
                with mock.patch.object(driver.subprocess, "run") as child:
                    with self.assertRaises(driver.ComparisonError):
                        driver.run_comparison(
                            self.binary, [PAIR], self.output, **options
                        )
                    child.assert_not_called()
                self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
