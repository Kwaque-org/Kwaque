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

    def fake_native(
        self, arguments, *, cwd, stdin, stdout, stderr, timeout, check, shell
    ):
        self.assertTrue(Path(arguments[0]).is_absolute())
        self.assertIn("--no-perf-counters", arguments)
        self.assertEqual(Path(arguments[0]), self.binary)
        self.assertEqual(cwd, self.output)
        self.assertEqual(stderr, subprocess.STDOUT)
        self.assertEqual(stdin, subprocess.DEVNULL)
        self.assertFalse(shell)
        self.assertFalse(check)
        self.assertGreater(timeout, 0)
        self.assertIn("--runs=7", arguments)
        self.assertIn("--smp=1", arguments)
        self.assertEqual(
            [arg for arg in arguments if arg.startswith("--cpuset=")],
            [f"--cpuset={self.expected_cpu}"],
        )
        self.assertIn("--thread-affinity=1", arguments)
        self.assertIn("--mbind=0", arguments)
        self.assertIn("--overprovisioned", arguments)
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
        return subprocess.CompletedProcess(arguments, 0)

    def run_mocked(self, behavior=None, **kwargs):
        with mock.patch.object(
            driver.subprocess, "run", side_effect=behavior or self.fake_native
        ) as child:
            result = driver.run_comparison(
                self.binary, [PAIR], self.output, seed=41, **kwargs
            )
        return result, child

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
            with self.subTest(document=document):
                with self.assertRaises(driver.ComparisonError):
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
        with mock.patch.object(driver, "MAXIMUM_RESULT_BYTES", 10):
            with self.assertRaisesRegex(driver.ComparisonError, "size limit"):
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
            with self.subTest(baseline=baseline, candidate=candidate):
                with self.assertRaisesRegex(driver.ComparisonError, "ratio"):
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
            with self.subTest(pair=pair):
                with self.assertRaises(driver.ComparisonError):
                    driver.parse_pair(pair)

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
        self.assertEqual(child.call_count, 6)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["comparisons"][0]["status"], "win")
        self.assertEqual(result["binary"]["name"], self.binary.name)
        self.assertEqual(result["binary"]["sha256"], driver.binary_digest(self.binary))
        self.assertEqual(result["configuration"]["runs"], 7)
        self.assertEqual(result["configuration"]["rounds"], 3)
        self.assertEqual(result["configuration"]["selected_cpu"], 3)
        self.assertEqual(result["configuration"]["allowed_cpus"], [3, 7])
        self.assertTrue(result["configuration"]["thread_affinity"])
        self.assertIsNone(result["configuration"]["task_quota_ms"])
        self.affinity.assert_called_once_with(0)
        self.assertNotIn(str(self.root), json.dumps(result))
        for invocation in result["invocations"]:
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
        with mock.patch.object(
            driver.subprocess, "run", side_effect=self.fake_native
        ) as child, mock.patch.object(
            driver.os, "sched_setaffinity", create=True
        ) as set_affinity:
            result = driver.run_comparison(self.binary, pairs, self.output, cpu=7)
        self.affinity.assert_called_once_with(0)
        set_affinity.assert_not_called()
        self.assertEqual(child.call_count, 12)
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
            with self.subTest(cpu=cpu), mock.patch.object(
                driver.subprocess, "run"
            ) as child:
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
        self.assertEqual(child.call_count, 12)
        self.assertEqual(result["configuration"]["task_quota_ms"], 0.125)
        recorded = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(recorded["configuration"]["task_quota_ms"], 0.125)
        for call, invocation in zip(child.call_args_list, result["invocations"]):
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
            with self.subTest(quota=quota), mock.patch.object(
                driver.subprocess, "run"
            ) as child:
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
        with mock.patch.object(
            driver.subprocess, "run", side_effect=self.fake_native
        ) as child, mock.patch.object(driver.sys, "argv", arguments), mock.patch(
            "builtins.print"
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 6)
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
            with self.subTest(quota=quota), mock.patch.object(
                driver.subprocess, "run"
            ) as child, mock.patch.object(driver.sys, "argv", arguments), mock.patch(
                "builtins.print"
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
            with self.subTest(error=error), mock.patch.object(
                driver.subprocess, "run"
            ) as child:
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
        with mock.patch.object(
            driver.subprocess, "run", side_effect=self.fake_native
        ) as child, mock.patch.object(driver.sys, "argv", arguments), mock.patch(
            "builtins.print"
        ):
            self.assertEqual(driver.main(), 0)
        self.assertEqual(child.call_count, 6)

    def test_child_failure_timeout_missing_and_malformed_json_are_not_parity(
        self,
    ) -> None:
        for failure in ("exit", "timeout", "missing", "malformed"):

            def failed(arguments, **kwargs):
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
            self.binary.write_bytes(b"changed binary")
            return completed

        with self.assertRaisesRegex(driver.ComparisonError, "changed"):
            self.run_mocked(changed)
        manifest = (self.output / "manifest.json").read_bytes()
        with self.assertRaisesRegex(driver.ComparisonError, "must be new"):
            self.run_mocked()
        self.assertEqual((self.output / "manifest.json").read_bytes(), manifest)

    def test_valid_native_results_can_fail_the_complete_cli_gate(self) -> None:
        for reason in ("counter_failure", "timing_regression"):
            self.output = self.root / reason

            def regressed(arguments, **kwargs):
                result = self.fake_native(arguments, **kwargs)
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
                with mock.patch.object(
                    driver.subprocess, "run", side_effect=regressed
                ) as child, mock.patch.object(
                    driver.sys, "argv", arguments
                ), mock.patch(
                    "builtins.print"
                ):
                    self.assertEqual(driver.main(), 1)
                self.assertEqual(child.call_count, 6)
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
