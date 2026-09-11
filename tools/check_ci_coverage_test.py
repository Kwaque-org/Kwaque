"""Guard the CI coverage contract; actionlint remains the YAML/workflow parser."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

WORKFLOW_ARGUMENTS = len(sys.argv) == 5 and sys.argv[1].endswith((".yml", ".yaml"))
WORKFLOW = (
    Path(sys.argv[1])
    if WORKFLOW_ARGUMENTS
    else Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
)
GOLDENS = "//src/simulation/tests:determinism_goldens"
FUZZ_WORKFLOW = (
    Path(sys.argv[2]) if WORKFLOW_ARGUMENTS else WORKFLOW.with_name("fuzz.yml")
)
BAZEL_CONFIG = (
    Path(sys.argv[3]) if WORKFLOW_ARGUMENTS else WORKFLOW.parents[2] / ".bazelrc"
)
SETUP_BUILD = (
    Path(sys.argv[4])
    if WORKFLOW_ARGUMENTS
    else WORKFLOW.parents[1] / "actions/setup-build/action.yml"
)
STATEFUL_FUZZERS = {
    "scheduler_fuzz",
    "fault_schedule_fuzz",
    "fake_file_fuzz",
    "fake_network_fuzz",
}
SMOKE_FUZZERS = {f"//src/simulation/tests:{name}" for name in STATEFUL_FUZZERS} | {
    "//src/config:bootstrap_config_fuzz",
    "//proto/kwaque/common/v1:build_info_fuzz",
    "//src/bytes:fragmented_buffer_fuzz",
    "//src/codec/tests:codec_fuzz",
    "//src/codec/tests:codec_cooperative_fuzz",
    "//src/simulation/tests:signal_canary_test",
}


def fuzz_coverage_errors(workflow: str, scheduled: str, config: str) -> list[str]:
    errors = []
    smoke = job_blocks(workflow).get("fuzz-smoke", "")
    commands = run_commands(smoke)
    runs = [command for command in commands if command.startswith("bazel test ")]
    if len(runs) != 1:
        errors.append("smoke must execute one explicit fuzz target set")
    else:
        command = runs[0]
        targets = {word for word in command.split() if word.startswith("//")}
        if targets != SMOKE_FUZZERS:
            errors.append(
                "smoke must execute every fuzzer and the verifying signal canary"
            )
        for flag in (
            "--config=ci",
            "--config=fuzz",
            "--keep_going",
            "--test_output=all",
            "--test_arg=-max_total_time=2",
            "--test_arg=-seed=1",
        ):
            if flag not in command.split():
                errors.append(f"smoke requires {flag}")
    if "build:fuzz --config=san-all" not in config.splitlines():
        errors.append("fuzz mode must select the full sanitizer configuration")
    for option in ("--copt", "--linkopt"):
        if (
            f"build:san-all {option}=-fsanitize=address,undefined,vptr,function,alignment"
            not in config.splitlines()
        ):
            errors.append(
                "fuzz mode must compile and link address and undefined behavior checks"
            )
    campaign = job_blocks(scheduled).get("stateful-fuzz", "")
    targets = set(re.findall(r"^          - ([a-z_]+)$", campaign, re.MULTILINE))
    if targets != STATEFUL_FUZZERS:
        errors.append(
            "scheduled matrix must contain every stateful target exactly once"
        )
    if "  schedule:" not in scheduled or "    - cron:" not in scheduled:
        errors.append("stateful campaigns must be scheduled")
    if "fail-fast: false" not in campaign:
        errors.append("one failure must not cancel the remaining campaigns")
    commands = run_commands(campaign)
    if len(commands) != 1 or not commands[0].startswith("bazel test "):
        errors.append("scheduled matrix must execute its fuzzer")
    else:
        for flag in (
            "--config=ci",
            "--config=fuzz",
            "--test_output=all",
            "--test_timeout=720",
            "--test_arg=-max_total_time=600",
            '"//src/simulation/tests:${FUZZ_TARGET}"',
        ):
            if flag not in commands[0].split():
                errors.append(f"scheduled campaign requires {flag}")
    for name, job in (("smoke", smoke), ("scheduled", campaign)):
        for required in (
            "--test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30",
            "if: always()",
            "actions/upload-artifact@",
            "bazel-testlogs/**/test.log",
            "bazel-testlogs/**/test.outputs/**",
            "retention-days: 30",
        ):
            if required not in job:
                errors.append(
                    f"{name} must retain bounded failure diagnostics: {required}"
                )
    repeated = run_commands(job_blocks(workflow).get("test", ""))
    if not any(
        all(
            flag in command.split()
            for flag in (
                "--runs_per_test=10",
                "--cache_test_results=no",
                "--spawn_strategy=sandboxed",
                "//src/runtime/tests:hermetic_contracts",
            )
        )
        for command in repeated
    ):
        errors.append("real contracts need ten uncached sandboxed repetitions")
    return errors


def analysis_coverage_errors(workflow: str) -> list[str]:
    jobs = job_blocks(workflow)
    errors = []
    ordinary = run_commands(jobs.get("clang-tidy", ""))
    if not any(
        "bazel build --config=ci-debug --remote_download_outputs=all "
        "--build_tag_filters=-fuzz,-manual //..."
        == command
        for command in ordinary
    ):
        errors.append(
            "ordinary analysis must materialize all selected translation units"
        )
    for target in (
        "//tools:compile_commands",
        "//tools:clang_tidy",
        "//tools:clang_tidy_strict",
    ):
        if not any(target in command.split() for command in ordinary):
            errors.append(f"ordinary analysis requires {target}")
    fuzz = run_commands(jobs.get("clang-tidy-fuzz", ""))
    if not any(
        "--fuzz-only" in command.split()
        and "//tools:compile_commands" in command.split()
        for command in fuzz
    ):
        errors.append("fuzz analysis needs its own compilation database")
    if not any(
        command.startswith("bazel build ")
        and all(
            flag in command.split()
            for flag in (
                "--remote_download_outputs=all",
                "--build_tag_filters=fuzz",
                "//...",
            )
        )
        for command in fuzz
    ):
        errors.append("fuzz analysis must materialize all fuzz roots and cached inputs")
    if not any("//tools:clang_tidy" in command.split() for command in fuzz):
        errors.append("fuzz analysis must execute ordinary clang-tidy")
    if any("//tools:clang_tidy_strict" in command.split() for command in fuzz):
        errors.append("strict clang-tidy is reserved for production")
    if any("--config=fuzz" not in command.split() for command in fuzz):
        errors.append("keep fuzz analysis in its own build configuration")
    integrity = run_commands(jobs.get("repository-checks", ""))
    for target in ("//tools:check_determinism",):
        if not any(target in command.split() for command in integrity):
            errors.append(f"repository integrity requires {target}")
    return errors


def job_blocks(workflow: str) -> dict[str, str]:
    """Read the known two-space job layout, without emulating a YAML parser."""
    starts = list(re.finditer(r"^  ([a-z][a-z0-9-]*):\s*$", workflow, re.MULTILINE))
    return {
        match.group(1): workflow[
            match.end() : (
                starts[index + 1].start() if index + 1 < len(starts) else len(workflow)
            )
        ]
        for index, match in enumerate(starts)
    }


def run_commands(job: str) -> list[str]:
    lines = job.splitlines()
    commands = []
    index = 0
    while index < len(lines):
        match = re.match(r"^( +)(?:-\s+)?run:\s*(.*)$", lines[index])
        index += 1
        if match is None:
            continue
        command = match.group(2)
        if command in {">-", ">", "|", "|-"}:
            body = []
            while index < len(lines):
                line = lines[index]
                if line.strip() and len(line) - len(line.lstrip()) <= len(
                    match.group(1)
                ):
                    break
                body.append(line.strip())
                index += 1
            command = " ".join(body).strip()
        commands.append(command)
    return commands


PROFILE_EXPECTATIONS = {
    "ci-debug": ("native", "true", "false", "false"),
    "ci-native": ("native", "false", "false", "false"),
    "ci-sanitizer": ("system", "false", "true", "true"),
    "ci-release": ("native", "false", "true", "false"),
}
PROFILE_FIELDS = ("ALLOCATOR", "INJECTION", "OPTIMIZED", "SANITIZED")
PROFILE_TARGETS = {
    "//bazel/tests:seastar_gtest",
    "//src/runtime/tests:reactor_smoke_test",
    "//src/runtime/tests:runtime_metrics_test",
    "//src/broker:failure_policy_test",
    "//src/broker:crash_recorder_process_test",
}


def validation_profile_errors(workflow: str, config: str) -> list[str]:
    errors = []
    lines = set(config.splitlines())
    for profile, expected in PROFILE_EXPECTATIONS.items():
        for field, value in zip(PROFILE_FIELDS, expected):
            required = f"test:{profile} --test_env=KWAQUE_EXPECT_TEST_{field}={value}"
            if required not in lines:
                errors.append(f"{profile}: missing independent {field} expectation")
    jobs = job_blocks(workflow)
    for name, profile in {
        "test": "ci-debug",
        "native-policy": "ci-native",
        "sanitizer": "ci-sanitizer",
        "build": "ci-release",
        "arm-build": "ci-release",
    }.items():
        commands = run_commands(jobs.get(name, ""))
        selected = [
            command for command in commands
            if command.startswith(f"bazel test --config={profile} ")
        ]
        if not any("//..." in command.split() for command in selected):
            targets = {
                word for command in selected for word in command.split()
                if word.startswith("//")
            }
            if not PROFILE_TARGETS <= targets:
                errors.append(
                    f"{name}: execute runtime, metric and process policy fixtures"
                )
        configs = set(re.findall(r"--config=([a-z-]+)", " ".join(commands)))
        if configs != {profile}:
            errors.append(f"{name}: isolate validation profiles in separate jobs")
    return errors


def coverage_errors(workflow: str) -> list[str]:
    jobs = job_blocks(workflow)
    errors = []
    for name, action, config in (
        ("build", "build", "ci-release"),
        ("arm-build", "build", "ci-release"),
        ("test", "test", "ci-debug"),
        ("sanitizer", "test", "ci-sanitizer"),
    ):
        commands = run_commands(jobs.get(name, ""))
        broad = [
            command
            for command in commands
            if re.search(rf"\bbazel {action}\b", command)
            and f"--config={config}" in command
            and "//..." in command.split()
        ]
        if len(broad) != 1:
            errors.append(f"{name}: requires one broad {config} {action} command")
            continue
        command = broad[0]
        if "--build_tag_filters=-fuzz,-manual" not in command:
            errors.append(
                f"{name}: build selection must exclude fuzz and manual targets"
            )
        if action == "test" and "--test_tag_filters=-fuzz,-manual" not in command:
            errors.append(
                f"{name}: test execution must exclude fuzz and manual targets"
            )
        if "--build_tests_only" in command or "-benchmark" in command:
            errors.append(
                f"{name}: ordinary builds must include benchmark and other translation units"
            )
        configs = set(re.findall(r"--config=([a-z-]+)", " ".join(commands)))
        if configs != {config}:
            errors.append(f"{name}: isolate build configurations in separate jobs")

    goldens = jobs.get("goldens", "")
    if "runs-on: ${{ matrix.runner }}" not in goldens:
        errors.append("goldens: select native runners through the runner matrix")
    for field, expected in (
        ("runner", {"ubuntu-24.04", "ubuntu-24.04-arm"}),
        ("config", {"ci-debug", "ci-release"}),
    ):
        match = re.search(
            rf"^        {field}:\s*\n((?:          - [^\n]+\n)+)", goldens, re.MULTILINE
        )
        values = set(re.findall(r"- (\S+)", match.group(1))) if match else set()
        if values != expected:
            errors.append(
                f"goldens: requires both {field} values for all four native jobs"
            )
    commands = run_commands(goldens)
    expected = f'bazel test --config="${{{{ matrix.config }}}}" --cache_test_results=no {GOLDENS}'
    if commands != [expected]:
        errors.append(
            "goldens: all four jobs must execute the identical explicit uncached suite"
        )

    arm = jobs.get("arm-build", "")
    if "runs-on: ubuntu-24.04-arm" not in arm:
        errors.append("arm-build: use a native aarch64 runner")
    if "//src/simulation/tests:fake_file_replay_test" not in arm:
        errors.append("arm-build: retain the wider release runtime coverage")
    return errors


class CiCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_documentation_selection_preserves_jobs_and_build_order(self) -> None:
        jobs = job_blocks(self.workflow)
        self.assertNotIn("paths-ignore:", self.workflow)
        self.assertNotRegex(self.workflow, r"(?m)^ +paths:")
        gate = jobs["workflow-lint"]
        self.assertIn("fetch-depth: 0", gate)
        self.assertIn("run_checks: ${{ steps.changes.outputs.run_checks }}", gate)
        self.assertIn("python3 tools/ci_changes.py", run_commands(gate))
        self.assertIn(
            "python3 -m unittest tools.ci_changes_test tools.check_ci_coverage_test",
            run_commands(gate),
        )
        for name in (
            "build",
            "test",
            "sanitizer",
            "native-policy",
            "goldens",
            "cpp-format",
            "clang-tidy",
            "clang-tidy-fuzz",
            "repository-checks",
            "fuzz-smoke",
            "arm-build",
        ):
            with self.subTest(job=name):
                self.assertIn(
                    "if: needs.workflow-lint.outputs.run_checks != 'false'", jobs[name]
                )
                if name == "arm-build":
                    self.assertIn("needs: [workflow-lint, build]", jobs[name])
                else:
                    self.assertIn("needs: workflow-lint", jobs[name])
        goldens = jobs["goldens"]
        self.assertNotRegex(goldens, r"(?m)^    if:")
        steps = re.split(r"(?m)^      - ", goldens)[1:]
        self.assertEqual(len(steps), 3)
        for step in steps:
            self.assertIn("if: needs.workflow-lint.outputs.run_checks != 'false'", step)

    def test_analysis_is_independent_and_parallel_without_reducing_scope(self) -> None:
        jobs = job_blocks(self.workflow)
        for name in ("clang-tidy", "clang-tidy-fuzz"):
            job = jobs[name]
            self.assertIn("needs: workflow-lint", job)
            self.assertNotIn("needs: [workflow-lint, build]", job)
            for command in run_commands(job):
                if "//tools:clang_tidy" in command:
                    self.assertIn("-- --jobs=2", command)

    def test_cache_families_have_one_writer_and_analysis_reuses_matching_inputs(
        self,
    ) -> None:
        jobs = job_blocks(self.workflow)
        for name, scope in {
            "build": "ci-release",
            "test": "ci-debug",
            "sanitizer": "ci-sanitizer",
            "native-policy": "ci-native",
            "cpp-format": "tools",
            "repository-checks": "tools",
            "clang-tidy": "ci-debug",
            "clang-tidy-fuzz": "fuzz",
            "fuzz-smoke": "fuzz",
            "arm-build": "ci-release",
        }.items():
            with self.subTest(job=name):
                self.assertIn(f"cache-scope: {scope}", jobs[name])
                if name in {
                    "build",
                    "test",
                    "sanitizer",
                    "native-policy",
                    "repository-checks",
                    "fuzz-smoke",
                    "arm-build",
                }:
                    self.assertIn("cache-write: 'true'", jobs[name])
                else:
                    self.assertNotIn("cache-write:", jobs[name])
        self.assertIn("cache-scope: ${{ matrix.config }}", jobs["goldens"])
        self.assertIn(
            "cache-write: ${{ matrix.runner == 'ubuntu-24.04-arm' && matrix.config == 'ci-debug' }}",
            jobs["goldens"],
        )
        scheduled = FUZZ_WORKFLOW.read_text()
        self.assertIn("cache-scope: fuzz", scheduled)
        self.assertNotIn("cache-write:", scheduled)
        setup = SETUP_BUILD.read_text()
        self.assertIn("disk-cache: false", setup)
        self.assertIn("build --disk_cache=${{ runner.temp }}/kwaque-bazel-disk", setup)
        self.assertEqual(setup.count("path: ${{ runner.temp }}/kwaque-bazel-disk"), 2)
        prefix = (
            "bazel-v1-${{ runner.os }}-${{ runner.arch }}-${{ inputs.cache-scope }}-"
        )
        self.assertEqual(setup.count(f"key: {prefix}${{{{ github.sha }}}}"), 2)
        self.assertEqual(setup.count(f"restore-keys: |\n          {prefix}\n"), 2)
        self.assertIn("inputs.cache-write == 'true'", setup)
        self.assertIn(
            "github.event.pull_request.head.repo.full_name == github.repository", setup
        )
        self.assertIn("if: steps.disk-cache.outputs.write == 'true'", setup)
        self.assertIn("if: steps.disk-cache.outputs.write != 'true'", setup)

    def test_analysis_covers_ordinary_and_fuzz_sources(self) -> None:
        self.assertEqual(analysis_coverage_errors(self.workflow), [])
        for token in (
            "--fuzz-only",
            "--build_tag_filters=fuzz",
            "//tools:clang_tidy_strict",
        ):
            with self.subTest(token=token):
                self.assertTrue(
                    analysis_coverage_errors(self.workflow.replace(token, ""))
                )
        job = job_blocks(self.workflow)["clang-tidy-fuzz"]
        changed = self.workflow.replace(
            job, job.replace("//tools:clang_tidy", "//tools:clang_tidy_strict")
        )
        self.assertTrue(analysis_coverage_errors(changed))

    def test_analysis_builds_materialize_cached_intermediate_inputs(self) -> None:
        flag = "--remote_download_outputs=all"
        jobs = job_blocks(self.workflow)
        for name in ("clang-tidy", "clang-tidy-fuzz"):
            job = jobs[name]
            self.assertIn(flag, job)
            for replacement in (
                "",
                "--remote_download_outputs=toplevel",
                "--remote_download_outputs=minimal",
            ):
                with self.subTest(job=name, replacement=replacement):
                    changed = self.workflow.replace(job, job.replace(flag, replacement))
                    self.assertTrue(analysis_coverage_errors(changed))
            with self.subTest(job=name, flag_only_on_database_generation=True):
                misplaced = job.replace(flag, "").replace(
                    "//tools:compile_commands", f"{flag} //tools:compile_commands"
                )
                self.assertTrue(
                    analysis_coverage_errors(self.workflow.replace(job, misplaced))
                )

    def test_fuzz_and_hermetic_coverage(self) -> None:
        self.assertEqual(
            fuzz_coverage_errors(
                self.workflow, FUZZ_WORKFLOW.read_text(), BAZEL_CONFIG.read_text()
            ),
            [],
        )

    def test_each_omitted_smoke_target_or_missing_cap_fails(self) -> None:
        for value in SMOKE_FUZZERS | {
            "--test_arg=-max_total_time=2",
            "--test_output=all",
            "--runs_per_test=10",
        }:
            with self.subTest(value=value):
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow.replace(value, ""),
                        FUZZ_WORKFLOW.read_text(),
                        BAZEL_CONFIG.read_text(),
                    )
                )

    def test_each_omitted_campaign_or_retention_setting_fails(self) -> None:
        scheduled = FUZZ_WORKFLOW.read_text()
        for value in STATEFUL_FUZZERS | {
            "--test_arg=-max_total_time=600",
            "if: always()",
            "--test_timeout=720",
            "fail-fast: false",
            "  schedule:",
        }:
            with self.subTest(value=value):
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow,
                        scheduled.replace(value, ""),
                        BAZEL_CONFIG.read_text(),
                    )
                )

    def test_dropping_ubsan_or_replacing_tests_with_builds_fails(self) -> None:
        config = BAZEL_CONFIG.read_text()
        self.assertTrue(
            fuzz_coverage_errors(
                self.workflow,
                FUZZ_WORKFLOW.read_text(),
                config.replace(
                    "build:fuzz --config=san-all", "build:fuzz --config=dev-base"
                ),
            )
        )
        self.assertTrue(
            fuzz_coverage_errors(
                self.workflow,
                FUZZ_WORKFLOW.read_text(),
                config.replace("address,undefined,vptr,function,alignment", "address"),
            )
        )
        self.assertTrue(
            fuzz_coverage_errors(
                self.workflow.replace(
                    "bazel test --config=ci --config=fuzz",
                    "bazel build --config=ci --config=fuzz",
                ),
                FUZZ_WORKFLOW.read_text(),
                config,
            )
        )

    def test_validation_profiles_are_asserted_and_executed(self) -> None:
        config = BAZEL_CONFIG.read_text()
        self.assertEqual(validation_profile_errors(self.workflow, config), [])
        for profile, expected in PROFILE_EXPECTATIONS.items():
            for field, value in zip(PROFILE_FIELDS, expected):
                with self.subTest(profile=profile, field=field):
                    flag = f"test:{profile} --test_env=KWAQUE_EXPECT_TEST_{field}={value}"
                    self.assertTrue(
                        validation_profile_errors(self.workflow, config.replace(flag, ""))
                    )
        for job_name in ("build", "arm-build", "native-policy"):
            job = job_blocks(self.workflow)[job_name]
            for target in PROFILE_TARGETS:
                with self.subTest(job=job_name, target=target):
                    changed = self.workflow.replace(job, job.replace(target, ""))
                    self.assertTrue(validation_profile_errors(changed, config))
        native = job_blocks(self.workflow)["native-policy"]
        changed = self.workflow.replace(
            native, native.replace("bazel test", "bazel build")
        )
        self.assertTrue(validation_profile_errors(changed, config))

    def test_current_workflow_covers_all_required_jobs(self) -> None:
        self.assertEqual(coverage_errors(self.workflow), [])

    def test_narrow_build_and_test_commands_fail(self) -> None:
        for name in ("build", "arm-build", "test", "sanitizer"):
            job = job_blocks(self.workflow)[name]
            with self.subTest(job=name):
                changed = self.workflow.replace(job, job.replace("//...", "//:kwaque"))
                self.assertTrue(coverage_errors(changed))

    def test_missing_filters_and_benchmark_exclusion_fail(self) -> None:
        for flag in (
            "--build_tag_filters=-fuzz,-manual",
            "--test_tag_filters=-fuzz,-manual",
        ):
            with self.subTest(flag=flag):
                self.assertTrue(coverage_errors(self.workflow.replace(flag, "")))
        self.assertTrue(
            coverage_errors(
                self.workflow.replace(
                    "--build_tag_filters=-fuzz,-manual",
                    "--build_tag_filters=-fuzz,-manual,-benchmark",
                )
            )
        )

    def test_each_missing_golden_dimension_or_changed_suite_fails(self) -> None:
        for value in ("ubuntu-24.04", "ubuntu-24.04-arm", "ci-debug", "ci-release"):
            with self.subTest(value=value):
                self.assertTrue(
                    coverage_errors(self.workflow.replace(f"          - {value}\n", ""))
                )
        self.assertTrue(
            coverage_errors(
                self.workflow.replace(GOLDENS, "//src/simulation/tests:philox_kat_test")
            )
        )
        self.assertTrue(
            coverage_errors(self.workflow.replace("--cache_test_results=no", ""))
        )

    def test_actual_test_execution_and_configuration_isolation_are_required(
        self,
    ) -> None:
        job = job_blocks(self.workflow)["test"]
        self.assertTrue(
            coverage_errors(
                self.workflow.replace(job, job.replace("bazel test", "bazel build"))
            )
        )
        self.assertTrue(
            coverage_errors(
                self.workflow.replace(
                    job,
                    job + "\n      - run: bazel build --config=ci-release //:kwaque\n",
                )
            )
        )


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
