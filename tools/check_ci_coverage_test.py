"""Guard the CI coverage contract; actionlint remains the YAML/workflow parser."""

from __future__ import annotations

import re
import runpy
import sys
import tempfile
import unittest
from pathlib import Path

WORKFLOW_ARGUMENTS = len(sys.argv) == 5 and sys.argv[1].endswith((".yml", ".yaml"))
WORKFLOW = (
    Path(sys.argv[1])
    if WORKFLOW_ARGUMENTS
    else Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
)
GOLDENS = "//src/simulation/tests:determinism_goldens"
GOLDEN_SUITE = (
    GOLDENS,
    "//src/storage/tests:format_tests",
    "//src/model/tests:checkpoint_tests",
    "//src/model/tests:format_fixture_test",
    "//src/model/tests:batch_builder_test",
    "//src/protocol/tests:golden_tests",
    "//src/compression/tests:format_fixture_test",
    "//tools:verify_format_fixtures_test",
)
SHIPPED_FLAGS = (
    "--config=ci-release",
    "--//bazel:reactor_backend=linux-aio",
    "--build_tag_filters=-fuzz,-manual",
    "--test_tag_filters=package,smoke,fuzz_replay",
    "//...",
)
GATE_FAILURE = (
    "contains(needs.*.result, 'failure') || contains(needs.*.result, 'cancelled')"
)
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
RETAIN_LOGS = SETUP_BUILD.parents[1] / "retain-logs/action.yml"
REACTOR_WORKFLOW = WORKFLOW.with_name("reactor-backends.yml")
NIGHTLY_WORKFLOW = WORKFLOW.with_name("nightly.yml")
RELEASE_WORKFLOW = WORKFLOW.with_name("release.yml")
CAMPAIGN_QUERY = 'attr(tags, "\\bfuzz-campaign\\b", tests(//...))'
CAMPAIGN_MATRIX = "target: ${{ fromJSON(needs.campaign-targets.outputs.targets) }}"


def fuzz_coverage_errors(workflow: str, scheduled: str, config: str) -> list[str]:
    errors = []
    smoke = job_blocks(workflow).get("fuzz-smoke", "")
    commands = run_commands(smoke)
    runs = [command for command in commands if command.startswith("bazel test ")]
    if len(runs) != 1:
        errors.append("smoke must execute one fuzz target selection")
    else:
        words = runs[0].split()
        # Tags select the targets, so a new fuzz test cannot be left out.
        if [word for word in words if word.startswith("//")] != ["//..."]:
            errors.append("smoke must select every fuzz test by tag, not by list")
        for flag in (
            "--config=ci",
            "--config=fuzz",
            "--keep_going",
            "--test_output=all",
            "--test_arg=-max_total_time=2",
            "--test_arg=-seed=1",
            "--build_tag_filters=fuzz",
            "--test_tag_filters=fuzz,-manual",
        ):
            if flag not in words:
                errors.append(f"smoke requires {flag}")
    if "build:fuzz --config=san-all" not in config.splitlines():
        errors.append("fuzz mode must select the full sanitizer configuration")
    if "test:fuzz --cache_test_results=no" not in config.splitlines():
        errors.append("fuzz campaigns must execute without cached test results")
    for option in ("--copt", "--linkopt"):
        if (
            f"build:san-all {option}=-fsanitize=address,undefined,vptr,function,alignment"
            not in config.splitlines()
        ):
            errors.append(
                "fuzz mode must compile and link address and undefined behavior checks"
            )
    jobs = job_blocks(scheduled)
    campaign = jobs.get("fuzz-campaign", "")
    selection = " ".join(run_commands(jobs.get("campaign-targets", "")))
    if "bazel query" not in selection or CAMPAIGN_QUERY not in selection:
        errors.append("scheduled targets must come from the fuzz-campaign tag")
    if "needs: campaign-targets" not in campaign or CAMPAIGN_MATRIX not in campaign:
        errors.append("scheduled matrix must use the queried campaign targets")
    if re.search(r"^ +target: //", campaign, re.MULTILINE):
        errors.append("scheduled matrix must not list targets by hand")
    if "  schedule:" not in scheduled or "    - cron:" not in scheduled:
        errors.append("fuzz campaigns must be scheduled")
    if "fail-fast: false" not in campaign:
        errors.append("one failure must not cancel the remaining campaigns")
    if not re.search(r"^      max-parallel: [1-4]$", campaign, re.MULTILINE):
        errors.append("scheduled campaigns must run at most four jobs together")
    if "          FUZZ_TARGET: ${{ matrix.target }}" not in campaign.splitlines():
        errors.append("scheduled campaigns must pass the selected full target label")
    commands = [
        command for command in run_commands(campaign) if command.startswith("bazel ")
    ]
    if len(commands) != 1 or not commands[0].startswith("bazel test "):
        errors.append("scheduled matrix must execute its fuzzer")
    else:
        for flag in (
            "--config=ci",
            "--config=fuzz",
            "--test_output=all",
            "--test_timeout=720",
            "--test_arg=-max_total_time=600",
            "--test_arg=-timeout=15",
            '--test_env=KWAQUE_FUZZ_CORPUS_DIR="${CORPUS}"',
            '--sandbox_writable_path="${CORPUS}"',
            '"${FUZZ_TARGET}"',
        ):
            if flag not in commands[0].split():
                errors.append(f"scheduled campaign requires {flag}")
    restored = campaign.find("uses: actions/cache/restore@")
    saved = campaign.find("uses: actions/cache/save@")
    run = campaign.find("bazel test ")
    if not 0 <= restored < run < saved:
        errors.append("scheduled campaigns must restore and then keep their corpus")
    for name, job in (("smoke", smoke), ("scheduled", campaign)):
        for required in (
            "--test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30",
            "if: failure()",
            "uses: ./.github/actions/retain-logs",
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
    analysis_inputs = (
        "--remote_download_outputs=all",
        "--aspects=//bazel:analysis_inputs.bzl%analysis_inputs",
        "--output_groups=clang_tidy_inputs",
        "//...",
    )
    if not any(
        command.startswith("bazel build ")
        and all(
            flag in command.split()
            for flag in (
                *analysis_inputs,
                "--config=ci-debug",
                "--build_tag_filters=-fuzz,-manual",
            )
        )
        for command in ordinary
    ):
        errors.append(
            "ordinary analysis must materialize all compilation prerequisites"
        )
    for target in (
        "//tools:compile_commands",
        "//tools:clang_tidy",
    ):
        if not any(target in command.split() for command in ordinary):
            errors.append(f"ordinary analysis requires {target}")
    if not any(
        "//tools:clang_tidy" in command.split()
        and "--production-config=.clang-tidy-strict" in command.split()
        for command in ordinary
    ):
        errors.append(
            "ordinary analysis must cover the disjoint strict production commands"
        )
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
            for flag in (*analysis_inputs, "--build_tag_filters=fuzz")
        )
        for command in fuzz
    ):
        errors.append("fuzz analysis must materialize all fuzz roots and cached inputs")
    if not any("//tools:clang_tidy" in command.split() for command in fuzz):
        errors.append("fuzz analysis must execute ordinary clang-tidy")
    if any(
        "//tools:clang_tidy_strict" in command.split()
        or "--production-config=.clang-tidy-strict" in command.split()
        for command in fuzz
    ):
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


def jobs_section(workflow: str) -> dict[str, str]:
    """Job blocks only: keys under `on:` share the two-space layout."""
    return job_blocks(workflow.split("\njobs:\n", 1)[1])


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


def untagged_broad(command: str) -> bool:
    """A //... test command that no positive tag filter narrows."""
    words = command.split()
    filters = [
        word.split("=", 1)[1]
        for word in words
        if word.startswith("--test_tag_filters=")
    ]
    return "//..." in words and all(
        not tag or tag.startswith("-") for value in filters for tag in value.split(",")
    )


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
            command
            for command in commands
            if command.startswith(f"bazel test --config={profile} ")
        ]
        if not any(untagged_broad(command) for command in selected):
            targets = {
                word
                for command in selected
                for word in command.split()
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

    # The x86-64 debug goldens run inside the test job's //... suite.
    for name, config, runner in (
        ("build", "ci-release", "ubuntu-24.04"),
        ("arm-build", "ci-release", "ubuntu-24.04-arm"),
        ("goldens", "ci-debug", "ubuntu-24.04-arm"),
    ):
        job = jobs.get(name, "")
        if f"    runs-on: {runner}\n" not in job:
            errors.append(f"{name}: run the goldens on {runner}")
        if not any(
            command.startswith(f"bazel test --config={config} ")
            and "--cache_test_results=no" in command.split()
            and set(GOLDEN_SUITE) <= set(command.split())
            for command in run_commands(job)
        ):
            errors.append(f"{name}: execute the complete golden suite uncached")
    arm = jobs.get("arm-build", "")
    if "runs-on: ubuntu-24.04-arm" not in arm:
        errors.append("arm-build: use a native aarch64 runner")
    if "//src/simulation/tests:fake_file_replay_test" not in arm:
        errors.append("arm-build: retain the wider release runtime coverage")
    for target in (
        "//src/compression/tests:format_tests",
        "//src/model/tests:batch_compression_test",
        "//src/model/tests:batch_builder_test",
        "//src/model/tests:format_fixture_test",
        "//src/model/tests:checkpoint_tests",
        "//src/protocol/tests:golden_tests",
        "//src/storage/tests:format_tests",
    ):
        if not any(
            target in command.split()
            for command in run_commands(arm)
            if command.startswith("bazel test ")
        ):
            errors.append(
                f"arm-build: execute format allocation and integration coverage: {target}"
            )
    return errors


def shipped_artifact_errors(workflow: str) -> list[str]:
    jobs = job_blocks(workflow)
    errors = []
    for name in ("build", "arm-build"):
        commands = run_commands(jobs.get(name, ""))
        if "sudo sysctl -w fs.aio-max-nr=1048576" not in commands:
            errors.append(f"{name}: allow linux-aio reactors")
        if not any(
            command.startswith("bazel test ")
            and all(flag in command.split() for flag in SHIPPED_FLAGS)
            for command in commands
        ):
            errors.append(
                f"{name}: test the release package, broker processes and replays"
            )
    return errors


def gate_errors(workflow: str) -> list[str]:
    jobs = jobs_section(workflow)
    gate = jobs.get("ci-ok", "")
    errors = []
    if "    if: always()\n" not in gate:
        errors.append("ci-ok must run when other jobs fail or are skipped")
    listed = re.search(r"^    needs:\n((?:      - [a-z0-9-]+\n)+)", gate, re.MULTILINE)
    needed = set(re.findall(r"- ([a-z0-9-]+)", listed.group(1))) if listed else set()
    if needed != set(jobs) - {"ci-ok"}:
        errors.append("ci-ok must depend on every other job")
    failing = re.search(r"      - if: \$\{\{ (.+) \}\}\n        run: exit 1\n", gate)
    if failing is None or failing.group(1) != GATE_FAILURE:
        errors.append("ci-ok must fail when any job fails or is cancelled")
    return errors


class CiCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")
        cls.retention = runpy.run_path(str(RETAIN_LOGS.with_name("collect.py")))

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
        # No job waits for another job's build; all start from the change check.
        for name in set(jobs_section(self.workflow)) - {"workflow-lint", "ci-ok"}:
            with self.subTest(job=name):
                self.assertIn(
                    "    if: needs.workflow-lint.outputs.run_checks != 'false'\n",
                    jobs[name],
                )
                self.assertIn("    needs: workflow-lint\n", jobs[name])

    def test_only_failed_test_steps_can_trigger_retention(self) -> None:
        for workflow in (
            self.workflow,
            FUZZ_WORKFLOW.read_text(),
            REACTOR_WORKFLOW.read_text(),
            NIGHTLY_WORKFLOW.read_text(),
        ):
            for name, job in job_blocks(workflow).items():
                with self.subTest(job=name):
                    steps = re.split(r"(?m)^      - ", job)[1:]
                    tests = [
                        step
                        for step in steps
                        if any(
                            command.startswith(("bazel test ", "bazel coverage "))
                            for command in run_commands(step)
                        )
                    ]
                    retention = [
                        step
                        for step in steps
                        if "uses: ./.github/actions/retain-logs" in step
                    ]
                    if not tests:
                        self.assertEqual(retention, [])
                        continue
                    self.assertEqual(len(retention), 1)
                    self.assertEqual(retention[0], steps[-1])
                    ids = []
                    for step in tests:
                        match = re.search(r"(?m)^        id: (\w+)$", step)
                        self.assertIsNotNone(match)
                        ids.append(match.group(1))
                    self.assertEqual(len(ids), len(set(ids)))
                    failed = " || ".join(
                        f"steps.{step}.outcome == 'failure'" for step in ids
                    )
                    if len(ids) > 1:
                        failed = f"({failed})"
                    self.assertIn(f"if: failure() && {failed}", retention[0])
                    self.assertIn("${{ github.run_attempt }}", retention[0])
                    if name == "fuzz-campaign":
                        self.assertIn("${{ strategy.job-index }}", retention[0])
                    elif name != "fuzz-smoke":
                        self.assertIn("${{ github.job }}", retention[0])

    def test_retention_upload_requires_failed_reports(self) -> None:
        action = RETAIN_LOGS.read_text()
        for required in (
            'python3 "${{ github.action_path }}/collect.py"',
            "path: ${{ steps.collect.outputs.paths }}",
            "retention-days: 30",
            "if-no-files-found: warn",
            "actions/upload-artifact@",
        ):
            with self.subTest(setting=required):
                self.assertIn(required, action)
        upload = action.split("- name: Upload diagnostics", 1)[1]
        self.assertIn(
            "if: always() && steps.collect.outputs.has_failures == 'true'", upload
        )

    def test_retention_skips_successful_and_unexecuted_tests(self) -> None:
        collect = self.retention["failure_paths"]
        with tempfile.TemporaryDirectory() as directory:
            logs, binaries = Path(directory) / "logs", Path(directory) / "bin"
            self.assertEqual(collect(logs, binaries), [])
            for name, xml in (
                ("passed", '<testsuite tests="1" failures="0" errors="0"/>'),
                ("skipped", "<testsuite><testcase><skipped/></testcase></testsuite>"),
                ("passed/test.outputs/example", '<testsuite failures="1"/>'),
            ):
                report = logs / name / "test.xml"
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(xml)
            build_log = logs / "not_executed" / "test.log"
            build_log.parent.mkdir()
            build_log.write_text("build failed before test execution")
            self.assertEqual(collect(logs, binaries), [])

    def test_retention_collects_failed_nested_runs_and_their_binary(self) -> None:
        collect = self.retention["failure_paths"]
        with tempfile.TemporaryDirectory() as directory:
            logs, binaries = Path(directory) / "logs", Path(directory) / "bin"
            failed = logs / "pkg/test/run_2_of_3"
            failed.mkdir(parents=True)
            (failed / "test.xml").write_text(
                '<testsuite><testcase><error message="aborted"/></testcase></testsuite>'
            )
            passed = logs / "pkg/test/run_1_of_3"
            passed.mkdir(parents=True)
            (passed / "test.xml").write_text('<testsuite errors="0"/>')
            binary = binaries / "pkg/test"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"test executable")
            expected = [
                str(failed / pattern)
                for pattern in (
                    "test.log",
                    "test.xml",
                    "test.outputs/**",
                    "test_attempts/**",
                )
            ] + [str(binary)]
            self.assertEqual(collect(logs, binaries), sorted(expected))

    def test_retention_recognizes_counts_and_malformed_reports(self) -> None:
        failed_report = self.retention["failed_report"]
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "test.xml"
            for xml in (
                '<testsuite failures="1"/>',
                '<testsuite errors="1"/>',
                (
                    '<testsuites xmlns="urn:junit"><testsuite><testcase><failure/>'
                    "</testcase></testsuite></testsuites>"
                ),
                "<testsuite",
            ):
                with self.subTest(report=xml):
                    report.write_text(xml)
                    self.assertTrue(failed_report(report))

    def test_analysis_is_independent_and_parallel_without_reducing_scope(self) -> None:
        jobs = job_blocks(self.workflow)
        for name in ("clang-tidy", "clang-tidy-fuzz"):
            job = jobs[name]
            self.assertIn("needs: workflow-lint", job)
            self.assertNotIn("needs: [workflow-lint, build]", job)
            for command in run_commands(job):
                if "//tools:clang_tidy" in command:
                    self.assertIn("-- --jobs=2", command)

    def test_caches_hold_downloads_written_only_from_main(self) -> None:
        setup = SETUP_BUILD.read_text()
        self.assertIn("disk-cache: false", setup)
        self.assertIn("repository-cache: true", setup)
        self.assertIn("bazelisk-cache: true", setup)
        self.assertIn("cache-save: ${{ github.ref == 'refs/heads/main' }}", setup)
        self.assertNotIn("--disk_cache", setup)
        self.assertNotIn("inputs:", setup)
        for workflow in (
            self.workflow,
            FUZZ_WORKFLOW.read_text(),
            REACTOR_WORKFLOW.read_text(),
            NIGHTLY_WORKFLOW.read_text(),
        ):
            for value in ("cache-scope:", "cache-write:", "--disk_cache"):
                self.assertNotIn(value, workflow)
        # The only other cache is each scheduled fuzz target's corpus.
        for workflow in (
            self.workflow,
            REACTOR_WORKFLOW.read_text(),
            NIGHTLY_WORKFLOW.read_text(),
        ):
            self.assertNotIn("actions/cache", workflow)

    def test_one_result_job_gates_every_job(self) -> None:
        self.assertEqual(gate_errors(self.workflow), [])
        gate = job_blocks(self.workflow)["ci-ok"]
        for changed in (
            gate.replace("      - sanitizer\n", ""),
            gate.replace("    if: always()\n", ""),
            gate.replace(" || contains(needs.*.result, 'cancelled')", ""),
        ):
            with self.subTest(changed=changed):
                self.assertNotEqual(changed, gate)
                self.assertTrue(gate_errors(self.workflow.replace(gate, changed)))

    def test_runs_on_main_are_never_cancelled(self) -> None:
        self.assertIn(
            "  cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}\n",
            self.workflow,
        )

    def test_release_jobs_test_the_shipped_artifact(self) -> None:
        self.assertEqual(shipped_artifact_errors(self.workflow), [])
        for value in (
            "package,smoke,fuzz_replay",
            "--//bazel:reactor_backend=linux-aio",
        ):
            with self.subTest(value=value):
                job = job_blocks(self.workflow)["arm-build"]
                changed = job.replace(value, "")
                self.assertTrue(
                    shipped_artifact_errors(self.workflow.replace(job, changed))
                )

    def test_python_workflow_module_and_license_checks_run(self) -> None:
        jobs = job_blocks(self.workflow)
        python = jobs["python-lint"]
        self.assertIn("args: format --check --diff", python)
        self.assertIn("args: check --output-format=github", python)
        self.assertEqual(python.count("version: 0.16.9"), 2)
        self.assertEqual(python.count("checksum: "), 2)
        lint = jobs["workflow-lint"]
        self.assertIn("uses: zizmorcore/zizmor-action@", lint)
        self.assertRegex(lint, r"(?m)^          version: \d+\.\d+\.\d+$")
        integrity = run_commands(jobs["repository-checks"])
        joined = " ".join(integrity)
        for required in (
            "bazel mod tidy --lockfile_mode=update",
            "bazel mod deps --lockfile_mode=update",
            "git diff --exit-code -- MODULE.bazel MODULE.bazel.lock",
            "//tools:check_package_licenses",
        ):
            with self.subTest(required=required):
                self.assertIn(required, joined)

    def test_release_attests_and_drafts_only_tested_tagged_packages(self) -> None:
        release = RELEASE_WORKFLOW.read_text()
        jobs = jobs_section(release)
        self.assertIn("  push:\n    tags:\n", release)
        self.assertIn("permissions:\n  contents: read\n", release)
        package = jobs["package"]
        for runner in ("ubuntu-24.04", "ubuntu-24.04-arm"):
            self.assertIn(f"          - {runner}\n", package)
        commands = run_commands(package)
        tests = [command for command in commands if command.startswith("bazel test ")]
        self.assertEqual(len(tests), 1)
        for flag in (
            "--config=ci-release",
            "--test_tag_filters=package",
            "//bazel/packaging:all",
        ):
            self.assertIn(flag, tests[0].split())
        gate = " ".join(commands)
        for required in (
            'tag != "v" + fields["version"]',
            '"-dev" in fields["version"]',
            'fields["dirty"] != "false"',
        ):
            self.assertIn(required, gate)
        # Attestation follows the tests and the version gate.
        self.assertLess(
            package.index("bazel test "), package.index("attest-build-provenance")
        )
        self.assertLess(
            package.index("--version"), package.index("attest-build-provenance")
        )
        self.assertIn("      id-token: write\n      attestations: write\n", package)
        publish = jobs["publish"]
        self.assertIn("    needs: package\n", publish)
        self.assertIn("    permissions:\n      contents: write\n", publish)
        self.assertIn("--draft", " ".join(run_commands(publish)))

    def test_nightly_compares_two_independent_release_packages(self) -> None:
        nightly = NIGHTLY_WORKFLOW.read_text()
        jobs = job_blocks(nightly)
        output_bases = set()
        for name in ("reproducible-package-a", "reproducible-package-b"):
            builds = [
                command
                for command in run_commands(jobs[name])
                if "build --config=ci-release" in command
            ]
            self.assertEqual(len(builds), 1, name)
            self.assertIn("//bazel/packaging:kwaque_tar", builds[0].split())
            output_bases.add(re.search(r"--output_base=(\S+)", builds[0]).group(1))
        self.assertEqual(len(output_bases), 2)
        self.assertIn("path: relocated/kwaque", jobs["reproducible-package-b"])
        compare = jobs["reproducible-package"]
        self.assertIn(
            "needs: [reproducible-package-a, reproducible-package-b]", compare
        )
        self.assertIn('cmp -s "${first[0]}" "${second[0]}"', compare)

    def test_analysis_covers_ordinary_and_fuzz_sources(self) -> None:
        self.assertEqual(analysis_coverage_errors(self.workflow), [])
        for token in (
            "--fuzz-only",
            "--build_tag_filters=fuzz",
            "--production-config=.clang-tidy-strict",
            "--aspects=//bazel:analysis_inputs.bzl%analysis_inputs",
            "--output_groups=clang_tidy_inputs",
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

    def test_each_missing_smoke_selection_or_cap_fails(self) -> None:
        for value in (
            "--test_arg=-max_total_time=2",
            "--test_output=all",
            "--build_tag_filters=fuzz",
            "--test_tag_filters=fuzz,-manual",
            "--runs_per_test=10",
        ):
            with self.subTest(value=value):
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow.replace(value, ""),
                        FUZZ_WORKFLOW.read_text(),
                        BAZEL_CONFIG.read_text(),
                    )
                )
        listed = self.workflow.replace(
            "--test_tag_filters=fuzz,-manual\n          //...",
            "--test_tag_filters=fuzz,-manual\n          //src/codec/tests:codec_fuzz",
        )
        self.assertNotEqual(listed, self.workflow)
        self.assertTrue(
            fuzz_coverage_errors(
                listed, FUZZ_WORKFLOW.read_text(), BAZEL_CONFIG.read_text()
            )
        )

    def test_each_omitted_campaign_or_retention_setting_fails(self) -> None:
        scheduled = FUZZ_WORKFLOW.read_text()
        for value in (
            "--test_arg=-max_total_time=600",
            "--test_arg=-timeout=15",
            "if: failure()",
            "--test_timeout=720",
            "fail-fast: false",
            "max-parallel: 4",
            "FUZZ_TARGET: ${{ matrix.target }}",
            '"${FUZZ_TARGET}"',
            "  schedule:",
            "needs: campaign-targets",
            "fromJSON(needs.campaign-targets.outputs.targets)",
            "fuzz-campaign\\b",
            "uses: actions/cache/restore@",
            "uses: actions/cache/save@",
            '--test_env=KWAQUE_FUZZ_CORPUS_DIR="${CORPUS}"',
        ):
            with self.subTest(value=value):
                self.assertIn(value, scheduled)
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow,
                        scheduled.replace(value, ""),
                        BAZEL_CONFIG.read_text(),
                    )
                )

    def test_campaign_listing_or_wrong_target_binding_fail(self) -> None:
        scheduled = FUZZ_WORKFLOW.read_text()
        for altered in (
            scheduled.replace(
                "        target: ${{ fromJSON(needs.campaign-targets.outputs.targets) }}",
                "        target:\n          - //src/codec/tests:codec_fuzz",
            ),
            scheduled.replace('"${FUZZ_TARGET}"', "//src/codec/tests:codec_fuzz"),
            scheduled.replace("bazel test", "bazel build"),
            scheduled.replace("max-parallel: 4", "max-parallel: 12"),
            scheduled.replace("fuzz-campaign\\b", "fuzz\\b"),
        ):
            with self.subTest(workflow=altered):
                self.assertNotEqual(altered, scheduled)
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow, altered, BAZEL_CONFIG.read_text()
                    )
                )

    def test_nightly_repeats_flaky_candidates_and_measures_coverage(self) -> None:
        jobs = job_blocks(NIGHTLY_WORKFLOW.read_text())
        self.assertIn("  schedule:", NIGHTLY_WORKFLOW.read_text())
        stress = run_commands(jobs["stress"])
        self.assertIn("sudo sysctl -w fs.aio-max-nr=1048576", stress)
        repeated = [command for command in stress if command.startswith("bazel test ")]
        self.assertEqual(len(repeated), 1)
        for flag in (
            "--config=ci-debug",
            "--//bazel:reactor_backend=linux-aio",
            "--runs_per_test=20",
            "--cache_test_results=no",
            "--test_env=GTEST_SHUFFLE=1",
            "--build_tag_filters=smoke,stress",
            "--test_tag_filters=smoke,stress,-manual",
            "//...",
        ):
            with self.subTest(flag=flag):
                self.assertIn(flag, repeated[0].split())
        coverage = [
            command
            for command in run_commands(jobs["coverage"])
            if command.startswith("bazel coverage ")
        ]
        self.assertEqual(len(coverage), 1)
        self.assertIn("//...", coverage[0].split())
        self.assertIn(
            "path: bazel-out/_coverage/_coverage_report.dat", jobs["coverage"]
        )

    def test_fuzz_cache_cannot_skip_a_campaign(self) -> None:
        config = BAZEL_CONFIG.read_text()
        for replacement in ("", "test:fuzz --cache_test_results=yes"):
            with self.subTest(replacement=replacement):
                self.assertTrue(
                    fuzz_coverage_errors(
                        self.workflow,
                        FUZZ_WORKFLOW.read_text(),
                        config.replace(
                            "test:fuzz --cache_test_results=no", replacement
                        ),
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
                    flag = (
                        f"test:{profile} --test_env=KWAQUE_EXPECT_TEST_{field}={value}"
                    )
                    self.assertTrue(
                        validation_profile_errors(
                            self.workflow, config.replace(flag, "")
                        )
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

    def test_production_and_io_uring_reactor_backends_are_exercised(self) -> None:
        commands = run_commands(job_blocks(self.workflow)["test"])
        self.assertIn("sudo sysctl -w fs.aio-max-nr=1048576", commands)
        tests = [command for command in commands if command.startswith("bazel test ")]
        self.assertEqual(len(tests), 2)
        for command in tests:
            self.assertIn("--//bazel:reactor_backend=linux-aio", command.split())
        scheduled = REACTOR_WORKFLOW.read_text()
        self.assertIn("  schedule:", scheduled)
        tests = [
            command
            for command in run_commands(job_blocks(scheduled).get("io-uring", ""))
            if command.startswith("bazel test ")
        ]
        self.assertEqual(len(tests), 1)
        for flag in (
            "--config=ci-debug",
            "--//bazel:reactor_backend=io_uring",
            "--build_tag_filters=-fuzz,-manual",
            "--test_tag_filters=-fuzz,-manual",
            "//...",
        ):
            with self.subTest(flag=flag):
                self.assertIn(flag, tests[0].split())

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
        jobs = job_blocks(self.workflow)
        goldens = jobs["goldens"]
        self.assertTrue(
            coverage_errors(
                self.workflow.replace(
                    goldens, goldens.replace("ubuntu-24.04-arm", "ubuntu-24.04")
                )
            )
        )
        for name in ("build", "arm-build", "goldens"):
            job = jobs[name]
            with self.subTest(job=name):
                self.assertTrue(
                    coverage_errors(
                        self.workflow.replace(
                            job, job.replace("--cache_test_results=no", "")
                        )
                    )
                )
                for target in GOLDEN_SUITE:
                    self.assertTrue(
                        coverage_errors(
                            self.workflow.replace(job, job.replace(target + "\n", "\n"))
                        ),
                        target,
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
