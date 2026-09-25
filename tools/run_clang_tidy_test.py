from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

from tools import run_clang_tidy as driver
from tools.run_clang_tidy import production_sources_from_query, select_files


def rule(name: str, source: str, testonly: bool = False) -> str:
    return (
        f'<rule class="cc_library" name="//src/model:{name}">'
        f'<boolean name="testonly" value="{str(testonly).lower()}"/>'
        f'<list name="srcs"><label value="{source}"/></list></rule>'
    )


def write_python_executable(
    path: Path, source: str, interpreter: str = sys.executable
) -> None:
    script = path.with_name(path.name + ".py")
    script.write_text(source)
    # Bazel's interpreter path can exceed the kernel's shebang length limit.
    path.write_text(
        "#!/bin/sh\n"
        f'exec {shlex.quote(interpreter)} {shlex.quote(str(script))} "$@"\n'
    )
    path.chmod(0o700)


class ClangTidySelectionTest(unittest.TestCase):
    def test_parallel_selection_is_exact_and_preserves_database_variants(self) -> None:
        root = Path("/workspace with spaces")
        selected = "src/a[1].cc"
        entries = [
            {"directory": str(root), "file": selected, "arguments": ["clang", "-DONE"]},
            {"directory": str(root), "file": selected, "arguments": ["clang", "-DTWO"]},
            {"directory": str(root), "file": "src/a1.cc", "arguments": ["clang"]},
        ]
        original = json.dumps(entries)
        command = driver.runner_command(
            Path("/runner"),
            Path("/clang-tidy"),
            root / ".clang-tidy-strict",
            root / ".cache/clang-tidy-production",
            entries,
            [selected],
            4,
            True,
        )
        self.assertEqual(command[:3], [sys.executable, "-u", "/runner"])
        self.assertIn("-j=1", command)
        self.assertIn("-enable-check-profile", command)
        self.assertIn(f"-p={root / '.cache/clang-tidy-production'}", command)
        self.assertIn(f"-config-file={root / '.clang-tidy-strict'}", command)
        pattern = command[-1]
        self.assertIsNotNone(re.fullmatch(pattern, str(root / selected)))
        self.assertIsNone(re.search(pattern, str(root / "src/a1.cc")))
        self.assertIsNone(re.search(pattern, str(root / "src/a[1].cc.extra")))
        self.assertEqual(json.dumps(entries), original)

    def test_zero_or_negative_jobs_do_not_enable_unbounded_native_parallelism(
        self,
    ) -> None:
        for value in ("0", "-1"):
            with self.subTest(value=value), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                driver.positive_jobs(value)
        self.assertEqual(driver.positive_jobs("3"), 3)

    def test_wrapper_propagates_runner_failure_and_uses_two_workers_by_default(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            entries = [
                {"directory": str(root), "file": f"src/{name}.cc"}
                for name in ("a", "b", "c")
            ]
            (root / "compile_commands.json").write_text(json.dumps(entries))
            with mock.patch.object(
                driver, "workspace_root", return_value=root
            ), mock.patch.object(
                driver.subprocess, "run", return_value=mock.Mock(returncode=17)
            ) as run, mock.patch.object(
                sys,
                "argv",
                [
                    "clang_tidy",
                    "--tool=/tidy",
                    "--runner=/runner",
                    "--config=.clang-tidy",
                ],
            ):
                self.assertEqual(driver.main(), 17)
            self.assertIn("-j=2", run.call_args.args[0])
            self.assertNotIn("-enable-check-profile", run.call_args.args[0])

    def test_strict_scope_uses_testonly_and_architectural_boundaries(self) -> None:
        xml = (
            "<query>"
            + "".join(
                (
                    rule("production", "//src/model:record.cc"),
                    rule("helper", "//src/model:fixture.cc", True),
                    rule("simulator", "//src/simulation:scheduler.cc"),
                    rule("testing", "//src/runtime/testing:main.cc"),
                    rule("test", "//src/model:record_test.cc"),
                    rule("bench", "//src/model:record_bench.cc"),
                    rule("fuzzer", "//src/model:record_fuzz.cc"),
                )
            )
            + "</query>"
        )
        self.assertEqual(production_sources_from_query(xml), {"src/model/record.cc"})

    def test_ordinary_scope_keeps_test_simulation_fuzz_and_benchmark_sources(
        self,
    ) -> None:
        files = [
            "src/model/record.cc",
            "src/model/record_test.cc",
            "src/simulation/scheduler.cc",
            "src/model/record_fuzz.cc",
            "src/model/record_bench.cc",
        ]
        entries = [{"file": name} for name in files]
        self.assertEqual(select_files(entries, [], None), sorted(files))
        self.assertEqual(select_files(entries, [], {files[0]}), [files[0]])

    def test_missing_or_empty_selection_fails_instead_of_passing_silently(self) -> None:
        for entries, requested, production in (
            ([], [], None),
            ([{}], [], None),
            ([{"file": "src/a.cc"}], ["src/missing.cc"], None),
            ([{"file": "src/a.cc"}], [], set()),
            ([{"file": "src/a.cc"}], [], {"src/a.cc", "src/missing.cc"}),
        ):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                select_files(entries, requested, production)


class PartitionedAnalysisTest(unittest.TestCase):
    def entries(self, root: Path) -> list[dict]:
        return [
            {
                "directory": str(root),
                "file": source,
                "arguments": ["clang", define, "-c", source],
            }
            for source, define in (
                ("src/model/record.cc", "-DPRODUCTION"),
                ("src/model/record.cc", "-DTEST_VARIANT"),
                ("src/model/record_test.cc", "-DTEST"),
            )
        ]

    def test_partition_keeps_other_compile_variants_of_production_files(self) -> None:
        entries = self.entries(Path("/workspace"))
        production = entries[:1]
        remaining = driver.remaining_commands(entries, production)
        self.assertEqual(remaining, entries[1:])
        self.assertEqual(
            {driver.command_key(entry) for entry in remaining + production},
            {driver.command_key(entry) for entry in entries},
        )
        for stale in (
            [],
            [{**production[0], "arguments": ["clang", "-DSTALE"]}],
            [{**production[0], "directory": "/other-workspace"}],
        ):
            with self.subTest(stale=stale), self.assertRaisesRegex(ValueError, "stale"):
                driver.remaining_commands(entries, stale)

    def test_both_passes_run_and_propagate_failures_without_changing_databases(self):
        for requested in ([], ["src/model/record.cc"]):
            for failures in ((7, 0), (0, 9), (0, 0)):
                with self.subTest(requested=requested, failures=failures):
                    with tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary)
                        entries = self.entries(root)
                        database = root / "compile_commands.json"
                        strict = (
                            root / driver.PRODUCTION_DATABASE_DIRECTORY / database.name
                        )
                        strict.parent.mkdir(parents=True)
                        database.write_text(json.dumps(entries))
                        strict.write_text(json.dumps(entries[:1]))
                        before = (database.read_bytes(), strict.read_bytes())
                        seen = []

                        def run(command, **kwargs):
                            path = Path(
                                next(
                                    arg[3:] for arg in command if arg.startswith("-p=")
                                )
                            )
                            seen.append(
                                (
                                    command,
                                    json.loads((path / database.name).read_text()),
                                )
                            )
                            self.assertEqual(kwargs["cwd"], root)
                            return mock.Mock(returncode=failures[len(seen) - 1])

                        with (
                            mock.patch.object(
                                driver, "workspace_root", return_value=root
                            ),
                            mock.patch.object(
                                driver,
                                "production_targets",
                                return_value={
                                    "//src/model:record": {"src/model/record.cc"}
                                },
                            ),
                            mock.patch.object(
                                driver.subprocess, "run", side_effect=run
                            ),
                            mock.patch.object(
                                sys,
                                "argv",
                                [
                                    "clang_tidy",
                                    "--tool=/tidy",
                                    "--runner=/runner",
                                    "--config=.clang-tidy",
                                    "--production-config=.clang-tidy-strict",
                                    *requested,
                                ],
                            ),
                        ):
                            self.assertEqual(driver.main(), failures[0] or failures[1])
                        self.assertEqual(len(seen), 2)
                        self.assertEqual(
                            seen[0][1], entries[1:2] if requested else entries[1:]
                        )
                        self.assertEqual(seen[1][1], entries[:1])
                        self.assertIn(
                            f"-config-file={root / '.clang-tidy-strict'}", seen[1][0]
                        )
                        self.assertEqual(
                            (database.read_bytes(), strict.read_bytes()), before
                        )

    def test_production_checks_cover_the_baseline_checks_and_headers(self):
        root = Path(__file__).resolve().parents[1]
        ordinary = (root / ".clang-tidy").read_text()
        strict = (root / ".clang-tidy-strict").read_text()

        def checks(text):
            section = text.split("Checks: >-\n", 1)[1].split("WarningsAsErrors:", 1)[0]
            return {value.strip() for value in section.split(",")}

        ordinary_checks, strict_checks = checks(ordinary), checks(strict)
        self.assertLessEqual(
            {value for value in ordinary_checks if not value.startswith("-")},
            strict_checks,
        )
        self.assertLessEqual(
            {value for value in strict_checks if value.startswith("-")},
            ordinary_checks,
        )
        for field in ("HeaderFilterRegex", "ExcludeHeaderFilterRegex"):
            self.assertEqual(
                re.search(rf"^{field}:.*$", ordinary, re.MULTILINE)[0],
                re.search(rf"^{field}:.*$", strict, re.MULTILINE)[0],
            )
        self.assertIn("WarningsAsErrors: '*'", strict)

    @unittest.skipUnless(
        os.environ.get("KWAQUE_TEST_TIDY_RUNNER"),
        "native runner supplied by the Bazel test target",
    )
    def test_native_runner_reads_each_partition_without_repeating_commands(self):
        runner = driver.resolve_runfile(os.environ["KWAQUE_TEST_TIDY_RUNNER"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            entries = self.entries(root)
            (root / "compile_commands.json").write_text(json.dumps(entries))
            strict = root / driver.PRODUCTION_DATABASE_DIRECTORY
            strict.mkdir(parents=True)
            (strict / "compile_commands.json").write_text(json.dumps(entries[:1]))
            fake = root / "fake-clang-tidy"
            write_python_executable(
                fake,
                textwrap.dedent("""\
                import json, pathlib, sys
                if '-list-checks' in sys.argv:
                    sys.exit(0)
                database = pathlib.Path(next(arg[3:] for arg in sys.argv if arg.startswith('-p=')))
                entries = json.loads((database / 'compile_commands.json').read_text())
                source = pathlib.Path(sys.argv[-1])
                with pathlib.Path('observed.jsonl').open('a') as output:
                    for entry in entries:
                        if pathlib.Path(entry['directory']) / entry['file'] == source:
                            output.write(json.dumps([database.name, entry]) + '\\n')
                """),
            )
            for name in (".clang-tidy", ".clang-tidy-strict"):
                (root / name).write_text("Checks: '*'\n")
            with (
                mock.patch.object(driver, "workspace_root", return_value=root),
                mock.patch.object(
                    driver,
                    "production_targets",
                    return_value={"//src/model:record": {"src/model/record.cc"}},
                ),
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "clang_tidy",
                        f"--tool={fake}",
                        f"--runner={runner}",
                        "--config=.clang-tidy",
                        "--production-config=.clang-tidy-strict",
                    ],
                ),
            ):
                self.assertEqual(driver.main(), 0)
            observed = [
                json.loads(line)
                for line in (root / "observed.jsonl").read_text().splitlines()
            ]
            self.assertCountEqual(
                observed,
                [
                    ["production", entries[0]],
                    *[["ordinary", entry] for entry in entries[1:]],
                ],
            )


class NativeParallelRunnerTest(unittest.TestCase):
    def test_fixture_handles_long_quoted_interpreter_and_script_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            interpreter = (
                root
                / ("python runtime's directory " + "x" * 100)
                / ("nested directory " + "y" * 100)
                / "python3"
            )
            interpreter.parent.mkdir(parents=True)
            interpreter.symlink_to(sys.executable)
            self.assertGreater(len(os.fsencode(interpreter)), 256)
            fake = root / "fake tool's executable"
            write_python_executable(
                fake,
                "import json, sys\n"
                "print(json.dumps([sys.executable, *sys.argv[1:]]))\n"
                "sys.exit(7)\n",
                str(interpreter),
            )
            arguments = ["argument with spaces", "quote's", "$(printf unexpected)"]
            result = subprocess.run(
                [str(fake), *arguments],
                cwd=interpreter.parent,
                env={**os.environ, "PATH": ""},
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 7, result.stderr)
            self.assertEqual(json.loads(result.stdout), [str(interpreter), *arguments])
            self.assertEqual(result.stderr, "")

    @unittest.skipUnless(
        os.environ.get("KWAQUE_TEST_TIDY_RUNNER"),
        "native runner supplied by the Bazel test target",
    )
    def test_bounded_overlap_full_selection_and_failure_propagation(self) -> None:
        runner = driver.resolve_runfile(os.environ["KWAQUE_TEST_TIDY_RUNNER"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sources = ("a[1].cc", "b.cc", "failure.cc", "ignored.cc")
            entries = [
                {
                    "directory": str(root),
                    "file": name,
                    "arguments": ["clang", "-c", name],
                }
                for name in sources
            ]
            entries.append(
                {
                    **entries[0],
                    "arguments": ["clang", "-DSECOND_VARIANT", "-c", sources[0]],
                }
            )
            database = root / "compile_commands.json"
            database.write_text(json.dumps(entries))
            original = database.read_bytes()
            fake = root / "fake-clang-tidy"
            write_python_executable(
                fake,
                textwrap.dedent("""\
                import fcntl, json, pathlib, sys, time
                if '-list-checks' in sys.argv:
                    sys.exit(0)
                root = pathlib.Path.cwd()
                name = pathlib.Path(sys.argv[-1]).name
                def update(delta):
                    with (root / 'state.json').open('a+') as state_file:
                        fcntl.flock(state_file, fcntl.LOCK_EX)
                        state_file.seek(0)
                        state = json.loads(state_file.read() or '{"active": 0, "peak": 0}')
                        state['active'] += delta
                        state['peak'] = max(state['peak'], state['active'])
                        state_file.seek(0)
                        state_file.truncate()
                        json.dump(state, state_file)
                update(1)
                (root / (name + '.started')).touch()
                if name in ('a[1].cc', 'b.cc'):
                    deadline = time.monotonic() + 10
                    while not all((root / (item + '.started')).exists() for item in ('a[1].cc', 'b.cc')):
                        if time.monotonic() >= deadline:
                            raise RuntimeError('the two workers did not overlap')
                        time.sleep(0.005)
                entries = json.loads((root / 'compile_commands.json').read_text())
                variants = sum(entry['file'] == name for entry in entries)
                print(f'analyzed {name}: {variants} variants')
                update(-1)
                if name == 'failure.cc':
                    print('expected diagnostic', file=sys.stderr)
                    sys.exit(7)
                """),
            )
            (root / ".clang-tidy").write_text("Checks: '*'\n")
            environment = dict(os.environ)
            environment["BUILD_WORKSPACE_DIRECTORY"] = str(root)
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(driver.__file__).resolve()),
                    f"--tool={fake}",
                    f"--runner={runner}",
                    "--config=.clang-tidy",
                    "--jobs=2",
                    *sources[:3],
                ],
                cwd=root,
                env=environment,
                text=True,
                capture_output=True,
                timeout=30,
            )
            diagnostic = result.stdout + result.stderr
            self.assertEqual(result.returncode, 1, diagnostic)
            self.assertIn("expected diagnostic", diagnostic)
            self.assertIn("analyzed a[1].cc: 2 variants", diagnostic)
            for source in sources[:3]:
                self.assertTrue((root / (source + ".started")).exists(), diagnostic)
            self.assertFalse((root / "ignored.cc.started").exists())
            self.assertEqual(
                json.loads((root / "state.json").read_text()), {"active": 0, "peak": 2}
            )
            self.assertEqual(database.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
