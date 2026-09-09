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
