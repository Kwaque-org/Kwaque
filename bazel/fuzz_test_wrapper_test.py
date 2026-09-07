from __future__ import annotations

import contextlib
import io
import os
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from bazel import fuzz_test_wrapper as wrapper


class FuzzTestWrapperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runfiles = self.root / "runfiles"
        self.runfiles.mkdir()
        self.work = self.root / "temporary"
        self.outputs = self.root / "outputs"
        self.seed = self.runfiles / "seed"
        self.seed.write_bytes(b"KQS1-extra")
        self.binary = self.runfiles / "fake_fuzzer"
        self.script = self.runfiles / "fake_fuzzer.py"
        self.script.write_text('''
import os, pathlib, sys
args = sys.argv[1:]
def option(name):
    return next((x.split("=", 1)[1] for x in reversed(args) if x.startswith(name + "=")), "")
for name in ("ASAN_SYMBOLIZER_PATH",):
    if os.environ.get(name):
        assert pathlib.Path(os.environ[name]).is_file()
if option("-dict"):
    assert pathlib.Path(option("-dict")).read_text() == "dictionary"
if option("-minimize_crash") == "1":
    pathlib.Path(option("-exact_artifact_path")).write_bytes(b"KQS1")
    sys.exit(0)
corpus = pathlib.Path(args[-1])
assert corpus.is_dir()
assert any(p.read_bytes() == b"KQS1-extra" for p in corpus.iterdir())
(corpus / "new-input").write_bytes(b"new")
pathlib.Path("child-working-directory").touch()
print("fake campaign executed", flush=True)
if option("-fail"):
    pathlib.Path(option("-artifact_prefix") + "crash-example").write_bytes(b"KQS1-extra")
    sys.exit(77)
''', encoding="utf-8")
        self.write_launcher(Path(sys.executable))
        self.environment = mock.patch.dict(os.environ, {
            "TEST_TMPDIR": str(self.work),
            "TEST_UNDECLARED_OUTPUTS_DIR": str(self.outputs),
            "RUNFILES_DIR": str(self.runfiles),
            "TEST_WORKSPACE": "workspace",
            "KWAQUE_FUZZ_MINIMIZE_SECONDS": "0",
            "ASAN_SYMBOLIZER_PATH": "",
            "ASAN_OPTIONS": "", "LSAN_OPTIONS": "", "UBSAN_OPTIONS": "",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write_launcher(self, interpreter: Path) -> None:
        # Keep the kernel interpreter line short; hermetic paths are ordinary
        # quoted arguments to exec, so their length and spaces remain harmless.
        self.binary.write_text(
            "#!/bin/sh\nexec " + shlex.quote(str(interpreter)) + " "
            + shlex.quote(str(self.script)) + ' "$@"\n', encoding="utf-8")
        self.binary.chmod(0o700)

    def test_long_quoted_interpreter_path_runs_the_real_child(self) -> None:
        for component in ("x" * 80, "interpreter's directory " + "x" * 60):
            with self.subTest(component=component):
                directory = self.root
                for _ in range(4):
                    directory /= component
                directory.mkdir(parents=True)
                interpreter = directory / "python"
                interpreter.symlink_to(Path(sys.executable).resolve())
                self.assertGreater(len(os.fsencode(interpreter)), 256)
                self.write_launcher(interpreter)
                self.assertEqual(self.binary.read_text().splitlines()[0], "#!/bin/sh")
                self.assertEqual(self.run_wrapper(), 0)

    def run_wrapper(self, *arguments: str) -> int:
        with contextlib.redirect_stdout(io.StringIO()):
            return wrapper.main(["--binary=fake_fuzzer", "--seed=seed", "--", *arguments])

    def test_runfiles_dictionary_and_sanitizer_paths_survive_changed_child_cwd(self) -> None:
        (self.runfiles / "dictionary").write_text("dictionary")
        (self.runfiles / "suppressions").write_text("suppression")
        with mock.patch.dict(os.environ, {
            "ASAN_SYMBOLIZER_PATH": "fake_fuzzer",
            "LSAN_OPTIONS": "suppressions=suppressions:print_suppressions=0",
            "UBSAN_OPTIONS": "halt_on_error=1:suppressions=suppressions",
        }):
            environment = wrapper.normalized_environment()
            self.assertIn(str(self.runfiles / "suppressions"), environment["LSAN_OPTIONS"])
            self.assertIn(str(self.runfiles / "suppressions"), environment["UBSAN_OPTIONS"])
            self.assertEqual(self.run_wrapper("-dict=dictionary"), 0)
        self.assertEqual(self.seed.read_bytes(), b"KQS1-extra")
        self.assertFalse((self.runfiles / "child-working-directory").exists())
        self.assertEqual(len(list(self.work.glob("*/child-working-directory"))), 1)
        self.assertEqual(len(list(self.outputs.glob("*/campaign.log"))), 1)

    def test_failure_is_preserved_and_original_and_minimized_inputs_are_retained(self) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(next(self.outputs.glob("*/crash-*")).read_bytes(), b"KQS1-extra")
        self.assertEqual(next(self.outputs.glob("*/minimized-*")).read_bytes(), b"KQS1")
        self.assertTrue(next(self.outputs.glob("*/minimize.log")).is_file())

    def test_reducer_failure_keeps_the_original_input_and_failing_status(self) -> None:
        self.script.write_text(self.script.read_text().replace(
            'pathlib.Path(option("-exact_artifact_path")).write_bytes(b"KQS1")\n    sys.exit(0)',
            'sys.exit(1)'))
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(next(self.outputs.glob("*/crash-*")).read_bytes(), b"KQS1-extra")
        self.assertEqual(next(self.outputs.glob("*/minimized-*")).read_bytes(), b"KQS1-extra")

    def test_repeated_invocations_have_fresh_corpora_and_outputs(self) -> None:
        before = Path.cwd()
        for _ in range(2):
            self.assertEqual(self.run_wrapper(), 0)
        self.assertEqual(Path.cwd(), before)
        self.assertEqual(len(list(self.work.glob("*/corpus"))), 2)
        self.assertEqual(len(list(self.outputs.glob("*/campaign.log"))), 2)

    def test_limits_reject_unbounded_or_oversized_campaigns(self) -> None:
        for argument in ("-max_total_time=0", "-max_total_time=601", "-timeout=0",
                         "-timeout=61", "-max_len=16385", "-max_len=0"):
            with self.subTest(argument=argument), self.assertRaises(ValueError):
                self.run_wrapper(argument)
        self.assertEqual(wrapper.positive_limit(["-max_total_time=2", "-max_total_time=600"],
                                               "max_total_time", 2, 600), 600)

    def test_output_overrides_and_parallel_campaigns_are_rejected(self) -> None:
        for argument in ("-artifact_prefix=elsewhere/", "-exact_artifact_path=elsewhere",
                         "-jobs=2", "-workers=2", "-fork=1", "-merge=1", "-minimize_crash=1"):
            with self.subTest(argument=argument), self.assertRaises(ValueError):
                self.run_wrapper(argument)

    def test_missing_runfiles_and_oversized_seed_fail_before_running(self) -> None:
        with self.assertRaises(FileNotFoundError):
            wrapper.resolve_runfile("missing")
        with self.assertRaises(ValueError):
            self.run_wrapper("-max_len=4")
        self.assertFalse(list(self.outputs.glob("*/campaign.log")))

    def test_external_watchdog_terminates_child_and_records_failure(self) -> None:
        self.work.mkdir()
        self.outputs.mkdir()
        with contextlib.redirect_stdout(io.StringIO()):
            status = wrapper.run_logged([sys.executable, "-c", "import signal; signal.pause()"],
                                        self.work, dict(os.environ), self.outputs / "timeout.log", 1)
        self.assertEqual(status, 124)
        self.assertIn("external watchdog expired", (self.outputs / "timeout.log").read_text())

    def test_logs_preserve_child_diagnostics_and_failure_status(self) -> None:
        self.work.mkdir()
        diagnostic = "native diagnostic\n"
        with contextlib.redirect_stdout(io.StringIO()) as captured:
            status = wrapper.run_logged(
                [sys.executable, "-c", "import sys; sys.stdout.write(sys.argv[1]); sys.exit(77)", diagnostic],
                self.work, dict(os.environ), self.root / "diagnostic.log", 5)
        self.assertEqual(status, 77)
        self.assertEqual((self.root / "diagnostic.log").read_bytes(), diagnostic.encode())
        self.assertEqual(captured.getvalue(), diagnostic)

    def test_workspace_relative_and_absolute_runfiles_resolve(self) -> None:
        nested = self.runfiles / "workspace" / "nested"
        nested.mkdir(parents=True)
        (nested / "input").touch()
        self.assertEqual(wrapper.resolve_runfile("nested/input"), nested / "input")
        self.assertEqual(wrapper.resolve_runfile(str(self.seed)), self.seed)


if __name__ == "__main__":
    unittest.main()
