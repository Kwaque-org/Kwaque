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
        self.script.write_text(
            """
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
directories = [pathlib.Path(x) for x in args if not x.startswith("-")]
assert directories and all(d.is_dir() for d in directories)
# Coverage marks: the catalog at start, then each hit, as a simulation target
# prints them; a merge prints the hits its inputs reach.
for entry in filter(None, option("-mark_catalog").split(",")):
    name, kind = entry.split(":")
    print(f"kwaque-mark-catalog {name} {kind}", flush=True)
hits = option("-merge_mark_hits" if option("-merge") == "1" else "-mark_hits")
for name in filter(None, hits.split(",")):
    print(f"kwaque-mark-hit {name}", flush=True)
if option("-merge") == "1":
    # The union of the inputs, less those named redundant; an input that
    # fails is written as an artifact and left out, as libFuzzer's merge does.
    if option("-hang_merge"):
        import time
        time.sleep(60)
    for directory in directories[1:]:
        for path in directory.iterdir():
            if path.read_bytes() == b"fails":
                pathlib.Path(option("-artifact_prefix") + "crash-merged").write_bytes(b"fails")
            elif not path.name.startswith("redundant"):
                (directories[0] / path.name).write_bytes(path.read_bytes())
    sys.exit(0)
assert any(p.read_bytes() == b"KQS1-extra" for d in directories for p in d.iterdir())
pathlib.Path("loaded").write_text(
    "\\n".join(sorted(p.name for d in directories[1:] for p in d.iterdir())))
(directories[0] / "new-input").write_bytes(b"new")
pathlib.Path("child-working-directory").touch()
print("fake campaign executed", flush=True)
if option("-fail"):
    pathlib.Path(option("-artifact_prefix") + "crash-example").write_bytes(b"KQS1-extra")
    sys.exit(77)
""",
            encoding="utf-8",
        )
        self.write_launcher(Path(sys.executable))
        self.environment = mock.patch.dict(
            os.environ,
            {
                "TEST_TMPDIR": str(self.work),
                "TEST_UNDECLARED_OUTPUTS_DIR": str(self.outputs),
                "RUNFILES_DIR": str(self.runfiles),
                "TEST_WORKSPACE": "workspace",
                "KWAQUE_FUZZ_MINIMIZE_SECONDS": "0",
                "ASAN_SYMBOLIZER_PATH": "",
                "ASAN_OPTIONS": "",
                "LSAN_OPTIONS": "",
                "UBSAN_OPTIONS": "",
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write_launcher(self, interpreter: Path) -> None:
        # Keep the kernel interpreter line short; hermetic paths are ordinary
        # quoted arguments to exec, so their length and spaces remain harmless.
        self.binary.write_text(
            "#!/bin/sh\nexec "
            + shlex.quote(str(interpreter))
            + " "
            + shlex.quote(str(self.script))
            + ' "$@"\n',
            encoding="utf-8",
        )
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
            return wrapper.main(
                ["--binary=fake_fuzzer", "--seed=seed", "--", *arguments]
            )

    def test_runfiles_dictionary_and_sanitizer_paths_survive_changed_child_cwd(
        self,
    ) -> None:
        (self.runfiles / "dictionary").write_text("dictionary")
        (self.runfiles / "suppressions").write_text("suppression")
        with mock.patch.dict(
            os.environ,
            {
                "ASAN_SYMBOLIZER_PATH": "fake_fuzzer",
                "LSAN_OPTIONS": "suppressions=suppressions:print_suppressions=0",
                "UBSAN_OPTIONS": "halt_on_error=1:suppressions=suppressions",
            },
        ):
            environment = wrapper.normalized_environment()
            self.assertIn(
                str(self.runfiles / "suppressions"), environment["LSAN_OPTIONS"]
            )
            self.assertIn(
                str(self.runfiles / "suppressions"), environment["UBSAN_OPTIONS"]
            )
            self.assertEqual(self.run_wrapper("-dict=dictionary"), 0)
        self.assertEqual(self.seed.read_bytes(), b"KQS1-extra")
        self.assertFalse((self.runfiles / "child-working-directory").exists())
        self.assertEqual(len(list(self.work.glob("*/child-working-directory"))), 1)
        self.assertEqual(len(list(self.outputs.glob("*/campaign.log"))), 1)

    def test_failure_is_preserved_and_original_and_minimized_inputs_are_retained(
        self,
    ) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(
            next(self.outputs.glob("*/crash-*")).read_bytes(), b"KQS1-extra"
        )
        self.assertEqual(next(self.outputs.glob("*/minimized-*")).read_bytes(), b"KQS1")
        self.assertTrue(next(self.outputs.glob("*/minimize.log")).is_file())

    def test_a_reduction_is_held_to_the_verdict_the_campaign_failed_with(
        self,
    ) -> None:
        # The stub says why it failed, and its reducer writes down the
        # verdict it was told to keep.
        self.script.write_text(
            self.script.read_text()
            .replace(
                'pathlib.Path(option("-exact_artifact_path")).write_bytes(b"KQS1")',
                'pathlib.Path(option("-exact_artifact_path")).write_bytes('
                'os.environ.get("KWAQUE_FUZZ_REQUIRED_VERDICT", "none").encode())',
            )
            .replace(
                'if option("-fail"):',
                'if option("-fail"):\n'
                '    assert "KWAQUE_FUZZ_REQUIRED_VERDICT" not in os.environ\n'
                '    print("kwaque-fuzz-verdict altered", flush=True)\n'
                '    print("kwaque-fuzz-verdict lost", flush=True)',
            )
        )
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(next(self.outputs.glob("*/minimized-*")).read_bytes(), b"lost")

    def test_a_failure_that_names_no_verdict_is_reduced_as_any_failure(self) -> None:
        self.script.write_text(
            self.script.read_text()
            .replace(
                'pathlib.Path(option("-exact_artifact_path")).write_bytes(b"KQS1")',
                'pathlib.Path(option("-exact_artifact_path")).write_bytes('
                'os.environ.get("KWAQUE_FUZZ_REQUIRED_VERDICT", "none").encode())',
            )
            .replace(
                'if option("-fail"):',
                'if option("-fail"):\n'
                '    print("kwaque-fuzz-verdict Not A Verdict", flush=True)',
            )
        )
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(next(self.outputs.glob("*/minimized-*")).read_bytes(), b"none")

    def test_reducer_failure_keeps_the_original_input_and_failing_status(self) -> None:
        self.script.write_text(
            self.script.read_text().replace(
                'pathlib.Path(option("-exact_artifact_path")).write_bytes(b"KQS1")\n    sys.exit(0)',
                "sys.exit(1)",
            )
        )
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MINIMIZE_SECONDS": "1"}):
            self.assertEqual(self.run_wrapper("-fail=1"), 77)
        self.assertEqual(
            next(self.outputs.glob("*/crash-*")).read_bytes(), b"KQS1-extra"
        )
        self.assertEqual(
            next(self.outputs.glob("*/minimized-*")).read_bytes(), b"KQS1-extra"
        )

    def test_repeated_invocations_have_fresh_corpora_and_outputs(self) -> None:
        before = Path.cwd()
        for _ in range(2):
            self.assertEqual(self.run_wrapper(), 0)
        self.assertEqual(Path.cwd(), before)
        self.assertEqual(len(list(self.work.glob("*/corpus"))), 2)
        self.assertEqual(len(list(self.outputs.glob("*/campaign.log"))), 2)

    def test_a_kept_corpus_carries_inputs_into_the_next_campaign(self) -> None:
        kept = self.root / "kept-corpus"
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": str(kept)}):
            self.assertEqual(self.run_wrapper(), 0)
            (kept / "new-input").rename(kept / "earlier-input")
            self.assertEqual(self.run_wrapper(), 0)
        self.assertEqual(
            {path.name for path in kept.iterdir()},
            {"000-seed", "earlier-input", "new-input"},
        )
        latest = max(
            self.work.glob("*/loaded"), key=lambda path: path.stat().st_mtime_ns
        )
        self.assertIn("earlier-input", latest.read_text().split())

    def test_a_campaign_starts_from_a_bounded_sample_of_the_kept_corpus(
        self,
    ) -> None:
        kept = self.root / "kept-corpus"
        kept.mkdir()
        for index in range(400):
            (kept / f"kept-{index:03d}").write_bytes(b"%d" % index)
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": str(kept)}):
            self.assertEqual(self.run_wrapper("-seed=7"), 0)
        loaded = next(self.work.glob("*/loaded")).read_text().split()
        self.assertIn("000-seed", loaded)
        self.assertIn(
            len([name for name in loaded if name.startswith("kept-")]),
            wrapper.CORPUS_SUBSET_SIZES,
        )
        self.assertEqual(len(list(kept.glob("kept-*"))), 400)
        # The campaign's seed names its sample of a given corpus.
        samples = []
        for seed in ("7", "7", "8"):
            sample = Path(tempfile.mkdtemp(dir=self.root))
            wrapper.copy_subset(kept, sample, wrapper.random.Random(seed))
            samples.append(sorted(path.name for path in sample.iterdir()))
        self.assertEqual(samples[0], samples[1])
        self.assertNotEqual(samples[0], samples[2])

    def test_the_kept_corpus_becomes_the_merge_of_it_and_what_the_campaign_found(
        self,
    ) -> None:
        kept = self.root / "kept-corpus"
        kept.mkdir()
        (kept / "redundant-input").write_bytes(b"redundant")
        (kept / "covering-input").write_bytes(b"covering")
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": str(kept)}):
            self.assertEqual(self.run_wrapper(), 0)
        self.assertEqual(
            {path.name for path in kept.iterdir()},
            {"000-seed", "covering-input", "new-input"},
        )
        self.assertTrue(next(self.outputs.glob("*/merge.log")).is_file())

    def test_an_input_that_fails_in_the_merge_fails_the_campaign(self) -> None:
        kept = self.root / "kept-corpus"
        kept.mkdir()
        (kept / "failing-input").write_bytes(b"fails")
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": str(kept)}):
            self.assertNotEqual(self.run_wrapper(), 0)
        self.assertEqual(
            next(self.outputs.glob("*/crash-merged")).read_bytes(), b"fails"
        )
        self.assertEqual({path.name for path in kept.iterdir()}, {"failing-input"})

    def test_a_merge_that_does_not_finish_keeps_the_corpus(self) -> None:
        kept = self.root / "kept-corpus"
        kept.mkdir()
        (kept / "earlier-input").write_bytes(b"earlier")
        with (
            mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": str(kept)}),
            mock.patch.object(wrapper, "MERGE_SECONDS", 1),
        ):
            self.assertEqual(self.run_wrapper("-hang_merge=1"), 0)
        self.assertEqual({path.name for path in kept.iterdir()}, {"earlier-input"})
        self.assertIn(
            "external watchdog expired",
            next(self.outputs.glob("*/merge.log")).read_text(),
        )

    def test_the_watchdog_waits_for_every_input_libfuzzer_runs_first(self) -> None:
        (self.runfiles / "second").write_bytes(b"KQS1-second")
        bounds = []
        run_logged = wrapper.run_logged

        def recorded(command, work, environment, log, timeout):
            bounds.append(timeout)
            return run_logged(command, work, environment, log, timeout)

        with (
            mock.patch.object(wrapper, "run_logged", recorded),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            status = wrapper.main(
                [
                    "--binary=fake_fuzzer",
                    "--seed=seed",
                    "--seed=second",
                    "--",
                    "-max_total_time=2",
                    "-timeout=3",
                ]
            )
        self.assertEqual(status, 0)
        # The empty input, both seeds and a round of mutations, each within
        # -timeout, after the campaign's own seconds.
        self.assertEqual(bounds[0], 2 + 3 * (1 + 2 + wrapper.MUTATIONS_PER_ROUND) + 5)

    def test_a_relative_kept_corpus_is_rejected(self) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_CORPUS_DIR": "corpus"}):
            with self.assertRaises(ValueError):
                self.run_wrapper()

    def test_limits_reject_unbounded_or_oversized_campaigns(self) -> None:
        for argument in (
            "-max_total_time=0",
            "-max_total_time=601",
            "-timeout=0",
            "-timeout=61",
            f"-max_len={wrapper.MAXIMUM_INPUT_BYTES + 1}",
            "-max_len=0",
        ):
            with self.subTest(argument=argument), self.assertRaises(ValueError):
                self.run_wrapper(argument)
        # A target may bound its input above the default, up to the ceiling:
        # the configuration fuzzer runs one byte past its 64 KiB input cap.
        self.assertEqual(
            wrapper.positive_limit(
                ["-max_len=65537"], "max_len", 4096, wrapper.MAXIMUM_INPUT_BYTES
            ),
            65537,
        )
        self.assertEqual(
            wrapper.positive_limit(
                ["-max_total_time=2", "-max_total_time=600"], "max_total_time", 2, 600
            ),
            600,
        )

    def test_output_overrides_and_parallel_campaigns_are_rejected(self) -> None:
        for argument in (
            "-artifact_prefix=elsewhere/",
            "-exact_artifact_path=elsewhere",
            "-jobs=2",
            "-workers=2",
            "-fork=1",
            "-merge=1",
            "-minimize_crash=1",
        ):
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
            status = wrapper.run_logged(
                [sys.executable, "-c", "import signal; signal.pause()"],
                self.work,
                dict(os.environ),
                self.outputs / "timeout.log",
                1,
            )
        self.assertEqual(status, 124)
        self.assertIn(
            "external watchdog expired", (self.outputs / "timeout.log").read_text()
        )

    def test_logs_preserve_child_diagnostics_and_failure_status(self) -> None:
        self.work.mkdir()
        diagnostic = "native diagnostic\n"
        with contextlib.redirect_stdout(io.StringIO()) as captured:
            status = wrapper.run_logged(
                [
                    sys.executable,
                    "-c",
                    "import sys; sys.stdout.write(sys.argv[1]); sys.exit(77)",
                    diagnostic,
                ],
                self.work,
                dict(os.environ),
                self.root / "diagnostic.log",
                5,
            )
        self.assertEqual(status, 77)
        self.assertEqual(
            (self.root / "diagnostic.log").read_bytes(), diagnostic.encode()
        )
        self.assertEqual(captured.getvalue(), diagnostic)

    def mark_summary(self) -> str:
        return next(self.outputs.glob("*/marks-summary.txt")).read_text()

    def test_the_marks_gate_fails_a_campaign_that_missed_a_required_mark(
        self,
    ) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MARKS_GATE": "1"}):
            self.assertEqual(
                self.run_wrapper(
                    "-mark_catalog=reached:required,planted:required,seldom:rare",
                    "-mark_hits=reached",
                ),
                1,
            )
        self.assertEqual(
            self.mark_summary(),
            "planted required missed\nreached required hit\nseldom rare missed\n",
        )

    def test_a_rare_mark_never_hit_is_listed_and_fails_nothing(self) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MARKS_GATE": "1"}):
            self.assertEqual(
                self.run_wrapper(
                    "-mark_catalog=reached:required,seldom:rare",
                    "-mark_hits=reached",
                ),
                0,
            )
        self.assertEqual(
            self.mark_summary(), "reached required hit\nseldom rare missed\n"
        )

    def test_without_the_gate_marks_neither_fail_nor_are_summarized(self) -> None:
        self.assertEqual(self.run_wrapper("-mark_catalog=planted:required"), 0)
        self.assertEqual(list(self.outputs.glob("*/marks-summary.txt")), [])

    def test_a_mark_hit_only_by_the_corpus_merge_counts(self) -> None:
        kept = self.root / "kept"
        with mock.patch.dict(
            os.environ,
            {"KWAQUE_FUZZ_CORPUS_DIR": str(kept), "KWAQUE_FUZZ_MARKS_GATE": "1"},
        ):
            self.assertEqual(
                self.run_wrapper(
                    "-mark_catalog=late:required", "-merge_mark_hits=late"
                ),
                0,
            )
        self.assertEqual(self.mark_summary(), "late required hit\n")

    def test_a_mark_owned_as_required_anywhere_is_required(self) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MARKS_GATE": "1"}):
            self.assertEqual(
                self.run_wrapper("-mark_catalog=both:rare,both:required"), 1
            )
        self.assertEqual(self.mark_summary(), "both required missed\n")

    def test_a_log_the_gate_cannot_read_fails_a_passing_campaign(self) -> None:
        artifacts = self.root / "gate"
        artifacts.mkdir()
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(
                wrapper.gate_marks(artifacts, [artifacts / "campaign.log"], 0), 1
            )
            self.assertEqual(
                wrapper.gate_marks(artifacts, [artifacts / "campaign.log"], 77), 77
            )
        self.assertIn("cannot read the campaign's marks", printed.getvalue())

    def test_the_summary_is_printed_for_a_passing_campaign(self) -> None:
        artifacts = self.root / "gate"
        artifacts.mkdir()
        (artifacts / "campaign.log").write_bytes(
            b"kwaque-mark-catalog seldom rare\nkwaque-mark-catalog often required\n"
            b"kwaque-mark-hit often\n"
        )
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(
                wrapper.gate_marks(artifacts, [artifacts / "campaign.log"], 0), 0
            )
        self.assertEqual(printed.getvalue(), "often required hit\nseldom rare missed\n")

    def test_a_failed_campaign_keeps_its_status_and_still_summarizes(self) -> None:
        with mock.patch.dict(os.environ, {"KWAQUE_FUZZ_MARKS_GATE": "1"}):
            self.assertEqual(
                self.run_wrapper("-mark_catalog=planted:required", "-fail=1"), 77
            )
        self.assertEqual(self.mark_summary(), "planted required missed\n")

    def test_workspace_relative_and_absolute_runfiles_resolve(self) -> None:
        nested = self.runfiles / "workspace" / "nested"
        nested.mkdir(parents=True)
        (nested / "input").touch()
        self.assertEqual(wrapper.resolve_runfile("nested/input"), nested / "input")
        self.assertEqual(wrapper.resolve_runfile(str(self.seed)), self.seed)


if __name__ == "__main__":
    unittest.main()
