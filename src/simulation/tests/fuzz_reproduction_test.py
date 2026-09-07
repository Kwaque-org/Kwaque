from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from bazel.fuzz_test_wrapper import normalized_environment

# Each child starts a fresh native reactor; leave room for loaded CI runners.
# The Bazel target separately bounds the complete replay matrix.
PROCESS_TIMEOUT_SECONDS = 120


def runfile(path: str) -> Path:
    root = Path(os.environ["RUNFILES_DIR"])
    workspace = os.environ.get("TEST_WORKSPACE", "_main")
    candidate = root / workspace / path
    if candidate.exists():
        return candidate.resolve()
    return (root / path).resolve()


class FuzzReproductionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.replay = runfile("src/simulation/tests/fuzz_replay")
        self.canary = runfile("src/simulation/tests/fuzz_semantic_canary")
        self.environment = normalized_environment()
        self.artifacts = Path(os.environ["TEST_UNDECLARED_OUTPUTS_DIR"])
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.replay_attempt = 0
        temporary = tempfile.TemporaryDirectory(
            prefix="reproduction-", dir=os.environ["TEST_TMPDIR"]
        )
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)

    def run_process(
        self, binary: Path, stem: str, block: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                [str(binary)],
                cwd=self.work,
                env=self.environment,
                input=block,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=PROCESS_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            diagnostic = (
                f"{binary.name} timed out after {error.timeout} seconds\n"
                f"{self.process_output(error)}"
            )
            (self.artifacts / f"{stem}.log").write_text(diagnostic, encoding="utf-8")
            if block is not None:
                (self.artifacts / f"{stem}.input.txt").write_text(
                    block, encoding="utf-8"
                )
            self.fail(diagnostic)

    def replay_block(self, block: str) -> subprocess.CompletedProcess[str]:
        return self.run_process(
            self.replay, f"replay-failure-{self.replay_attempt:03d}", block
        )

    @staticmethod
    def process_output(
        completed: subprocess.CompletedProcess[str] | subprocess.TimeoutExpired,
    ) -> str:
        # TimeoutExpired can carry bytes even when subprocess.run uses text=True.
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        return f"stdout:\n{stdout}\nstderr:\n{stderr}"

    def assert_replay_exit(
        self, block: str, expected: int
    ) -> subprocess.CompletedProcess[str]:
        self.replay_attempt += 1
        completed = self.replay_block(block)
        diagnostic = self.process_output(completed)
        if completed.returncode != expected:
            stem = f"replay-failure-{self.replay_attempt:03d}"
            (self.artifacts / f"{stem}.input.txt").write_text(block, encoding="utf-8")
            (self.artifacts / f"{stem}.log").write_text(
                f"expected exit: {expected}\nactual exit: {completed.returncode}\n{diagnostic}",
                encoding="utf-8",
            )
        self.assertEqual(completed.returncode, expected, diagnostic)
        return completed

    @staticmethod
    def mutate_section(block: str, name: str) -> str:
        lines = block.splitlines()
        section = lines.index(
            next(line for line in lines if line.startswith(f"SECTION {name} "))
        )
        data = next(
            index
            for index in range(section + 1, len(lines))
            if lines[index].startswith("D ")
        )
        replacement = "0" if lines[data][2] != "0" else "1"
        lines[data] = "D " + replacement + lines[data][3:]
        return "\n".join(lines) + "\n"

    @staticmethod
    def mutate_metadata(block: str, key: str) -> str:
        lines = block.splitlines()
        index = next(
            index for index, line in enumerate(lines) if line.startswith(f"{key} ")
        )
        replacement = "0" if lines[index][-1] != "0" else "1"
        lines[index] = lines[index][:-1] + replacement
        return "\n".join(lines) + "\n"

    @staticmethod
    def replace_section_size(block: str, name: str, size: int) -> str:
        lines = block.splitlines()
        index = next(
            index
            for index, line in enumerate(lines)
            if line.startswith(f"SECTION {name} ")
        )
        lines[index] = f"SECTION {name} {size:016x}"
        return "\n".join(lines) + "\n"

    @staticmethod
    def change_event_with_valid_integrity(block: str) -> str:
        lines = block.splitlines()
        start = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("SECTION EVENTS ")
        )
        end = lines.index("ENDSECTION EVENTS", start)
        events = bytearray.fromhex("".join(line[2:] for line in lines[start + 1 : end]))
        # A valid schema-1 event begins after the 60-byte log header and its
        # two-byte record length. Change only its monotonic time, preserving
        # descriptor names, field types, sequence, and the complete encoding.
        event_start = 62
        event_name_length = events[event_start + 6]
        events[event_start + 8 + event_name_length] ^= 1
        lines[start + 1 : end] = [
            "D " + events[offset : offset + 2000].hex()
            for offset in range(0, len(events), 2000)
        ]
        event_digest = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("EVENT_DIGEST ")
        )
        lines[event_digest] = "EVENT_DIGEST " + hashlib.sha256(events).hexdigest()
        return FuzzReproductionTest.refresh_envelope_integrity(lines)

    @staticmethod
    def refresh_envelope_integrity(lines: list[str]) -> str:
        metadata = dict(
            line.split(" ", 1)
            for line in lines[
                1 : lines.index(
                    next(line for line in lines if line.startswith("SECTION "))
                )
            ]
        )
        integrity = hashlib.sha256(b"KQREPRO" + (1).to_bytes(4, "big"))
        for key in (
            "HARNESS",
            "HARNESS_VERSION",
            "MASTER_SEED",
            "RANDOM_ALGORITHM",
            "COORDINATE_SCHEMA",
            "ORDERING",
            "TRACE_SCHEMA",
            "EVENT_SCHEMA",
            "EVENT_EPOCH",
            "OUTCOME_CODE",
            "OUTCOME_OPERATION",
            "CONFIGURATION_DIGEST",
            "INPUT_DIGEST",
            "TERMINAL_DIGEST",
            "TRACE_DIGEST",
            "EVENT_DIGEST",
        ):
            integrity.update(bytes.fromhex(metadata[key]))
        envelope_digest = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("ENVELOPE_DIGEST ")
        )
        lines[envelope_digest] = "ENVELOPE_DIGEST " + integrity.hexdigest()
        return "\n".join(lines) + "\n"

    @staticmethod
    def obsolete_harness_with_valid_integrity(block: str) -> str:
        lines = block.splitlines()
        version = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("HARNESS_VERSION ")
        )
        lines[version] = "HARNESS_VERSION 00000001"
        return FuzzReproductionTest.refresh_envelope_integrity(lines)

    def test_semantic_failure_is_complete_host_independent_and_replayable(self) -> None:
        failed = self.run_process(self.canary, "canary")
        (self.artifacts / "canary.log").write_text(
            self.process_output(failed), encoding="utf-8"
        )
        self.assertNotEqual(failed.returncode, 0, self.process_output(failed))
        self.assertIn("KQREPRO 01\n", failed.stderr, self.process_output(failed))
        self.assertIn("END KQREPRO\n", failed.stderr, self.process_output(failed))
        start = failed.stderr.index("KQREPRO 01\n")
        end = failed.stderr.index("END KQREPRO\n", start) + len("END KQREPRO\n")
        block = failed.stderr[start:end]

        (self.artifacts / "reproduction.txt").write_text(block, encoding="utf-8")
        self.assertIn("SECTION TRACE ", block)
        self.assertIn("SECTION EVENTS ", block)
        lines = block.splitlines()
        event_start = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("SECTION EVENTS ")
        )
        event_end = lines.index("ENDSECTION EVENTS", event_start)
        event_bytes = bytes.fromhex(
            "".join(line[2:] for line in lines[event_start + 1 : event_end])
        )
        emitted_digest = next(
            line for line in lines if line.startswith("EVENT_DIGEST ")
        )
        self.assertEqual(
            emitted_digest, "EVENT_DIGEST " + hashlib.sha256(event_bytes).hexdigest()
        )
        self.assertIn("OUTCOME_CODE 00000010", block)
        self.assertIn("HARNESS_VERSION 00000002", block)
        rooted_home = os.sep.join(("", "home", ""))
        rooted_users = os.sep.join(("", "Users", ""))
        self.assertNotIn(rooted_home, block)
        self.assertNotIn(rooted_users, block)
        self.assertTrue(all(len(line) + 1 <= 4096 for line in block.splitlines()))
        self.assert_replay_exit(block, 0)

        divergent = self.assert_replay_exit(
            self.change_event_with_valid_integrity(block), 1
        )
        self.assertIn("operation=observability", divergent.stderr)
        self.assertIn("sequence=1", divergent.stderr)
        self.assertIn("detail=3", divergent.stderr)
        self.assert_replay_exit(block, 0)

        bad_header = block.replace("KQREPRO 01", "KQREPRO 02", 1)
        self.assert_replay_exit(bad_header, 2)
        self.assert_replay_exit(self.obsolete_harness_with_valid_integrity(block), 2)

        for key in (
            "HARNESS",
            "HARNESS_VERSION",
            "MASTER_SEED",
            "RANDOM_ALGORITHM",
            "COORDINATE_SCHEMA",
            "ORDERING",
            "TRACE_SCHEMA",
            "EVENT_SCHEMA",
            "EVENT_EPOCH",
            "CONFIGURATION_DIGEST",
            "INPUT_DIGEST",
            "TRACE_DIGEST",
            "EVENT_DIGEST",
            "ENVELOPE_DIGEST",
            "OUTCOME_CODE",
            "OUTCOME_OPERATION",
            "TERMINAL_DIGEST",
        ):
            with self.subTest(key=key):
                self.assert_replay_exit(self.mutate_metadata(block, key), 2)

        self.assert_replay_exit(self.mutate_section(block, "CONFIGURATION"), 2)
        self.assert_replay_exit(self.mutate_section(block, "INPUT"), 2)
        self.assert_replay_exit(self.mutate_section(block, "TRACE"), 2)
        self.assert_replay_exit(self.mutate_section(block, "EVENTS"), 2)
        self.assert_replay_exit(
            self.replace_section_size(block, "INPUT", 16 * 1024 + 1), 2
        )
        self.assert_replay_exit(
            self.replace_section_size(block, "TRACE", 128 * 1024 + 1), 2
        )
        self.assert_replay_exit(block + "trailing\n", 2)
        self.assert_replay_exit("KQREPRO 01" + "x" * 4096 + "\n", 2)


if __name__ == "__main__":
    unittest.main()
