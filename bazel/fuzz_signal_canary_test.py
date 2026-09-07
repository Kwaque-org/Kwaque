"""Verify the reactor crash path emits an isolated, reusable libFuzzer input."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from bazel import fuzz_test_wrapper as wrapper


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    binary = wrapper.resolve_runfile(sys.argv[1])
    seed = wrapper.resolve_runfile(sys.argv[2])
    parent = Path(os.environ["TEST_TMPDIR"])
    with tempfile.TemporaryDirectory(prefix="signal-canary-", dir=parent) as temporary:
        root = Path(temporary)
        original_cwd = Path.cwd()
        original_crashes = set(original_cwd.glob("crash-*"))
        outputs = Path(os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]) / "signal-canary"
        os.environ["TEST_TMPDIR"] = str(root)
        os.environ["TEST_UNDECLARED_OUTPUTS_DIR"] = str(outputs)
        os.environ["KWAQUE_FUZZ_MINIMIZE_SECONDS"] = "5"
        status = wrapper.main([f"--binary={binary}", f"--seed={seed}", "--",
                               "-max_len=8", "-max_total_time=2", "-rss_limit_mb=256", "-seed=1"])
        require(status != 0, "signal canary unexpectedly succeeded")
        require(Path.cwd() == original_cwd, "wrapper changed the parent working directory")
        failures = list(outputs.glob("*/crash-*"))
        minimized = list(outputs.glob("*/minimized-crash-*"))
        require(len(failures) == len(minimized) == 1, "canary input was not retained")
        require(minimized[0].read_bytes() == b"KQS1", "canary was not minimized to its four-byte trigger")
        log = (failures[0].parent / "campaign.log").read_text(errors="replace")
        require("KQFUZZ HARNESS=signal_canary VERSION=1" in log, "canary identity was not emitted")
        # Replay only the artifact in a fresh process, with no corpus or mutation.
        replay = wrapper.run_logged(
            [str(binary), "-runs=1", "-timeout=15", f"-artifact_prefix={outputs}/", str(minimized[0])],
            root, wrapper.normalized_environment(), outputs / "replay.log", 20)
        require(replay != 0 and replay != 124, "canary artifact did not reproduce the signal")
        require("KQFUZZ HARNESS=signal_canary VERSION=1" in (outputs / "replay.log").read_text(errors="replace"),
                "artifact replay did not emit the canary identity")
        require(set(original_cwd.glob("crash-*")) == original_crashes, "canary polluted the runfiles directory")


if __name__ == "__main__":
    main()
