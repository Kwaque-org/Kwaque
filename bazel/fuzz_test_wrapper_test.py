from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from bazel import fuzz_test_wrapper


class FuzzTestWrapperTest(unittest.TestCase):
    def test_copies_seeds_and_executes_fuzzer_with_writable_corpus(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            seed = temporary_root / "seed"
            seed.write_bytes(b"seed-data")
            argv = [
                "fuzz_test_wrapper.py",
                "--binary=/bin/echo",
                f"--seed={seed}",
                "--",
                "-max_len=64",
            ]

            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.dict(os.environ, {"TEST_TMPDIR": temporary_directory}),
                mock.patch.object(os, "execv") as execv,
            ):
                fuzz_test_wrapper.main()

            corpus = temporary_root / "corpus"
            self.assertEqual((corpus / "000-seed").read_bytes(), b"seed-data")
            execv.assert_called_once_with(
                Path("/bin/echo"),
                ["/bin/echo", "-max_len=64", str(corpus)],
            )


if __name__ == "__main__":
    unittest.main()
