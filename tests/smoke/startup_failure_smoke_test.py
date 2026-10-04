from __future__ import annotations

import sys
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    EXIT_NOT_CONFIGURED,
    REACTOR_ARGUMENTS,
    assert_one_error,
    lease_loopback_endpoint,
    run_broker,
    test_directory,
    write_config,
)


class StartupFailureSmokeTest(unittest.TestCase):
    def test_invalid_config_fails_before_any_startup_stage(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with test_directory() as directory:
            root = Path(directory)
            config = root / "invalid.yaml"
            write_config(
                template,
                config,
                root / "data",
                lease_loopback_endpoint(),
                schema_version=2,
            )
            before = set(root.rglob("*"))

            result = run_broker(binary, config, *REACTOR_ARGUMENTS, cwd=root)

            # A configuration error is not retryable and says so by status.
            self.assertEqual(result.returncode, EXIT_NOT_CONFIGURED, result.stdout)
            assert_one_error(
                result.stdout, "unsupported configuration schema version 2"
            )
            # No stage, so no directory, PID file, or listener, was started.
            self.assertNotIn("startup stage=", result.stdout)
            self.assertNotIn("configuration loaded", result.stdout)
            self.assertEqual(set(root.rglob("*")), before)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
