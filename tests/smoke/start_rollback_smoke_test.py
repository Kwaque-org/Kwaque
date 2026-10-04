from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    EXIT_FAILURE,
    REACTOR_ARGUMENTS,
    assert_one_error,
    lease_loopback_endpoint,
    run_broker,
    test_directory,
    write_config,
)


class StartRollbackSmokeTest(unittest.TestCase):
    """Failures at later startup stages must leave nothing behind.

    Configuration rejection is covered separately. These cases fail after
    configuration is accepted, including one that fails after the PID file has
    already been claimed, which is what proves rollback releases it.
    """

    def setUp(self) -> None:
        self.binary = Path(sys.argv[1])
        self.template = Path(sys.argv[2])

    def _run(self, config: Path) -> subprocess.CompletedProcess[str]:
        return run_broker(self.binary, config, *REACTOR_ARGUMENTS)

    def _assert_failed_before_admin(
        self, result: subprocess.CompletedProcess[str], expected: str
    ) -> None:
        self.assertEqual(result.returncode, EXIT_FAILURE, result.stdout)
        assert_one_error(result.stdout, expected)
        self.assertNotIn("startup stage=admin state=ready", result.stdout)

    def test_data_directory_path_is_not_a_directory(self) -> None:
        with test_directory() as directory:
            root = Path(directory)
            occupied = root / "not_a_directory"
            occupied.write_text("", encoding="utf-8")
            config = root / "kwaque.yaml"
            write_config(self.template, config, occupied, lease_loopback_endpoint())

            result = self._run(config)

            self._assert_failed_before_admin(result, "Not a directory")
            self.assertNotIn("startup stage=data_directory", result.stdout)

    @unittest.skipIf(os.geteuid() == 0, "root bypasses directory permissions")
    def test_data_directory_is_not_writable(self) -> None:
        with test_directory() as directory:
            root = Path(directory)
            data_directory = root / "read_only"
            data_directory.mkdir()
            data_directory.chmod(0o500)
            config = root / "kwaque.yaml"
            write_config(
                self.template, config, data_directory, lease_loopback_endpoint()
            )

            try:
                result = self._run(config)
            finally:
                data_directory.chmod(0o700)

            self._assert_failed_before_admin(result, "data directory is not writable")
            self.assertEqual(list(data_directory.iterdir()), [])

    def test_unavailable_admin_port_releases_the_pid_file(self) -> None:
        with test_directory() as directory:
            root = Path(directory)
            data_directory = root / "data"
            config = root / "kwaque.yaml"
            endpoint = lease_loopback_endpoint()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blocker:
                blocker.bind((endpoint.address, endpoint.port))
                blocker.listen(1)
                write_config(self.template, config, data_directory, endpoint)

                result = self._run(config)

            self.assertEqual(result.returncode, EXIT_FAILURE, result.stdout)
            self.assertRegex(
                result.stdout,
                re.compile(r"posix_listen failed .*Address already in use"),
            )
            # The data directory and PID file stages completed before the
            # listener failed, so rollback is what must remove the PID file.
            self.assertIn("startup stage=pid_file state=ready", result.stdout)
            self.assertTrue(data_directory.is_dir())
            self.assertFalse((data_directory / "kwaque.pid").exists())


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
