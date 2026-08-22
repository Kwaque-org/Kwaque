from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    REACTOR_ARGUMENTS,
    reserve_loopback_port,
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
        return subprocess.run(
            [self.binary, "--config", config, *REACTOR_ARGUMENTS],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30.0,
        )

    def _assert_no_listener(self, port: int) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", port))

    def test_data_directory_path_is_not_a_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            port = reserve_loopback_port()
            occupied = root / "not_a_directory"
            occupied.write_text("", encoding="utf-8")
            config = root / "kwaque.yaml"
            write_config(self.template, config, occupied, port)

            result = self._run(config)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Not a directory", result.stdout)
            self._assert_no_listener(port)

    @unittest.skipIf(os.geteuid() == 0, "root bypasses directory permissions")
    def test_data_directory_is_not_writable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            port = reserve_loopback_port()
            data_directory = root / "read_only"
            data_directory.mkdir()
            data_directory.chmod(0o500)
            config = root / "kwaque.yaml"
            write_config(self.template, config, data_directory, port)

            try:
                result = self._run(config)
            finally:
                data_directory.chmod(0o700)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("data directory is not writable", result.stdout)
            self.assertFalse((data_directory / "kwaque.pid").exists())
            self._assert_no_listener(port)

    def test_unavailable_admin_port_releases_the_pid_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_directory = root / "data"
            config = root / "kwaque.yaml"
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blocker:
                blocker.bind(("127.0.0.1", 0))
                blocker.listen(1)
                port = blocker.getsockname()[1]
                write_config(self.template, config, data_directory, port)

                result = self._run(config)

            self.assertNotEqual(result.returncode, 0)
            self.assertRegex(
                result.stdout,
                re.compile(r"posix_listen failed .*Address already in use"),
            )
            # The data directory and PID file stages completed before the
            # listener failed, so rollback is what must remove the PID file.
            self.assertTrue(data_directory.is_dir())
            self.assertFalse((data_directory / "kwaque.pid").exists())
            self._assert_no_listener(port)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
