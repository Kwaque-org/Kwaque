from __future__ import annotations

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


class StartupFailureSmokeTest(unittest.TestCase):
    def test_invalid_config_fails_before_listener_start(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            port = reserve_loopback_port()
            config = root / "invalid.yaml"
            write_config(
                template,
                config,
                root / "data",
                port,
                schema_version=2,
            )
            result = subprocess.run(
                [binary, "--config", config, *REACTOR_ARGUMENTS],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10.0,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unsupported configuration schema version", result.stdout)
            self.assertFalse((root / "data").exists())
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(("127.0.0.1", port))


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
