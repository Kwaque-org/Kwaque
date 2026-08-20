from __future__ import annotations

import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    REACTOR_ARGUMENTS,
    BrokerProcess,
    assert_clean_shutdown,
    assert_json_response,
    reserve_loopback_port,
    write_config,
)


class PidFileSmokeTest(unittest.TestCase):
    def test_second_process_cannot_damage_owned_pid_file(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_directory = root / "data"
            port = reserve_loopback_port()
            config = root / "kwaque.yaml"
            write_config(template, config, data_directory, port)
            broker = BrokerProcess(binary, config, root / "broker.log")
            try:
                broker.wait_until_ready(port)
                pid_file = data_directory / "kwaque.pid"
                original_pid = pid_file.read_text(encoding="utf-8")

                contender = subprocess.run(
                    [binary, "--config", config, *REACTOR_ARGUMENTS],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=10.0,
                )
                self.assertNotEqual(contender.returncode, 0)
                self.assertIn("PID file is already locked", contender.stdout)
                self.assertEqual(pid_file.read_text(encoding="utf-8"), original_pid)
                assert_json_response(port, "/v1/health/ready", {"status": "ready"})

                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                self.assertFalse(pid_file.exists())
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
