from __future__ import annotations

import signal
import sys
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    EXIT_DATA_DIRECTORY_IN_USE,
    REACTOR_ARGUMENTS,
    BrokerProcess,
    assert_clean_shutdown,
    assert_json_response,
    assert_one_error,
    lease_loopback_endpoint,
    log_path,
    run_broker,
    test_directory,
    write_config,
)


class PidFileSmokeTest(unittest.TestCase):
    def test_second_process_cannot_damage_owned_pid_file(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with test_directory() as directory:
            root = Path(directory)
            data_directory = root / "data"
            endpoint = lease_loopback_endpoint()
            config = root / "kwaque.yaml"
            write_config(template, config, data_directory, endpoint)
            broker = BrokerProcess(binary, config, log_path(root, "broker.log"))
            try:
                broker.wait_until_ready(endpoint)
                pid_file = data_directory / "kwaque.pid"
                original_pid = pid_file.read_text(encoding="utf-8")

                # The contender shares the endpoint, so reaching the PID check
                # also proves it runs before any listener is bound.
                contender = run_broker(binary, config, *REACTOR_ARGUMENTS)
                self.assertEqual(
                    contender.returncode, EXIT_DATA_DIRECTORY_IN_USE, contender.stdout
                )
                assert_one_error(contender.stdout, "PID file is already locked")
                self.assertEqual(pid_file.read_text(encoding="utf-8"), original_pid)
                assert_json_response(endpoint, "/v1/health/ready", {"status": "ready"})

                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                self.assertFalse(pid_file.exists())
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
