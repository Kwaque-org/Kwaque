from __future__ import annotations

import os
import signal
import socket
import sys
import time
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    assert_json_response,
    lease_loopback_endpoint,
    log_path,
    test_directory,
    write_config,
)


class SignalSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.binary = Path(sys.argv[1])
        self.template = Path(sys.argv[2])

    def test_sigint_uses_clean_shutdown_path(self) -> None:
        with test_directory() as directory:
            root = Path(directory)
            endpoint = lease_loopback_endpoint()
            config = root / "kwaque.yaml"
            write_config(self.template, config, root / "data", endpoint)
            broker = BrokerProcess(self.binary, config, log_path(root, "sigint.log"))
            try:
                broker.wait_until_ready(endpoint)
                output = broker.stop(signal.SIGINT)
                assert_clean_shutdown(output)
            finally:
                broker.kill_if_running()

    def test_sighup_is_ignored(self) -> None:
        with test_directory() as directory:
            root = Path(directory)
            endpoint = lease_loopback_endpoint()
            config = root / "kwaque.yaml"
            write_config(self.template, config, root / "data", endpoint)
            broker = BrokerProcess(self.binary, config, log_path(root, "sighup.log"))
            try:
                broker.wait_until_ready(endpoint)
                broker.process.send_signal(signal.SIGHUP)
                # A terminating disposition would end the process at once.
                time.sleep(0.5)
                self.assertIsNone(broker.process.poll(), broker.output())
                assert_json_response(endpoint, "/v1/health/ready", {"status": "ready"})
                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
            finally:
                broker.kill_if_running()

    def test_service_manager_hears_readiness_and_stopping(self) -> None:
        # The datagram socket a service manager passes in NOTIFY_SOCKET; an
        # abstract name avoids the socket path length limit.
        name = f"kwaque-notify-{os.getpid()}"
        receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        receiver.bind("\0" + name)
        receiver.settimeout(30.0)
        with receiver, test_directory() as directory:
            root = Path(directory)
            endpoint = lease_loopback_endpoint()
            config = root / "kwaque.yaml"
            write_config(self.template, config, root / "data", endpoint)
            broker = BrokerProcess(
                self.binary,
                config,
                log_path(root, "notify.log"),
                environment={**os.environ, "NOTIFY_SOCKET": "@" + name},
            )
            try:
                self.assertEqual(receiver.recv(256), b"READY=1", broker.output())
                assert_json_response(endpoint, "/v1/health/ready", {"status": "ready"})
                broker.process.send_signal(signal.SIGTERM)
                self.assertEqual(receiver.recv(256), b"STOPPING=1", broker.output())
                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
