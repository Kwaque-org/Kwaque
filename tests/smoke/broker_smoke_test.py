from __future__ import annotations

import json
import signal
import sys
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    assert_json_response,
    http_get,
    reserve_loopback_port,
    write_config,
)


class BrokerSmokeTest(unittest.TestCase):
    def test_serves_admin_endpoints_and_stops_on_sigterm(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            port = reserve_loopback_port()
            config = root / "kwaque.yaml"
            write_config(template, config, root / "data", port)
            broker = BrokerProcess(binary, config, root / "broker.log")
            try:
                broker.wait_until_ready(port)
                assert_json_response(port, "/v1/health/live", {"status": "live"})
                assert_json_response(port, "/v1/health/ready", {"status": "ready"})

                status, content_type, body = http_get(port, "/v1/version")
                self.assertEqual(status, 200)
                self.assertEqual(content_type, "application/json")
                version = json.loads(body)
                self.assertTrue(version["version"])
                self.assertTrue(version["revision"])
                self.assertTrue(version["build_mode"])

                status, content_type, metrics = http_get(port, "/metrics")
                self.assertEqual(status, 200)
                self.assertEqual(content_type, "text/plain")
                self.assertIn("kwaque_broker_process_readiness", metrics)

                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                self.assertFalse((root / "data" / "kwaque.pid").exists())
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
