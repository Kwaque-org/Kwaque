from __future__ import annotations

import json
import re
import signal
import sys
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    assert_json_response,
    http_get,
    http_request,
    lease_loopback_endpoint,
    log_path,
    test_directory,
    version_fields,
    write_config,
)


class BrokerSmokeTest(unittest.TestCase):
    def test_serves_admin_endpoints_and_stops_on_sigterm(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with test_directory() as directory:
            root = Path(directory)
            endpoint = lease_loopback_endpoint()
            config = root / "kwaque.yaml"
            write_config(template, config, root / "data", endpoint)
            broker = BrokerProcess(binary, config, log_path(root, "broker.log"))
            try:
                broker.wait_until_ready(endpoint)
                assert_json_response(endpoint, "/v1/health/live", {"status": "live"})
                assert_json_response(endpoint, "/v1/health/ready", {"status": "ready"})

                # The HTTP identity and the command-line identity agree.
                status, content_type, body = http_get(endpoint, "/v1/version")
                self.assertEqual(status, 200)
                self.assertEqual(content_type, "application/json")
                served = json.loads(body)
                printed = version_fields(binary)
                for field in ("version", "revision", "build_timestamp", "build_mode"):
                    self.assertEqual(served[field], printed[field], field)
                self.assertEqual(served["dirty"], printed["dirty"] == "true")

                status, content_type, metrics = http_get(endpoint, "/metrics")
                self.assertEqual(status, 200)
                self.assertEqual(content_type, "text/plain")
                self.assertRegex(
                    metrics,
                    r"(?m)^kwaque_broker_process_readiness(\{[^}]*\})? 1(\.0*)?$",
                )
                self.assertRegex(
                    metrics,
                    rf'(?m)^kwaque_build_info\{{[^}}]*version="{re.escape(printed["version"])}"',
                )

                # Probes may use HEAD; it carries the GET status without a body.
                status, _, _, body = http_request(
                    endpoint, "/v1/health/ready", method="HEAD"
                )
                self.assertEqual((status, body), (200, ""))
                status, _, _, body = http_request(endpoint, "/metrics", method="HEAD")
                self.assertEqual((status, body), (200, ""))

                # Routing errors are RFC 9457 problem documents.
                status, content_type, _, body = http_request(endpoint, "/missing")
                self.assertEqual(status, 404)
                self.assertEqual(content_type, "application/problem+json")
                self.assertEqual(json.loads(body)["code"], "not_found")
                status, content_type, headers, body = http_request(
                    endpoint, "/v1/version", method="DELETE"
                )
                self.assertEqual(status, 405)
                self.assertEqual(headers.get("Allow"), "GET, HEAD")
                self.assertEqual(json.loads(body)["status"], 405)

                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                self.assertFalse((root / "data" / "kwaque.pid").exists())
                # Component and shard identity prefix every broker log line.
                self.assertRegex(output, r"\[shard 0:\w+\] kwaque-broker - build ")
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
