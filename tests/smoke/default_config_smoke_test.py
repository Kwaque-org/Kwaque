from __future__ import annotations

import shutil
import signal
import sys
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    log_path,
    test_directory,
    wait_for_loopback_endpoint,
)


class DefaultConfigSmokeTest(unittest.TestCase):
    def test_committed_example_starts_without_extra_flags(self) -> None:
        """The development example runs as committed, found by default path.

        Only the reactor options a test needs are passed; there is no
        --config. The example's relative data directory resolves against the
        working directory, and the broker logs the absolute paths it used.
        """
        binary = Path(sys.argv[1])
        example = Path(sys.argv[2])
        # The example names one fixed endpoint, which no other test may use
        # while this broker runs.
        endpoint = wait_for_loopback_endpoint("127.0.0.1")
        with test_directory() as directory:
            root = Path(directory).resolve()
            (root / "conf").mkdir()
            shutil.copyfile(example, root / "conf" / "kwaque.yaml")
            broker = BrokerProcess(
                binary, None, log_path(root, "default-config.log"), cwd=root
            )
            try:
                # This broker's own listener: a process outside the tests
                # could answer on the endpoint in its place.
                broker.wait_for(
                    "startup stage=admin state=ready"
                    f" address={endpoint.address} port={endpoint.port}"
                )
                broker.wait_until_ready(endpoint)
                self.assertTrue((root / "data" / "kwaque.pid").exists())
                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                self.assertIn(f"path={root / 'conf' / 'kwaque.yaml'}", output)
                # Resolution is not normalization: "." is kept, and ".." is
                # never removed lexically because a symlink can make it differ.
                self.assertIn(f"data_directory={root}/./data", output)
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
