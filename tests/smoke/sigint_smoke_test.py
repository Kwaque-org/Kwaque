from __future__ import annotations

import signal
import sys
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    reserve_loopback_port,
    write_config,
)


class SigintSmokeTest(unittest.TestCase):
    def test_sigint_uses_clean_shutdown_path(self) -> None:
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
                output = broker.stop(signal.SIGINT)
                assert_clean_shutdown(output)
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
