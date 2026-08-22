from __future__ import annotations

import signal
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    reserve_loopback_port,
    write_config,
)


class PackageSmokeTest(unittest.TestCase):
    def test_extracted_broker_starts_and_stops(self) -> None:
        archive = Path(sys.argv[1])
        package_name = archive.name.removesuffix(".tar.gz")
        with tempfile.TemporaryDirectory() as directory:
            temporary_root = Path(directory)
            with tarfile.open(archive, "r:gz") as package:
                package.extractall(temporary_root, filter="data")

            package_root = temporary_root / package_name
            binary = package_root / "bin" / "kwaque"
            template = package_root / "etc" / "kwaque" / "kwaque.yaml"
            port = reserve_loopback_port()
            config = temporary_root / "kwaque.yaml"
            write_config(template, config, temporary_root / "data", port)

            broker = BrokerProcess(binary, config, temporary_root / "broker.log")
            try:
                broker.wait_until_ready(port)
                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
