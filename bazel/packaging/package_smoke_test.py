from __future__ import annotations

import os
import signal
import sys
import tarfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    REACTOR_BACKEND,
    BrokerProcess,
    assert_clean_shutdown,
    lease_loopback_endpoint,
    log_path,
    test_directory,
    write_config,
)

# The production profile needs the native allocator and more memory per shard
# than its suitability floor.
PRODUCTION_ARGUMENTS = (
    f"--reactor-backend={REACTOR_BACKEND}",
    "--smp=2",
    "--memory=384M",
    "--overprovisioned",
)


class PackageSmokeTest(unittest.TestCase):
    def test_extracted_broker_starts_and_stops(self) -> None:
        archive = Path(sys.argv[1])
        package_name = archive.name.removesuffix(".tar.gz")
        with test_directory() as directory:
            temporary_root = Path(directory)
            with tarfile.open(archive, "r:gz") as package:
                package.extractall(temporary_root, filter="data")

            package_root = temporary_root / package_name
            binary = package_root / "bin" / "kwaque"
            # Start the installed production configuration wherever the build
            # can run it; a system-allocator build refuses the production
            # profile and starts the development example instead.
            native = os.environ.get("KWAQUE_EXPECT_TEST_ALLOCATOR") == "native"
            template = (
                package_root / "etc" / "kwaque" / "kwaque.yaml"
                if native
                else package_root / "etc" / "kwaque" / "kwaque.development.yaml"
            )
            endpoint = lease_loopback_endpoint()
            config = temporary_root / "kwaque.yaml"
            # Only the data directory and the listener change.
            write_config(template, config, temporary_root / "data", endpoint)

            broker = BrokerProcess(
                binary,
                config,
                log_path(temporary_root, "package-broker.log"),
                **({"reactor_arguments": PRODUCTION_ARGUMENTS} if native else {}),
            )
            try:
                broker.wait_until_ready(endpoint)
                output = broker.stop(signal.SIGTERM)
                assert_clean_shutdown(output)
                profile = "production" if native else "development"
                self.assertIn(f"profile={profile}", output)
            finally:
                broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
