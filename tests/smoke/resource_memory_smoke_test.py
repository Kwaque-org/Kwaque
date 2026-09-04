from __future__ import annotations

import re
import signal
import sys
import tempfile
import unittest
from pathlib import Path

from tests.smoke.broker_test_support import (
    BrokerProcess,
    assert_clean_shutdown,
    http_get,
    reserve_loopback_port,
    write_config,
)

TOTAL_MEMORY_BYTES = 128 * 1024 * 1024
REACTOR_HEADROOM_BYTES = 16 * 1024 * 1024
MINIMUM_SHARD_MEMORY_BYTES = 64 * 1024 * 1024
SYSTEM_ALLOCATOR_WARNING = (
    "Seastar compiled with default allocator, --memory option won't take effect"
)
CONFIGURED_MEMORY_METRIC = (
    "kwaque_resource_manager_memory_configured_bytes"
)


def reactor_arguments(shards: int) -> tuple[str, ...]:
    return (
        "--reactor-backend=epoll",
        f"--smp={shards}",
        "--memory=128M",
        "--overprovisioned",
    )


def configured_memory(exposition: str) -> int:
    values = []
    for line in exposition.splitlines():
        if line.startswith(
            (CONFIGURED_MEMORY_METRIC + "{", CONFIGURED_MEMORY_METRIC + " ")
        ):
            values.append(float(line.rsplit(maxsplit=1)[1]))
    if len(values) != 8:
        raise AssertionError(
            "expected one aggregated configured-memory sample per workload: "
            f"{values}"
        )
    total = sum(values)
    if not total.is_integer():
        raise AssertionError(f"configured memory was not integral: {values}")
    return int(total)


class ResourceMemorySmokeTest(unittest.TestCase):
    def test_native_memory_supports_one_and_two_shards(self) -> None:
        binary = Path(sys.argv[1])
        template = Path(sys.argv[2])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for shards in (1, 2):
                with self.subTest(shards=shards):
                    port = reserve_loopback_port()
                    data_directory = root / f"data-{shards}"
                    config = root / f"kwaque-{shards}.yaml"
                    write_config(template, config, data_directory, port)
                    broker = BrokerProcess(
                        binary,
                        config,
                        root / f"broker-{shards}.log",
                        reactor_arguments(shards),
                    )
                    try:
                        broker.wait_until_ready(port)
                        output = broker.output()
                        match = re.search(
                            r"runtime shards=(\d+) "
                            r"minimum_shard_memory_bytes=(\d+)",
                            output,
                        )
                        self.assertIsNotNone(match, output)
                        observed_shards = int(match.group(1))
                        observed_minimum = int(match.group(2))
                        expected_minimum = TOTAL_MEMORY_BYTES // shards
                        self.assertEqual(observed_shards, shards)
                        if SYSTEM_ALLOCATOR_WARNING in output:
                            self.assertNotEqual(observed_minimum, 0)
                        else:
                            self.assertEqual(observed_minimum, expected_minimum)
                        self.assertGreaterEqual(
                            observed_minimum, MINIMUM_SHARD_MEMORY_BYTES
                        )

                        status, content_type, metrics = http_get(port, "/metrics")
                        self.assertEqual(status, 200)
                        self.assertEqual(content_type, "text/plain")
                        self.assertEqual(
                            configured_memory(metrics),
                            shards
                            * (observed_minimum - REACTOR_HEADROOM_BYTES),
                        )

                        output = broker.stop(signal.SIGTERM)
                        assert_clean_shutdown(output)
                        self.assertFalse(
                            (data_directory / "kwaque.pid").exists()
                        )
                    finally:
                        broker.kill_if_running()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
