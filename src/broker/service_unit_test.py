"""Keep the reference systemd unit consistent with the broker's contracts."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


def unit_settings(text: str) -> dict[str, list[str]]:
    settings: dict[str, list[str]] = {}
    section = ""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        key, value = line.split("=", 1)
        settings.setdefault(f"{section}.{key}", []).append(value)
    return settings


class ServiceUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        unit, exit_codes, host_checks = (Path(value) for value in sys.argv[1:4])
        cls.settings = unit_settings(unit.read_text(encoding="utf-8"))
        cls.exit_codes = {
            name: int(value)
            for name, value in re.findall(
                r"^#define KWAQUE_EXIT_([A-Z_]+) (\d+)$",
                exit_codes.read_text(encoding="utf-8"),
                re.MULTILINE,
            )
        }
        match = re.search(
            r"recommended_descriptor_limit = ([0-9']+);",
            host_checks.read_text(encoding="utf-8"),
        )
        cls.recommended_descriptors = int(match.group(1).replace("'", ""))

    def setting(self, key: str) -> str:
        values = self.settings.get(key, [])
        self.assertEqual(len(values), 1, key)
        return values[0]

    def test_only_unexpected_failures_are_restarted(self) -> None:
        self.assertEqual(self.setting("Service.Restart"), "on-failure")
        prevented = {
            int(value)
            for value in self.setting("Service.RestartPreventExitStatus").split()
        }
        retried = {self.exit_codes["SUCCESS"], self.exit_codes["FAILURE"]}
        self.assertEqual(prevented, set(self.exit_codes.values()) - retried)

    def test_the_broker_reports_readiness_to_systemd(self) -> None:
        self.assertEqual(self.setting("Service.Type"), "notify")

    def test_it_starts_the_launcher_with_the_installed_configuration(self) -> None:
        command = self.setting("Service.ExecStart").split()
        self.assertEqual(command[0], "/opt/kwaque/bin/kwaque")
        self.assertEqual(command[1:3], ["--config", "/etc/kwaque/kwaque.yaml"])

    def test_limits_cover_the_host_checks(self) -> None:
        self.assertGreaterEqual(
            int(self.setting("Service.LimitNOFILE")), self.recommended_descriptors
        )
        self.assertEqual(self.setting("Service.LimitMEMLOCK"), "infinity")
        self.assertNotEqual(self.setting("Service.User"), "root")


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
