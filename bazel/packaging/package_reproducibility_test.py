from __future__ import annotations

import hashlib
import sys
import unittest
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as archive:
        while chunk := archive.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class PackageReproducibilityTest(unittest.TestCase):
    def test_independent_package_actions_are_byte_identical(self) -> None:
        first = Path(sys.argv[1])
        second = Path(sys.argv[2])

        self.assertEqual(
            sha256(first),
            sha256(second),
            "independent package actions produced different archives",
        )


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
