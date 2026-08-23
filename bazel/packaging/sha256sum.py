#!/usr/bin/env python3
"""Write a SHA-256 checksum file for one package artifact."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def main() -> None:
    source = Path(sys.argv[1])
    output = Path(sys.argv[2])
    output.write_text(f"{digest(source)}  {source.name}\n", encoding="ascii")


if __name__ == "__main__":
    main()
