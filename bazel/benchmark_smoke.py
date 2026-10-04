"""Run a benchmark binary as a test with the given arguments."""

from __future__ import annotations

import os
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: benchmark_smoke.py BINARY [ARGUMENT...]", file=sys.stderr)
        return 2
    binary = sys.argv[1]
    os.execv(binary, [binary, *sys.argv[2:]])
    return 1


if __name__ == "__main__":
    sys.exit(main())
