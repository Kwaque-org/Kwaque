"""Writes the XXH3-64 vectors that pin a record key's routing point.

The routing point of a key is XXH3_64bits(key) with seed 0 and the default
secret. It is part of the protocol and must never change, so the values are
computed outside Kwaque's code, by the xxHash C library through the
python-xxhash binding, and checked in. `keys` writes one row per input:

  id, class, length, key, value

Classes that regenerate their bytes from the length write '-' as the key:
  sanity  xxHash's test buffer (tests/sanity_test.c fillTestBuffer)
  zero    0x00 repeated
  ff      0xFF repeated
  iota    byte i = i mod 256
Other classes write the key in hex: utf8 (UTF-8 text) and u64le/u64be
(an unsigned 64-bit integer in either byte order). Lengths cover every
XXH3 size path: 0, 1-3, 4-8, 9-16, 17-128, 129-240, then 64-byte stripes in
1,024-byte blocks, the 256-byte streaming buffer and powers of two to 1 MiB.

Run: python3 -I tools/routing_vectors.py keys <out.tsv>
(needs the python-xxhash package)
"""

import hashlib
import sys
from pathlib import Path

import xxhash

PRIME32 = 2654435761
PRIME64 = 11400714785074694797
MASK64 = (1 << 64) - 1
EMPTY = 0x2D06800538D394C2


def sanity(length: int) -> bytes:
    out = bytearray(length)
    generator = PRIME32
    for index in range(length):
        out[index] = generator >> 56
        generator = (generator * PRIME64) & MASK64
    return bytes(out)


def regenerate(kind: str, length: int) -> bytes:
    if kind == "sanity":
        return sanity(length)
    if kind == "zero":
        return bytes(length)
    if kind == "ff":
        return b"\xff" * length
    if kind == "iota":
        return bytes(index & 0xFF for index in range(length))
    raise ValueError(kind)


EDGES = sorted(
    {257, 383, 384, 385, 511, 512, 513, 959, 960, 961, 1023, 1024, 1025}
    | {1087, 1088, 1089}
    | {k * 1024 + d for k in (2, 4, 16, 64) for d in (-1, 0, 1)}
    | {(1 << p) + d for p in range(12, 21) for d in (-1, 0, 1)}
)

UTF8 = [
    "",
    "a",
    "user-42",
    "order:2026-10-29:000001",
    "ключ",
    "鍵",
    "🔑",
    "tenant/acme/orders/eu-west-1",
    "a" * 63,
    "a" * 64,
    "a" * 65,
    " ",
    "\u0000",
    "null",
    "NULL",
    "Ünïcödé-ascii-mix",
]

U64S = [
    0,
    1,
    2,
    255,
    256,
    65535,
    1 << 32,
    (1 << 63) - 1,
    1 << 63,
    MASK64,
    0x0123456789ABCDEF,
    42,
    1729,
    10**18,
    0xDEADBEEF,
    0x8000000000000001,
]


def value(key: bytes) -> int:
    return xxhash.xxh3_64_intdigest(key)


def key_rows():
    number = 0
    for kind in ("sanity", "zero", "ff", "iota"):
        for length in list(range(0, 257)) + EDGES:
            number += 1
            point = value(regenerate(kind, length))
            yield (f"v{number:05d}", kind, str(length), "-", f"{point:016x}")
    for text in UTF8:
        key = text.encode("utf-8")
        number += 1
        yield (
            f"v{number:05d}",
            "utf8",
            str(len(key)),
            key.hex() or "-",
            f"{value(key):016x}",
        )
    for integer in U64S:
        for order in ("little", "big"):
            key = integer.to_bytes(8, order)
            number += 1
            yield (
                f"v{number:05d}",
                "u64" + order[:1] + "e",
                "8",
                key.hex(),
                f"{value(key):016x}",
            )


def write_keys(out: Path) -> int:
    if value(b"") != EMPTY:
        raise SystemExit("the library's XXH3-64 of empty input is wrong")
    generator = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    body = ["\t".join(row) for row in key_rows()]
    header = [
        "# kwaque xxh3-64 vectors, format 1",
        "# value = XXH3_64bits(key), seed 0, default secret",
        f"# generator tools/routing_vectors.py sha256 {generator}",
        f"# reference python-xxhash {xxhash.VERSION} "
        f"(libxxhash {xxhash.XXHASH_VERSION})",
        "# columns: id class length key value",
    ]
    out.write_text("\n".join(header + body) + "\n")
    print(f"wrote {len(body)} rows to {out}")
    return 0


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[1] != "keys":
        raise SystemExit("usage: routing_vectors.py keys <out.tsv>")
    return write_keys(Path(sys.argv[2]))


if __name__ == "__main__":
    raise SystemExit(main())
