"""Check independently specified format bytes, fields and integrity projections."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

_CRC_TABLE = (
    0x00000000,
    0xF26B8303,
    0xE13B70F7,
    0x1350F3F4,
    0xC79A971F,
    0x35F1141C,
    0x26A1E7E8,
    0xD4CA64EB,
    0x8AD958CF,
    0x78B2DBCC,
    0x6BE22838,
    0x9989AB3B,
    0x4D43CFD0,
    0xBF284CD3,
    0xAC78BF27,
    0x5E133C24,
    0x105EC76F,
    0xE235446C,
    0xF165B798,
    0x030E349B,
    0xD7C45070,
    0x25AFD373,
    0x36FF2087,
    0xC494A384,
    0x9A879FA0,
    0x68EC1CA3,
    0x7BBCEF57,
    0x89D76C54,
    0x5D1D08BF,
    0xAF768BBC,
    0xBC267848,
    0x4E4DFB4B,
    0x20BD8EDE,
    0xD2D60DDD,
    0xC186FE29,
    0x33ED7D2A,
    0xE72719C1,
    0x154C9AC2,
    0x061C6936,
    0xF477EA35,
    0xAA64D611,
    0x580F5512,
    0x4B5FA6E6,
    0xB93425E5,
    0x6DFE410E,
    0x9F95C20D,
    0x8CC531F9,
    0x7EAEB2FA,
    0x30E349B1,
    0xC288CAB2,
    0xD1D83946,
    0x23B3BA45,
    0xF779DEAE,
    0x05125DAD,
    0x1642AE59,
    0xE4292D5A,
    0xBA3A117E,
    0x4851927D,
    0x5B016189,
    0xA96AE28A,
    0x7DA08661,
    0x8FCB0562,
    0x9C9BF696,
    0x6EF07595,
    0x417B1DBC,
    0xB3109EBF,
    0xA0406D4B,
    0x522BEE48,
    0x86E18AA3,
    0x748A09A0,
    0x67DAFA54,
    0x95B17957,
    0xCBA24573,
    0x39C9C670,
    0x2A993584,
    0xD8F2B687,
    0x0C38D26C,
    0xFE53516F,
    0xED03A29B,
    0x1F682198,
    0x5125DAD3,
    0xA34E59D0,
    0xB01EAA24,
    0x42752927,
    0x96BF4DCC,
    0x64D4CECF,
    0x77843D3B,
    0x85EFBE38,
    0xDBFC821C,
    0x2997011F,
    0x3AC7F2EB,
    0xC8AC71E8,
    0x1C661503,
    0xEE0D9600,
    0xFD5D65F4,
    0x0F36E6F7,
    0x61C69362,
    0x93AD1061,
    0x80FDE395,
    0x72966096,
    0xA65C047D,
    0x5437877E,
    0x4767748A,
    0xB50CF789,
    0xEB1FCBAD,
    0x197448AE,
    0x0A24BB5A,
    0xF84F3859,
    0x2C855CB2,
    0xDEEEDFB1,
    0xCDBE2C45,
    0x3FD5AF46,
    0x7198540D,
    0x83F3D70E,
    0x90A324FA,
    0x62C8A7F9,
    0xB602C312,
    0x44694011,
    0x5739B3E5,
    0xA55230E6,
    0xFB410CC2,
    0x092A8FC1,
    0x1A7A7C35,
    0xE811FF36,
    0x3CDB9BDD,
    0xCEB018DE,
    0xDDE0EB2A,
    0x2F8B6829,
    0x82F63B78,
    0x709DB87B,
    0x63CD4B8F,
    0x91A6C88C,
    0x456CAC67,
    0xB7072F64,
    0xA457DC90,
    0x563C5F93,
    0x082F63B7,
    0xFA44E0B4,
    0xE9141340,
    0x1B7F9043,
    0xCFB5F4A8,
    0x3DDE77AB,
    0x2E8E845F,
    0xDCE5075C,
    0x92A8FC17,
    0x60C37F14,
    0x73938CE0,
    0x81F80FE3,
    0x55326B08,
    0xA759E80B,
    0xB4091BFF,
    0x466298FC,
    0x1871A4D8,
    0xEA1A27DB,
    0xF94AD42F,
    0x0B21572C,
    0xDFEB33C7,
    0x2D80B0C4,
    0x3ED04330,
    0xCCBBC033,
    0xA24BB5A6,
    0x502036A5,
    0x4370C551,
    0xB11B4652,
    0x65D122B9,
    0x97BAA1BA,
    0x84EA524E,
    0x7681D14D,
    0x2892ED69,
    0xDAF96E6A,
    0xC9A99D9E,
    0x3BC21E9D,
    0xEF087A76,
    0x1D63F975,
    0x0E330A81,
    0xFC588982,
    0xB21572C9,
    0x407EF1CA,
    0x532E023E,
    0xA145813D,
    0x758FE5D6,
    0x87E466D5,
    0x94B49521,
    0x66DF1622,
    0x38CC2A06,
    0xCAA7A905,
    0xD9F75AF1,
    0x2B9CD9F2,
    0xFF56BD19,
    0x0D3D3E1A,
    0x1E6DCDEE,
    0xEC064EED,
    0xC38D26C4,
    0x31E6A5C7,
    0x22B65633,
    0xD0DDD530,
    0x0417B1DB,
    0xF67C32D8,
    0xE52CC12C,
    0x1747422F,
    0x49547E0B,
    0xBB3FFD08,
    0xA86F0EFC,
    0x5A048DFF,
    0x8ECEE914,
    0x7CA56A17,
    0x6FF599E3,
    0x9D9E1AE0,
    0xD3D3E1AB,
    0x21B862A8,
    0x32E8915C,
    0xC083125F,
    0x144976B4,
    0xE622F5B7,
    0xF5720643,
    0x07198540,
    0x590AB964,
    0xAB613A67,
    0xB831C993,
    0x4A5A4A90,
    0x9E902E7B,
    0x6CFBAD78,
    0x7FAB5E8C,
    0x8DC0DD8F,
    0xE330A81A,
    0x115B2B19,
    0x020BD8ED,
    0xF0605BEE,
    0x24AA3F05,
    0xD6C1BC06,
    0xC5914FF2,
    0x37FACCF1,
    0x69E9F0D5,
    0x9B8273D6,
    0x88D28022,
    0x7AB90321,
    0xAE7367CA,
    0x5C18E4C9,
    0x4F48173D,
    0xBD23943E,
    0xF36E6F75,
    0x0105EC76,
    0x12551F82,
    0xE03E9C81,
    0x34F4F86A,
    0xC69F7B69,
    0xD5CF889D,
    0x27A40B9E,
    0x79B737BA,
    0x8BDCB4B9,
    0x988C474D,
    0x6AE7C44E,
    0xBE2DA0A5,
    0x4C4623A6,
    0x5F16D052,
    0xAD7D5351,
)

MAX_FILE_BYTES = 131072
MAX_FIXTURES = 64
DEFAULT_MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "src/codec/tests/testdata/formats/manifest.json"
)


class FixtureError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise FixtureError(message)


def crc32c(data: bytes, seed: int = 0) -> int:
    value = seed ^ 0xFFFFFFFF
    for octet in data:
        value = _CRC_TABLE[(value & 0xFF) ^ octet] ^ (value >> 8)
    return value ^ 0xFFFFFFFF


def xxh32_short(data: bytes) -> int:
    """Seed-zero short-input leaf used by the literal raw LZ4 fixture."""
    require(len(data) < 16, "XXH32 fixture projection must be shorter than 16 bytes")

    def rotate(value, bits):
        value &= 0xFFFFFFFF
        return ((value << bits) | (value >> (32 - bits))) & 0xFFFFFFFF

    value = 374761393 + len(data)
    offset = 0
    while offset + 4 <= len(data):
        value += int.from_bytes(data[offset : offset + 4], "little") * 3266489917
        value = rotate(value, 17) * 668265263
        offset += 4
    for octet in data[offset:]:
        value = rotate(value + octet * 374761393, 11) * 2654435761
    value &= 0xFFFFFFFF
    value ^= value >> 15
    value = (value * 2246822519) & 0xFFFFFFFF
    value ^= value >> 13
    value = (value * 3266489917) & 0xFFFFFFFF
    return value ^ (value >> 16)


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def keys(value: object, required: set[str], optional: set[str] = frozenset()) -> None:
    require(isinstance(value, dict), "expected an object")
    require(required <= value.keys(), f"missing keys: {required - value.keys()}")
    require(value.keys() <= required | optional, "unknown manifest key")


def integer(value: object, maximum: int = MAX_FILE_BYTES) -> int:
    require(type(value) is int and 0 <= value <= maximum, "invalid unsigned integer")
    return value


def octets(value: object) -> bytes:
    require(isinstance(value, str), "expected hexadecimal text")
    require(len(value) <= 2 * MAX_FILE_BYTES, "hexadecimal value too large")
    require(re.fullmatch(r"(?:[0-9a-f]{2})*", value) is not None, "invalid hex")
    return bytes.fromhex(value)


def extent(value: object, size: int) -> tuple[int, int]:
    require(isinstance(value, list) and len(value) == 2, "expected [offset, length]")
    offset, length = (integer(item) for item in value)
    require(offset <= size and length <= size - offset, "extent exceeds owner")
    return offset, offset + length


def fields_match(
    fields: object, wire: bytes, fixtures: dict[str, bytes], depth: int = 0
) -> None:
    require(isinstance(fields, list) and 0 < len(fields) <= 256, "invalid field list")
    require(depth <= 8, "field nesting too deep")
    position = 0
    names = set()
    for field in fields:
        keys(
            field,
            {"name", "offset", "width"},
            {"le", "varint", "zigzag", "hex", "zero", "fields", "fixture"},
        )
        name = field["name"]
        require(isinstance(name, str) and 0 < len(name) <= 96, "invalid field name")
        require(name not in names, f"duplicate field: {name}")
        names.add(name)
        start, end = extent([field["offset"], field["width"]], len(wire))
        require(start == position and end > start, f"gap/overlap/empty field: {name}")
        position = end
        actual = wire[start:end]
        modes = field.keys() & {
            "le",
            "varint",
            "zigzag",
            "hex",
            "zero",
            "fields",
            "fixture",
        }
        require(len(modes) == 1, f"field needs exactly one representation: {name}")
        if "le" in field:
            require(len(actual) in (1, 2, 4, 8), "integer width must be 1/2/4/8")
            value = integer(field["le"], (1 << (8 * len(actual))) - 1)
            require(
                int.from_bytes(actual, "little") == value, f"integer mismatch: {name}"
            )
        elif "varint" in field or "zigzag" in field:
            require(len(actual) <= 10, "varint field too wide")
            require(
                all(b & 128 for b in actual[:-1]) and actual[-1] < 128,
                "invalid varint field",
            )
            value = sum((b & 127) << (7 * i) for i, b in enumerate(actual))
            integer(value, (1 << 64) - 1)
            if "zigzag" in field:
                expected = field["zigzag"]
                require(
                    type(expected) is int and -(1 << 63) <= expected < (1 << 63),
                    "invalid signed integer",
                )
                value = (value >> 1) ^ -(value & 1)
            else:
                expected = integer(field["varint"], (1 << 64) - 1)
            require(value == expected, f"varint mismatch: {name}")
        elif "hex" in field:
            require(actual == octets(field["hex"]), f"byte mismatch: {name}")
        elif "zero" in field:
            require(
                field["zero"] is True and not any(actual), f"nonzero padding: {name}"
            )
        elif "fixture" in field:
            reference = field["fixture"]
            require(
                isinstance(reference, str) and reference in fixtures,
                "unknown child fixture",
            )
            require(actual == fixtures[reference], f"child mismatch: {name}")
        else:
            fields_match(field["fields"], actual, fixtures, depth + 1)
    require(position == len(wire), "undeclared trailing bytes")


def projection(parts: object, fixtures: dict[str, bytes]) -> bytes:
    require(isinstance(parts, list) and 0 < len(parts) <= 256, "invalid projection")
    result = bytearray()
    for part in parts:
        require(isinstance(part, dict), "invalid projection part")
        if "hex" in part:
            keys(part, {"hex"})
            value = octets(part["hex"])
        else:
            keys(part, {"fixture", "range"}, {"zero"})
            name = part["fixture"]
            require(
                isinstance(name, str) and name in fixtures, "unknown projection fixture"
            )
            begin, end = extent(part["range"], len(fixtures[name]))
            value = bytearray(fixtures[name][begin:end])
            zeros = part.get("zero", [])
            require(
                isinstance(zeros, list) and len(zeros) <= 16, "invalid checksum masks"
            )
            previous = 0
            for masked in zeros:
                first, last = extent(masked, len(value))
                require(
                    first >= previous and last > first,
                    "overlapping/empty checksum mask",
                )
                value[first:last] = bytes(last - first)
                previous = last
        require(len(result) + len(value) <= MAX_FILE_BYTES, "projection too large")
        result.extend(value)
    return bytes(result)


def validate(document: object, fixtures: dict[str, bytes]) -> None:
    keys(document, {"version", "fixtures", "checks"})
    require(
        type(document["version"]) is int and document["version"] == 1,
        "unsupported manifest version",
    )
    entries = document["fixtures"]
    require(
        isinstance(entries, list) and 0 < len(entries) <= MAX_FIXTURES,
        "invalid fixture list",
    )
    names, paths = set(), set()
    for entry in entries:
        keys(entry, {"name", "path", "size", "fields"})
        name, path = entry["name"], entry["path"]
        require(
            isinstance(name, str)
            and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) is not None,
            "invalid fixture name",
        )
        require(
            isinstance(path, str)
            and re.fullmatch(r"[a-z][a-z0-9_]{0,63}\.hex", path) is not None,
            "invalid fixture path",
        )
        require(name not in names and path not in paths, "duplicate fixture")
        names.add(name)
        paths.add(path)
        require(name in fixtures, f"missing fixture: {name}")
        require(len(fixtures[name]) == integer(entry["size"]), f"size mismatch: {name}")
        fields_match(entry["fields"], fixtures[name], fixtures)
    require(names == fixtures.keys(), "unexpected fixture data")

    # Child aliases must eventually resolve to declared fields, not form a
    # cycle of byte-equal fixtures that never describes the underlying layout.
    def children(fields):
        for field in fields:
            if "fixture" in field:
                yield field["fixture"]
            elif "fields" in field:
                yield from children(field["fields"])

    graph = {entry["name"]: set(children(entry["fields"])) for entry in entries}
    active, done = set(), set()

    def visit(name):
        require(name not in active, "cyclic fixture containment")
        if name in done:
            return
        active.add(name)
        for child in graph[name]:
            visit(child)
        active.remove(name)
        done.add(name)

    for name in graph:
        visit(name)
    checks = document["checks"]
    require(isinstance(checks, list) and len(checks) <= 256, "invalid check list")
    check_names = set()
    for check in checks:
        keys(check, {"name", "algorithm", "parts", "expected"}, {"stored"})
        name = check["name"]
        require(
            isinstance(name, str) and 0 < len(name) <= 96 and name not in check_names,
            "invalid/duplicate check name",
        )
        check_names.add(name)
        value = projection(check["parts"], fixtures)
        if check["algorithm"] == "crc32c":
            actual = crc32c(value).to_bytes(4, "little")
        elif check["algorithm"] == "xxh32_short":
            actual = xxh32_short(value).to_bytes(4, "little")
        elif check["algorithm"] == "lz4_descriptor":
            actual = bytes([(xxh32_short(value) >> 8) & 0xFF])
        else:
            require(check["algorithm"] == "sha256", "unknown digest algorithm")
            actual = hashlib.sha256(value).digest()
        require(actual == octets(check["expected"]), f"digest mismatch: {name}")
        if "stored" in check:
            target = check["stored"]
            keys(target, {"fixture", "offset"})
            require(
                isinstance(target["fixture"], str) and target["fixture"] in fixtures,
                "unknown digest target",
            )
            wire = fixtures[target["fixture"]]
            begin, end = extent([target["offset"], len(actual)], len(wire))
            require(wire[begin:end] == actual, f"stored digest mismatch: {name}")


def verify(manifest: Path) -> int:
    require(manifest.stat().st_size <= 2 * 1024 * 1024, "manifest too large")
    document = json.loads(manifest.read_text(), object_pairs_hook=unique_object)
    keys(document, {"version", "fixtures", "checks"})
    entries = document["fixtures"]
    require(
        isinstance(entries, list) and 0 < len(entries) <= MAX_FIXTURES,
        "invalid fixture list",
    )
    fixtures = {}
    for entry in entries:
        keys(entry, {"name", "path", "size", "fields"})
        path = entry["path"]
        require(
            isinstance(path, str)
            and re.fullmatch(r"[a-z][a-z0-9_]{0,63}\.hex", path) is not None,
            "invalid fixture path",
        )
        name = entry["name"]
        require(
            isinstance(name, str) and name not in fixtures,
            "duplicate/invalid fixture name",
        )
        file = manifest.parent / path
        require(file.stat().st_size <= 3 * MAX_FILE_BYTES, "fixture file too large")
        fixtures[name] = b"".join(octets(token) for token in file.read_text().split())
    validate(document, fixtures)
    return len(fixtures)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    try:
        count = verify(args.manifest)
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.exit(1, f"format fixtures: {error}\n")
    print(f"Verified {count} format fixtures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
