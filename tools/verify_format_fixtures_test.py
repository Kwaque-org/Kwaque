from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

try:
    from tools import verify_format_fixtures as verifier
except ModuleNotFoundError:
    import verify_format_fixtures as verifier


class FormatFixtureTest(unittest.TestCase):
    def setUp(self):
        self.document = json.loads(verifier.DEFAULT_MANIFEST.read_text())
        self.fixtures = {
            entry["name"]: bytes.fromhex(
                (verifier.DEFAULT_MANIFEST.parent / entry["path"]).read_text()
            )
            for entry in self.document["fixtures"]
        }

    def test_all_checked_in_shapes_and_projections(self):
        self.assertEqual(verifier.verify(verifier.DEFAULT_MANIFEST), 31)
        self.assertEqual(
            {name for name in self.fixtures if name.startswith("frame_")},
            {
                "frame_request",
                "frame_response",
                "frame_redirect",
                "frame_error",
                "frame_submitted",
                "frame_assigned",
            },
        )
        for name in ("retry", "index", "manifest"):
            self.assertIn(name + "_page", self.fixtures)
        for name in (
            "sealed_footer",
            "index_root",
            "manifest_root",
            "checkpoint_extended",
            "record_rich",
            "assigned_sparse",
            "lz4_raw_abc",
        ):
            self.assertIn(name, self.fixtures)

    def test_crc_known_answers_and_slicing(self):
        for data, value in [
            (bytes(32), 0x8A9136AA),
            (bytes([255]) * 32, 0x62A8AB43),
            (bytes(range(32)), 0x46DD794E),
            (bytes(reversed(range(32))), 0x113FDB5C),
            (b"123456789", 0xE3069283),
            (b"", 0),
        ]:
            self.assertEqual(verifier.crc32c(data), value)
            for cut in range(len(data) + 1):
                self.assertEqual(
                    verifier.crc32c(data[cut:], verifier.crc32c(data[:cut])), value
                )

    def test_crc_packet_known_answer_and_three_part_slicing(self):
        data = bytes.fromhex(
            "01c000000000000000000000"
            "000000001400000000000400"
            "000000140000001828000000"
            "000000000200000000000000"
        )
        self.assertEqual(verifier.crc32c(data), 0xD9963A56)
        for first in range(len(data)):
            for second in range(first + 1, len(data) + 1):
                crc = 0
                if first > 0:
                    crc = verifier.crc32c(data[:first], crc)
                crc = verifier.crc32c(data[first:second], crc)
                if second < len(data):
                    crc = verifier.crc32c(data[second:], crc)
                self.assertEqual(crc, 0xD9963A56)

    def test_crc_continuation_matches_concatenation(self):
        self.assertEqual(
            verifier.crc32c(b"hello world"),
            verifier.crc32c(b"world", verifier.crc32c(b"hello ")),
        )

    def test_literal_lz4_checksums_use_the_short_native_hash_domain(self):
        self.assertEqual(verifier.xxh32_short(b""), 0x02CC5D05)
        self.assertEqual(verifier.xxh32_short(b"abc"), 0x32D153FF)
        header = self.fixtures["lz4_raw_abc"][4:14]
        self.assertEqual((verifier.xxh32_short(header) >> 8) & 255, 0x74)
        with self.assertRaises(verifier.FixtureError):
            verifier.xxh32_short(bytes(16))

    def test_every_fixture_detects_byte_corruption(self):
        for name, wire in self.fixtures.items():
            for offset in {0, len(wire) // 2, len(wire) - 1}:
                with self.subTest(name=name, offset=offset):
                    mutated = dict(self.fixtures)
                    bad = bytearray(wire)
                    bad[offset] ^= 1
                    mutated[name] = bytes(bad)
                    with self.assertRaises(verifier.FixtureError):
                        verifier.validate(self.document, mutated)

    def test_manifest_does_not_accept_ambiguous_fields(self):
        for mutation in (
            "gap",
            "overlap",
            "outside",
            "trailing",
            "duplicate",
            "modes",
            "width",
            "bool",
            "unknown",
        ):
            document = copy.deepcopy(self.document)
            fields = document["fixtures"][0]["fields"]
            if mutation == "gap":
                fields[0]["offset"] = 1
            elif mutation == "overlap":
                fields[1]["offset"] = 0
            elif mutation == "outside":
                fields[0]["width"] = 9999
            elif mutation == "trailing":
                fields.pop()
            elif mutation == "duplicate":
                fields[1]["name"] = fields[0]["name"]
            elif mutation == "modes":
                fields[0]["hex"] = "06"
            elif mutation == "width":
                fields[0]["width"] = 3
            elif mutation == "bool":
                fields[0]["le"] = True
            else:
                fields[0]["surprise"] = 1
            with (
                self.subTest(mutation=mutation),
                self.assertRaises(verifier.FixtureError),
            ):
                verifier.validate(document, self.fixtures)

    def test_manifest_identity_and_file_extents(self):
        for mutation in ("version", "size", "name", "path", "duplicate"):
            document = copy.deepcopy(self.document)
            entry = document["fixtures"][0]
            if mutation == "version":
                document["version"] = 2
            elif mutation == "size":
                entry["size"] += 1
            elif mutation == "name":
                entry["name"] = "absent"
            elif mutation == "path":
                entry["path"] = "../escape.hex"
            else:
                document["fixtures"].append(copy.deepcopy(entry))
            with (
                self.subTest(mutation=mutation),
                self.assertRaises(verifier.FixtureError),
            ):
                verifier.validate(document, self.fixtures)

    def test_child_fixtures_must_resolve_to_independent_fields(self):
        for child in ("record", "absent"):
            document = copy.deepcopy(self.document)
            entry = document["fixtures"][0]
            entry["fields"] = [
                {"name": "child", "offset": 0, "width": entry["size"], "fixture": child}
            ]
            with self.subTest(child=child), self.assertRaises(verifier.FixtureError):
                verifier.validate(document, self.fixtures)

    def test_varint_fields_keep_full_width_and_signed_domains(self):
        def field(wire, **value):
            verifier.fields_match(
                [{"name": "integer", "offset": 0, "width": len(wire), **value}],
                wire,
                {},
            )

        field(b"\x81\x00", varint=1)
        field(b"\xff" * 9 + b"\x01", varint=(1 << 64) - 1)
        field(b"\x01", zigzag=-1)
        field(b"\xff" * 9 + b"\x01", zigzag=-(1 << 63))
        for wire in (
            b"\x80",
            b"\x00\x01",
            b"\xff" * 9 + b"\x02",
            b"\x80" * 10 + b"\x00",
        ):
            with self.subTest(wire=wire), self.assertRaises(verifier.FixtureError):
                field(wire, varint=0)

    def test_changed_hash_preimage_and_crc_masks_reject(self):
        for name in (
            "submitted_semantic",
            "checkpoint_semantic",
            "segment_block_header_crc",
        ):
            document = copy.deepcopy(self.document)
            check = next(item for item in document["checks"] if item["name"] == name)
            if "header_crc" in name:
                check["parts"][0]["zero"] = [[24, 4]]
            else:
                check["parts"][0]["hex"] += "00"
            with self.subTest(name=name), self.assertRaises(verifier.FixtureError):
                verifier.validate(document, self.fixtures)

    def test_stored_digest_and_projection_ranges_are_checked(self):
        for mutation in (
            "expected",
            "offset",
            "range",
            "reference",
            "algorithm",
            "duplicate",
        ):
            document = copy.deepcopy(self.document)
            check = document["checks"][0]
            if mutation == "expected":
                check["expected"] = "00" * 4
            elif mutation == "offset":
                check["stored"]["offset"] = 131072
            elif mutation == "range":
                check["parts"][0]["range"] = [0, 131072]
            elif mutation == "reference":
                check["parts"][0]["fixture"] = "absent"
            elif mutation == "algorithm":
                check["algorithm"] = "crc32"
            else:
                document["checks"].append(copy.deepcopy(check))
            with (
                self.subTest(mutation=mutation),
                self.assertRaises(verifier.FixtureError),
            ):
                verifier.validate(document, self.fixtures)

    def test_json_duplicates_and_file_hex_reject(self):
        with self.assertRaises(verifier.FixtureError):
            json.loads(
                '{"version":1,"version":1}', object_pairs_hook=verifier.unique_object
            )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            document = {
                "version": 1,
                "fixtures": [
                    {
                        "name": "small",
                        "path": "small.hex",
                        "size": 1,
                        "fields": [{"name": "value", "offset": 0, "width": 1, "le": 1}],
                    }
                ],
                "checks": [],
            }
            (path / "manifest.json").write_text(json.dumps(document))
            for value in (
                "0",
                "gg",
                "01zz",
                "0 1",
                "00" * (verifier.MAX_FILE_BYTES + 1),
            ):
                (path / "small.hex").write_text(value)
                with (
                    self.subTest(value=value[:8]),
                    self.assertRaises(verifier.FixtureError),
                ):
                    verifier.verify(path / "manifest.json")

    def test_sha_domain_terminator_and_exact_objects_differ(self):
        body = self.fixtures["checkpoint"][32:]
        self.assertEqual(
            hashlib.sha256(b"KQ/CHECKPOINT/1\0" + body).digest(),
            self.fixtures["checkpoint_digest"],
        )
        self.assertNotEqual(
            hashlib.sha256(b"KQ/CHECKPOINT/1" + body).digest(),
            self.fixtures["checkpoint_digest"],
        )
        self.assertNotEqual(
            hashlib.sha256(self.fixtures["checkpoint"]).digest(),
            hashlib.sha256(self.fixtures["checkpoint_extended"]).digest(),
        )


if __name__ == "__main__":
    unittest.main()
