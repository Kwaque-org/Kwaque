from __future__ import annotations

import hashlib
import re
import sys
import tarfile
import unittest
from pathlib import Path, PurePosixPath


class PackageContentsTest(unittest.TestCase):
    def test_archive_has_the_exact_distribution_layout(self) -> None:
        archive = Path(sys.argv[1])
        self.assertTrue(archive.name.endswith(".tar.gz"))
        root = archive.name.removesuffix(".tar.gz")
        expected_files = {
            f"{root}/LICENSE",
            f"{root}/NOTICE",
            f"{root}/README.md",
            f"{root}/THIRD_PARTY.md",
            f"{root}/bin/kwaque",
            f"{root}/etc/kwaque/kwaque.yaml",
            f"{root}/lib/libcrypto.so.3",
            f"{root}/lib/libssl.so.3",
            f"{root}/licenses/abseil/LICENSE",
            f"{root}/licenses/boost/LICENSE_1_0.txt",
            f"{root}/licenses/c-ares/LICENSE.md",
            f"{root}/licenses/crc32c/LICENSE",
            f"{root}/licenses/fmt/LICENSE",
            f"{root}/licenses/hwloc/COPYING",
            f"{root}/licenses/liburing/LICENSE",
            f"{root}/licenses/lksctp-tools/COPYING.lib",
            f"{root}/licenses/lz4/LICENSE",
            f"{root}/licenses/openssl/LICENSE.txt",
            f"{root}/licenses/protobuf/LICENSE",
            f"{root}/licenses/seastar/LICENSE",
            f"{root}/licenses/seastar/NOTICE",
            f"{root}/licenses/unordered_dense/LICENSE",
            f"{root}/licenses/yaml-cpp/LICENSE",
        }

        with tarfile.open(archive, "r:gz") as package:
            members = package.getmembers()

        names = [member.name.rstrip("/") for member in members]
        self.assertEqual(len(names), len(set(names)))
        packaged_files = {
            member.name.rstrip("/") for member in members if not member.isdir()
        }
        self.assertEqual(packaged_files, expected_files)
        for member in members:
            path = PurePosixPath(member.name)
            self.assertFalse(path.is_absolute())
            self.assertNotIn("..", path.parts)
            self.assertEqual(member.uid, 0)
            self.assertEqual(member.gid, 0)
            self.assertEqual(member.mtime, 946684800)
            if member.isdir():
                self.assertEqual(member.mode & 0o777, 0o755)
            else:
                self.assertTrue(member.isfile())
                expected_mode = 0o755 if member.name == f"{root}/bin/kwaque" else 0o644
                self.assertEqual(member.mode & 0o777, expected_mode)

    def test_binaries_embed_no_absolute_build_paths(self) -> None:
        """Guard the reproducibility of the packaged binaries.

        A dependency built outside Bazel's own compile actions can record the
        absolute sandbox directory it was built in. That directory changes on
        every build, so the artifact stops being byte-reproducible and its
        published checksum stops being verifiable. Comparing two archives from
        one build cannot detect this, because both consume the same inputs.
        Asserting the absence of build paths can, and does so in one build.

        Sanitizer-instrumented members are exempt: the instrumentation records
        source locations on purpose so that reports are readable, and such a
        build is a development aid that is never distributed.
        """
        archive = Path(sys.argv[1])
        root = archive.name.removesuffix(".tar.gz")
        forbidden = (
            b"processwrapper-sandbox",
            b"/execroot/",
            b"/.cache/bazel/",
        )

        with tarfile.open(archive, "r:gz") as package:
            binaries = [
                member
                for member in package.getmembers()
                if member.isfile()
                and (
                    member.name.startswith(f"{root}/bin/")
                    or member.name.startswith(f"{root}/lib/")
                )
            ]
            self.assertNotEqual(binaries, [], "no packaged binaries were found")
            for member in binaries:
                extracted = package.extractfile(member)
                self.assertIsNotNone(extracted)
                assert extracted is not None
                contents = extracted.read()
                if b"__asan_init" in contents or b"__ubsan_handle" in contents:
                    continue
                for marker in forbidden:
                    if marker in contents:
                        self.fail(
                            f"{member.name} embeds the build path {marker!r}, "
                            "which changes between builds and breaks "
                            "reproducibility"
                        )

    def test_checksum_matches_archive(self) -> None:
        archive = Path(sys.argv[1])
        checksum_file = Path(sys.argv[2])
        match = re.fullmatch(
            r"([0-9a-f]{64})  ([^/\n]+)\n",
            checksum_file.read_text(encoding="ascii"),
        )
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.group(2), archive.name)
        self.assertEqual(
            match.group(1), hashlib.sha256(archive.read_bytes()).hexdigest()
        )


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
