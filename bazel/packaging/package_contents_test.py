from __future__ import annotations

import hashlib
import re
import struct
import sys
import tarfile
import unittest
from pathlib import Path, PurePosixPath

EXECUTABLES = ("bin/iotune", "bin/kwaque", "bin/kwaque_native")
# The oldest glibc the packaged binaries may require; README states it.
GLIBC_FLOOR = (2, 34)
# Host libraries a packaged binary may load: the C runtime, its loader, and
# the unwinder. Everything else, including OpenSSL and the C++ runtime, is
# linked statically.
HOST_LIBRARIES = {
    62: {"libc.so.6", "libm.so.6", "libgcc_s.so.1", "ld-linux-x86-64.so.2"},
    183: {"libc.so.6", "libm.so.6", "libgcc_s.so.1", "ld-linux-aarch64.so.1"},
}
INTERPRETERS = {
    62: b"/lib64/ld-linux-x86-64.so.2\0",
    183: b"/lib/ld-linux-aarch64.so.1\0",
}


def elf_sections(contents: bytes) -> dict[str, bytes]:
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", contents)
    if header[0][:6] != b"\x7fELF\x02\x01":
        raise AssertionError("expected a little-endian ELF64 binary")
    section_offset, entry_size, count, names_index = (
        header[6],
        header[11],
        header[12],
        header[13],
    )
    entries = [
        struct.unpack_from("<IIQQQQIIQQ", contents, section_offset + i * entry_size)
        for i in range(count)
    ]
    names_section = entries[names_index]
    names = contents[names_section[4] : names_section[4] + names_section[5]]
    return {
        names[entry[0] : names.index(0, entry[0])].decode("ascii"): contents[
            entry[4] : entry[4] + entry[5]
        ]
        for entry in entries
        if entry[1] != 8  # SHT_NOBITS has no bytes in the ELF file.
    }


def dynamic_strings(sections: dict[str, bytes], tag: int) -> set[str]:
    strings = sections[".dynstr"]
    return {
        strings[value : strings.index(0, value)].decode("ascii")
        for entry, value in struct.iter_unpack("<qQ", sections[".dynamic"])
        if entry == tag
    }


def required_versions(sections: dict[str, bytes]) -> set[str]:
    """Return the symbol versions named in the Elf64_Verneed records."""
    strings = sections[".dynstr"]
    data = sections.get(".gnu.version_r", b"")
    versions = set()
    offset = 0
    while data:
        _, count, _, auxiliary, following = struct.unpack_from("<HHIII", data, offset)
        position = offset + auxiliary
        for _ in range(count):
            _, _, _, name, step = struct.unpack_from("<IHHII", data, position)
            versions.add(strings[name : strings.index(0, name)].decode("ascii"))
            position += step
        if following == 0:
            break
        offset += following
    return versions


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
            f"{root}/bin/iotune",
            f"{root}/bin/kwaque",
            f"{root}/bin/kwaque_native",
            f"{root}/etc/kwaque/kwaque.development.yaml",
            f"{root}/etc/kwaque/kwaque.yaml",
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
            f"{root}/licenses/utf8_range/LICENSE",
            f"{root}/licenses/yaml-cpp/LICENSE",
            f"{root}/licenses/zlib/LICENSE",
            f"{root}/share/kwaque/systemd/kwaque.service",
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
                expected_mode = (
                    0o755
                    if member.name in {f"{root}/{name}" for name in EXECUTABLES}
                    else 0o644
                )
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
                if member.isfile() and member.name.startswith(f"{root}/bin/")
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

    def test_binaries_load_only_the_host_c_runtime_and_old_enough_glibc(
        self,
    ) -> None:
        """Every packaged executable runs on any host with the documented glibc.

        Nothing is found through a run path or LD_LIBRARY_PATH: libraries
        other than the C runtime are linked in, so a host library with the
        same name can never replace them.
        """
        archive = Path(sys.argv[1])
        root = archive.name.removesuffix(".tar.gz")
        with tarfile.open(archive, "r:gz") as package:
            for name in EXECUTABLES:
                with self.subTest(binary=name):
                    member = package.extractfile(f"{root}/{name}")
                    assert member is not None
                    contents = member.read()
                    if b"__asan_init" in contents or b"__ubsan_handle" in contents:
                        continue  # Sanitizer builds are never distributed.
                    sections = elf_sections(contents)
                    machine = struct.unpack_from("<H", contents, 18)[0]
                    self.assertIn(machine, HOST_LIBRARIES)
                    self.assertEqual(sections.get(".interp"), INTERPRETERS[machine])
                    needed = dynamic_strings(sections, 1)  # DT_NEEDED
                    self.assertIn("libc.so.6", needed)
                    self.assertLessEqual(
                        needed,
                        HOST_LIBRARIES[machine],
                        f"unexpected libraries {sorted(needed - HOST_LIBRARIES[machine])}",
                    )
                    self.assertEqual(dynamic_strings(sections, 15), set())  # DT_RPATH
                    self.assertEqual(dynamic_strings(sections, 29), set())  # DT_RUNPATH
                    glibc = [
                        tuple(int(part) for part in version.split("_")[1].split("."))
                        for version in required_versions(sections)
                        if version.startswith("GLIBC_2.")
                    ]
                    self.assertNotEqual(glibc, [])
                    self.assertLessEqual(max(glibc), GLIBC_FLOOR)

    def test_reference_systemd_unit_starts_the_installed_configuration(self) -> None:
        archive = Path(sys.argv[1])
        root = archive.name.removesuffix(".tar.gz")
        with tarfile.open(archive, "r:gz") as package:
            member = package.extractfile(f"{root}/share/kwaque/systemd/kwaque.service")
            assert member is not None
            unit = member.read().decode("utf-8")
        self.assertRegex(
            unit,
            r"(?m)^ExecStart=/opt/kwaque/bin/kwaque --config /etc/kwaque/kwaque.yaml ",
        )

    def test_installed_configuration_is_the_production_example(self) -> None:
        archive = Path(sys.argv[1])
        root = archive.name.removesuffix(".tar.gz")
        with tarfile.open(archive, "r:gz") as package:

            def text(name: str) -> str:
                member = package.extractfile(f"{root}/etc/kwaque/{name}")
                assert member is not None
                return member.read().decode("utf-8")

            production = text("kwaque.yaml")
            development = text("kwaque.development.yaml")
        self.assertRegex(production, r"(?m)^\s*developer_mode: false$")
        self.assertRegex(production, r'(?m)^\s*data_directory: "/')
        self.assertRegex(development, r"(?m)^\s*developer_mode: true$")

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
