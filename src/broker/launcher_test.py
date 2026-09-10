"""Exercise CPU rejection before exec without executing optional instructions."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

X86_REQUIRED = sum(1 << bit for bit in (0, 1, 9, 13, 19, 20, 23, 32))
ARM_REQUIRED = sum(1 << bit for bit in (0, 1, 3, 4, 5, 6, 7))


def host_runtime_libraries(machine: int, interpreter: bytes) -> set[str]:
    interpreters = {
        62: b"/lib64/ld-linux-x86-64.so.2\0",  # EM_X86_64
        183: b"/lib/ld-linux-aarch64.so.1\0",  # EM_AARCH64
    }
    if machine not in interpreters:
        raise AssertionError(f"unsupported launcher ELF machine: {machine}")
    if interpreter != interpreters[machine]:
        raise AssertionError(f"unexpected launcher interpreter: {interpreter!r}")
    allowed = {"libc.so.6", "libm.so.6"}
    if machine == 183:
        # The AArch64 C runtime imports its stack guard from the system loader,
        # which is already loaded through the verified ELF interpreter above.
        allowed.add("ld-linux-aarch64.so.1")
    return allowed


class HostRuntimePolicyTest(unittest.TestCase):
    def test_arm_accepts_its_system_loader_dependency(self) -> None:
        allowed = host_runtime_libraries(183, b"/lib/ld-linux-aarch64.so.1\0")
        self.assertEqual(allowed, {"libc.so.6", "libm.so.6", "ld-linux-aarch64.so.1"})

    def test_x86_retains_its_existing_library_restriction(self) -> None:
        allowed = host_runtime_libraries(62, b"/lib64/ld-linux-x86-64.so.2\0")
        self.assertEqual(allowed, {"libc.so.6", "libm.so.6"})

    def test_rejects_unexpected_interpreters_and_architectures(self) -> None:
        for machine, interpreter in (
            (183, b"/lib64/ld-linux-x86-64.so.2\0"),
            (62, b"/lib/ld-linux-aarch64.so.1\0"),
            (183, b"/tmp/ld-linux-aarch64.so.1\0"),
            (183, b""),
            (0, b"/lib/ld-linux-aarch64.so.1\0"),
        ):
            with self.subTest(machine=machine, interpreter=interpreter):
                with self.assertRaises(AssertionError):
                    host_runtime_libraries(machine, interpreter)

    def test_neither_architecture_permits_cpp_or_instrumentation_libraries(
        self,
    ) -> None:
        for machine, interpreter in (
            (62, b"/lib64/ld-linux-x86-64.so.2\0"),
            (183, b"/lib/ld-linux-aarch64.so.1\0"),
        ):
            with self.subTest(machine=machine):
                allowed = host_runtime_libraries(machine, interpreter)
                for forbidden in (
                    "libstdc++.so.6",
                    "libc++.so.1",
                    "libgcc_s.so.1",
                    "libunwind.so.1",
                    "libasan.so.8",
                    "libubsan.so.1",
                ):
                    self.assertNotIn(forbidden, allowed)


def elf_sections(binary: Path) -> dict[str, bytes]:
    contents = binary.read_bytes()
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", contents)
    if header[0][:6] != b"\x7fELF\x02\x01":
        raise AssertionError("expected a little-endian ELF64 launcher")
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


class LauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.launcher = self.root / "kwaque"
        shutil.copy2(Path(sys.argv[1]), self.launcher)
        self.native = self.root / "kwaque_native"
        self.marker = self.root / "native-executed.json"
        helper = self.root / "native_helper.py"
        helper.write_text(
            "import json, os, signal, sys\n"
            "from pathlib import Path\n"
            "Path(os.environ['KWAQUE_TEST_MARKER']).write_text(json.dumps({\n"
            "    'argv': sys.argv[1:], 'pid': os.getpid(), 'cwd': os.getcwd(),\n"
            "    'environment': os.environ['KWAQUE_TEST_PASSTHROUGH']}))\n"
            "if os.environ.get('KWAQUE_TEST_SIGNAL'):\n"
            "    os.kill(os.getpid(), signal.SIGTERM)\n"
            "sys.exit(int(os.environ.get('KWAQUE_TEST_STATUS', '0')))\n",
            encoding="utf-8",
        )
        self.native.write_text(
            "#!/bin/sh\n"
            f'exec {shlex.quote(sys.executable)} {shlex.quote(str(helper))} "$@"\n',
            encoding="utf-8",
        )
        self.native.chmod(0o755)
        self.environment = {
            **os.environ,
            "KWAQUE_TEST_ARCH": "x86_64",
            "KWAQUE_TEST_FEATURES": hex(X86_REQUIRED),
            "KWAQUE_TEST_MARKER": str(self.marker),
            "KWAQUE_TEST_PASSTHROUGH": "retained value",
        }

    def launch(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.launcher, *arguments],
            env=self.environment,
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_each_missing_cpu_feature_rejects_before_native_execution(self) -> None:
        for architecture, required in (
            ("x86_64", X86_REQUIRED),
            ("aarch64", ARM_REQUIRED),
        ):
            self.environment["KWAQUE_TEST_ARCH"] = architecture
            for bit in range(64):
                if not required & (1 << bit):
                    continue
                with self.subTest(architecture=architecture, missing_bit=bit):
                    self.environment["KWAQUE_TEST_FEATURES"] = hex(
                        required & ~(1 << bit)
                    )
                    result = self.launch("--version")
                    self.assertEqual(result.returncode, 1)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("CPU lacks required", result.stderr)
                    self.assertFalse(self.marker.exists())

    def test_empty_or_unknown_features_reject_before_native_execution(self) -> None:
        for architecture in ("x86_64", "aarch64", "unknown"):
            with self.subTest(architecture=architecture):
                self.environment["KWAQUE_TEST_ARCH"] = architecture
                self.environment["KWAQUE_TEST_FEATURES"] = "0"
                result = self.launch("--help")
                self.assertEqual(result.returncode, 1)
                self.assertIn("CPU lacks required", result.stderr)
                self.assertFalse(self.marker.exists())

    def test_required_features_and_additional_features_allow_exec(self) -> None:
        for architecture, required in (
            ("x86_64", X86_REQUIRED),
            ("aarch64", ARM_REQUIRED),
        ):
            for features in (required, (1 << 64) - 1):
                with self.subTest(architecture=architecture, features=features):
                    self.environment["KWAQUE_TEST_ARCH"] = architecture
                    self.environment["KWAQUE_TEST_FEATURES"] = hex(features)
                    result = self.launch("--help")
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, "")
                    self.assertEqual(result.stderr, "")
                    self.assertTrue(self.marker.exists())
                    self.marker.unlink()

    def test_exec_preserves_arguments_status_pid_cwd_and_environment(self) -> None:
        arguments = ["--config", "file with spaces.yaml", "", "--smp=2"]
        self.environment["KWAQUE_TEST_STATUS"] = "37"
        with subprocess.Popen(
            [self.launcher, *arguments],
            env=self.environment,
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as process:
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 37)
            self.assertEqual((stdout, stderr), ("", ""))
            observed = json.loads(self.marker.read_text(encoding="utf-8"))
            self.assertEqual(observed["pid"], process.pid)
        self.assertEqual(observed["argv"], arguments)
        self.assertEqual(observed["cwd"], str(self.root))
        self.assertEqual(observed["environment"], "retained value")

    def test_exec_preserves_signal_status(self) -> None:
        self.environment["KWAQUE_TEST_SIGNAL"] = "1"
        result = self.launch()
        self.assertEqual(result.returncode, -signal.SIGTERM)
        self.assertTrue(self.marker.exists())

    def test_symlink_launch_resolves_native_next_to_real_launcher(self) -> None:
        symlink = self.root / "different-directory" / "kwaque"
        symlink.parent.mkdir()
        symlink.symlink_to(self.launcher)
        self.launcher = symlink
        result = self.launch("--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.marker.exists())

    def test_native_exec_errors_fail_with_bounded_diagnostic(self) -> None:
        for failure in ("missing", "not-executable"):
            with self.subTest(failure=failure):
                if failure == "missing":
                    self.native.unlink()
                else:
                    self.native.write_text("not executable", encoding="ascii")
                    self.native.chmod(0o644)
                result = self.launch()
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(
                    result.stderr, "kwaque: cannot execute native broker\n"
                )
                self.assertFalse(self.marker.exists())

    def test_production_launcher_has_no_broker_or_instrumentation_dependencies(
        self,
    ) -> None:
        binary = Path(sys.argv[2])
        sections = elf_sections(binary)
        machine = struct.unpack_from("<H", binary.read_bytes(), 18)[0]
        allowed = host_runtime_libraries(machine, sections.get(".interp", b""))
        strings = sections[".dynstr"]
        needed = {
            strings[value : strings.index(0, value)].decode("ascii")
            for tag, value in struct.iter_unpack("<qQ", sections[".dynamic"])
            if tag == 1  # DT_NEEDED
        }
        # The toolchain can retain libm from its default link flags. Both
        # libraries belong to the supported host C runtime.
        self.assertIn("libc.so.6", needed)
        self.assertLessEqual(
            needed,
            allowed,
            f"ELF machine={machine}: unexpected libraries={sorted(needed - allowed)}; "
            f"DT_NEEDED={sorted(needed)}",
        )
        symbols = sections.get(".strtab", b"") + strings
        for forbidden in (
            b"__asan_",
            b"__ubsan_",
            b"__tsan_",
            b"__sancov_",
            b"__llvm_profile_",
            b"_GLOBAL__sub_I",
            b"__cxx_global_var_init",
            b"__cxa_atexit",
            b"KWAQUE_TEST_",
        ):
            self.assertNotIn(forbidden, symbols)
        # The C runtime may register frame_dummy; no application constructors
        # or preinitializers may run before the prerequisite check.
        self.assertLessEqual(len(sections.get(".init_array", b"")), 8)
        self.assertEqual(sections.get(".preinit_array", b""), b"")
        self.assertNotIn(b"KWAQUE_TEST_", Path(sys.argv[2]).read_bytes())


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
