from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_runtime_boundaries import scan


class RuntimeBoundaryTest(unittest.TestCase):
    def workspace(self, files: dict[str, str]) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        for name, contents in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents, encoding="utf-8")
        return root

    def test_accepts_narrow_component_api(self) -> None:
        root = self.workspace(
            {
                "src/storage/reader.h": (
                    '#include "src/runtime/environment.h"\n'
                    "template<typename RuntimeView> class reader {};\n"
                ),
                "src/storage/BUILD": "",
            }
        )
        self.assertEqual(scan(root), [])

    def test_rejects_concrete_adapter_in_component_header(self) -> None:
        root = self.workspace(
            {
                "src/storage/reader.h": (
                    '#include "src/runtime/production/backend.h"\n'
                ),
                "src/storage/BUILD": "",
            }
        )
        self.assertIn("concrete runtime adapter include", "\n".join(scan(root)))

    def test_rejects_broad_runtime_reference(self) -> None:
        for declaration in (
            "void consume(basic_runtime<int>& runtime);\n",
            "void consume(basic_runtime<int>* runtime);\n",
            "using runtime_type = basic_runtime<int>;\n",
            "basic_runtime<int> make_runtime();\n",
        ):
            with self.subTest(declaration=declaration):
                root = self.workspace(
                    {
                        "src/storage/reader.h": (
                            "template<typename T> class basic_runtime;\n"
                            + declaration
                        ),
                        "src/storage/BUILD": "",
                    }
                )
                self.assertIn(
                    "broad runtime reference", "\n".join(scan(root))
                )

    def test_allows_basic_runtime_declaration_owner(self) -> None:
        root = self.workspace(
            {
                "src/runtime/environment.h": (
                    "template<typename T> class basic_runtime;\n"
                    "template<typename T> class basic_runtime<T*> {};\n"
                ),
                "src/runtime/BUILD": "",
            }
        )
        self.assertEqual(scan(root), [])

    def test_rejects_reverse_and_build_dependencies(self) -> None:
        root = self.workspace(
            {
                "src/runtime/production/backend.cc": (
                    '#include "src/runtime/testing/contracts.h"\n'
                ),
                "src/storage/BUILD": (
                    'deps = ["//src/runtime/production:backend"]\n'
                ),
            }
        )
        violations = "\n".join(scan(root))
        self.assertIn("production reverse dependency", violations)
        self.assertIn("concrete runtime adapter dependency", violations)

    def test_rejects_foundational_resource_cycle(self) -> None:
        root = self.workspace(
            {
                "src/runtime/environment.h": (
                    '#include "src/resource/resource_manager.h"\n'
                ),
                "src/runtime/BUILD": "",
            }
        )
        self.assertIn("runtime-to-resource dependency", "\n".join(scan(root)))

    def test_composition_allowance_is_exact(self) -> None:
        root = self.workspace(
            {
                "src/broker/application_internal.h": (
                    '#include "src/runtime/production/backend.h"\n'
                    '#include "src/runtime/production/network.h"\n'
                ),
                "src/broker/BUILD": (
                    'deps = [\n'
                    '    "//src/runtime/production:backend",\n'
                    '    "//src/runtime/production:network",\n'
                    ']\n'
                ),
            }
        )
        violations = "\n".join(scan(root))
        self.assertEqual(violations.count("concrete runtime adapter include"), 1)
        self.assertEqual(violations.count("concrete runtime adapter dependency"), 1)

    def test_rejects_exported_internal_composition_header(self) -> None:
        root = self.workspace(
            {
                "src/broker/application_internal.h": (
                    '#include "src/runtime/production/backend.h"\n'
                ),
                "src/broker/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "application",\n'
                    '    hdrs = ["application.h", "application_internal.h"],\n'
                    '    visibility = ["//visibility:public"],\n'
                    '    deps = ["//src/runtime/production:backend"],\n'
                    ")\n"
                ),
            }
        )
        self.assertIn(
            "internal composition header exported publicly", "\n".join(scan(root))
        )


if __name__ == "__main__":
    unittest.main()
