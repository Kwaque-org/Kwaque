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

    def test_rejects_bootstrap_runtime_and_simulation_tuning(self) -> None:
        root = self.workspace(
            {
                "src/config/bootstrap_config.h": (
                    'constexpr auto key = "reactor_headroom";\n'
                ),
                "src/config/bootstrap_config.cc": "",
                "conf/kwaque.yaml": "kwaque:\n  simulation:\n    seed: 1\n",
            }
        )
        violations = "\n".join(scan(root))
        self.assertEqual(
            violations.count("bootstrap exposes runtime/simulation tuning"),
            2,
        )

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

    def test_rejects_broad_resource_owner_reference(self) -> None:
        for declaration in (
            "void consume(resource::resource_manager& manager);\n",
            "void consume(resource::resource_registry* registry);\n",
        ):
            with self.subTest(declaration=declaration):
                root = self.workspace(
                    {
                        "src/storage/reader.h": declaration,
                        "src/storage/BUILD": "",
                    }
                )
                self.assertIn(
                    "broad resource-owner reference", "\n".join(scan(root))
                )

        root = self.workspace(
            {
                "src/broker/application_internal.h": (
                    "void consume(resource::resource_manager& manager);\n"
                ),
                "src/broker/BUILD": "",
            }
        )
        self.assertIn(
            "broad resource-owner reference", "\n".join(scan(root))
        )

    def test_rejects_resource_to_simulation_source_dependency(self) -> None:
        root = self.workspace(
            {
                "src/resource/resource_registry.h": (
                    "namespace kwaque::simulation { class environment; }\n"
                    "friend class ::kwaque::simulation::environment;\n"
                ),
                "src/resource/BUILD": "",
            }
        )
        self.assertIn(
            "resource-to-simulation source dependency", "\n".join(scan(root))
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
                    "kwaque_cc_library(\n"
                    '    name = "reader",\n'
                    '    deps = ["//src/runtime/production:backend"],\n'
                    ")\n"
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
                    '#include "src/runtime/production/environment.h"\n'
                    '#include "src/runtime/production/network.h"\n'
                ),
                "src/broker/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "application_internal",\n'
                    '    deps = [\n'
                    '        "//src/runtime/production:environment",\n'
                    '        "//src/runtime/production:network",\n'
                    '    ],\n'
                    ")\n"
                ),
            }
        )
        violations = "\n".join(scan(root))
        self.assertEqual(violations.count("concrete runtime adapter include"), 1)
        self.assertEqual(violations.count("concrete runtime adapter dependency"), 1)

    def test_rejects_simulation_adapter_outside_tests(self) -> None:
        root = self.workspace(
            {
                "src/storage/reader.h": (
                    '#include "src/simulation/environment.h"\n'
                ),
                "src/storage/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "reader",\n'
                    '    hdrs = ["reader.h"],\n'
                    '    deps = ["//src/simulation:environment"],\n'
                    ")\n"
                ),
            }
        )
        violations = "\n".join(scan(root))
        self.assertIn("concrete runtime adapter include", violations)
        self.assertIn("concrete runtime adapter dependency", violations)

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

    def test_shared_environment_contract_rejects_concrete_include(self) -> None:
        root = self.workspace(
            {
                "src/runtime/testing/contracts/environment_component.h": (
                    '#include "src/simulation/environment.h"\n'
                ),
                "src/runtime/testing/contracts/BUILD": "",
            }
        )
        self.assertIn(
            "shared environment contract includes a concrete adapter",
            "\n".join(scan(root)),
        )

    def test_component_rejects_broad_owner_retention(self) -> None:
        for declaration in (
            "basic_runtime<Backend>* runtime;\n",
            "Backend& environment;\n",
            "resource::resource_manager& manager;\n",
            "resource::resource_registry& registry;\n",
        ):
            with self.subTest(declaration=declaration):
                root = self.workspace(
                    {
                        "src/runtime/testing/contracts/environment_component.h": (
                            declaration
                        ),
                        "src/runtime/testing/contracts/BUILD": "",
                    }
                )
                self.assertIn(
                    "component retains a broad owner", "\n".join(scan(root))
                )

    def test_shared_environment_rule_rejects_concrete_dependency(self) -> None:
        root = self.workspace(
            {
                "src/runtime/testing/contracts/environment_contract.h": "",
                "src/runtime/testing/contracts/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "environment_contract",\n'
                    '    deps = ["//src/runtime/production:environment"],\n'
                    ")\n"
                ),
            }
        )
        self.assertIn(
            "shared environment contract depends on a concrete adapter",
            "\n".join(scan(root)),
        )

    def test_allows_separate_production_component_instantiation(self) -> None:
        root = self.workspace(
            {
                "src/runtime/testing/contracts/production_environment_component.cc": (
                    '#include "src/runtime/production/environment.h"\n'
                ),
                "src/runtime/testing/contracts/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "production_environment_component",\n'
                    "    deps = [\n"
                    '        "//src/runtime/production:environment",\n'
                    "    ],\n"
                    ")\n"
                ),
            }
        )
        self.assertEqual(scan(root), [])

    def test_concrete_environment_test_rejects_copied_contract_body(self) -> None:
        root = self.workspace(
            {
                "src/runtime/tests/production_environment_test.cc": (
                    "auto result = component.admit(bytes);\n"
                ),
                "src/runtime/tests/BUILD": "",
            }
        )
        self.assertIn(
            "concrete test duplicates the shared environment contract body",
            "\n".join(scan(root)),
        )

    def test_simulation_test_rejects_local_scheduler_pump(self) -> None:
        for name in ("pump_until", "pump_deterministic_until"):
            with self.subTest(name=name):
                root = self.workspace(
                    {
                        "src/simulation/tests/component_test.cc": (
                            f"seastar::future<> {name}(scheduler&, future&);\n"
                        ),
                        "src/simulation/tests/BUILD": "",
                    }
                )
                self.assertIn(
                    "simulation test duplicates the shared scheduler driver",
                    "\n".join(scan(root)),
                )

    def test_broker_rejects_legacy_runtime_owner(self) -> None:
        for declaration in (
            "auto runtime_service_ = owner();\n",
            "auto production_backends_ = owner();\n",
            "using owner = backend_owner;\n",
        ):
            with self.subTest(declaration=declaration):
                root = self.workspace(
                    {
                        "src/broker/application_internal.h": declaration,
                        "src/broker/BUILD": "",
                    }
                )
                self.assertIn(
                    "broker retains a superseded runtime owner",
                    "\n".join(scan(root)),
                )

    def test_broker_rejects_runtime_checkpoint_state(self) -> None:
        root = self.workspace(
            {
                "src/broker/application_internal.h": (
                    "unsigned failure_point = 1;\n"
                ),
                "src/broker/application_test_support.h": (
                    "unsigned failure_point = 1;\n"
                ),
                "src/broker/BUILD": "",
            }
        )
        violations = "\n".join(scan(root))
        self.assertEqual(violations.count("runtime checkpoint state"), 1)

    def test_broker_checkpoint_support_must_be_test_only(self) -> None:
        root = self.workspace(
            {
                "src/broker/application_test_support.h": "",
                "src/broker/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "application_test_support",\n'
                    '    hdrs = ["application_test_support.h"],\n'
                    ")\n"
                ),
            }
        )
        self.assertIn("is not test-only", "\n".join(scan(root)))

    def test_production_library_rejects_checkpoint_support(self) -> None:
        root = self.workspace(
            {
                "src/broker/application_test_support.h": "",
                "src/broker/BUILD": (
                    "kwaque_cc_library(\n"
                    '    name = "application",\n'
                    '    deps = [":application_test_support"],\n'
                    ")\n"
                ),
            }
        )
        self.assertIn(
            "production library depends on application checkpoint support",
            "\n".join(scan(root)),
        )


if __name__ == "__main__":
    unittest.main()
