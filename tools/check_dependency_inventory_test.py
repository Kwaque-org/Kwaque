from __future__ import annotations

import unittest

from tools.check_dependency_inventory import (
    module_dependencies,
    module_dependency_errors,
    workflow_dependency_errors,
)


class DependencyInventoryTest(unittest.TestCase):
    def test_extracts_direct_module_versions(self) -> None:
        self.assertEqual(
            module_dependencies(
                'bazel_dep(name = "alpha", version = "1.2.3")\n'
                'bazel_dep(name = "beta")\n'
            ),
            [("alpha", "1.2.3"), ("beta", None)],
        )

    def test_requires_module_name_and_version(self) -> None:
        module = 'bazel_dep(name = "alpha_lib", version = "1.2.3")'
        inventory = "| Dependency | Version |\n|---|---|\n| `alpha-lib` | 1.2.3 |"
        self.assertEqual(module_dependency_errors(module, inventory), [])
        self.assertEqual(
            len(module_dependency_errors(module, inventory.replace("1.2.3", "2.0.0"))),
            1,
        )

    def test_requires_inventoried_and_immutable_workflow_dependencies(self) -> None:
        inventory = "| Dependency | Version |\n|---|---|\n| `owner/action` | 1.0 |"
        pinned = "uses: owner/action@" + "a" * 40
        self.assertEqual(workflow_dependency_errors([pinned], inventory), [])
        errors = workflow_dependency_errors(["uses: unknown/action@v1"], inventory)
        self.assertEqual(len(errors), 2)


if __name__ == "__main__":
    unittest.main()
