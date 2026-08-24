from __future__ import annotations

import unittest

from tools.check_dependency_inventory import (
    archive_dependencies,
    archive_dependency_errors,
    archive_import_errors,
    imported_archive_repositories,
    module_dependencies,
    module_dependency_errors,
    workflow_dependency_errors,
)

REPOSITORIES = """
load("//bazel:versions.bzl", "SEASTAR_REVISION")

def declare_native_dependencies():
    http_archive(
        name = "alpha",
        sha256 = "aaa",
        strip_prefix = "alpha-1.2.3",
        url = "https://example.invalid/alpha-1.2.3.tar.gz",
    )

    http_archive(
        name = "beta",
        sha256 = "bbb",
        strip_prefix = "beta-{}".format(SEASTAR_REVISION),
        url = "https://example.invalid/{}.tar.gz".format(SEASTAR_REVISION),
    )

    sysroot(
        name = "gamma_sysroot",
        sha256 = "ccc",
        urls = ["https://example.invalid/sysroot-gamma-2026-01-02.tar.zst"],
    )
"""

VERSIONS = 'SEASTAR_REVISION = "abc123"\n'

ARCHIVE_INVENTORY = (
    "| Dependency | Version |\n"
    "|---|---|\n"
    "| `alpha` | 1.2.3 |\n"
    "| `beta` | `abc123` |\n"
    "| `gamma_sysroot` | `sysroot-gamma-2026-01-02` |\n"
)

MODULE_IMPORTS = """
native_dependencies = use_extension("//bazel:extensions.bzl", "native_dependencies")
use_repo(
    native_dependencies,
    "alpha",
    "beta",
    "gamma_sysroot",
)
"""


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

    def test_extracts_pinned_archive_versions(self) -> None:
        """Literal prefixes, templated prefixes, and prefix-less sysroots all resolve."""
        self.assertEqual(
            archive_dependencies(REPOSITORIES, VERSIONS),
            [
                ("alpha", "1.2.3", True),
                ("beta", "abc123", True),
                ("gamma_sysroot", "sysroot-gamma-2026-01-02", True),
            ],
        )

    def test_requires_inventoried_pinned_archives(self) -> None:
        self.assertEqual(
            archive_dependency_errors(REPOSITORIES, VERSIONS, ARCHIVE_INVENTORY), []
        )
        without_sysroot = ARCHIVE_INVENTORY.replace(
            "| `gamma_sysroot` | `sysroot-gamma-2026-01-02` |\n", ""
        )
        errors = archive_dependency_errors(REPOSITORIES, VERSIONS, without_sysroot)
        self.assertEqual(len(errors), 1)
        self.assertIn("gamma_sysroot", errors[0])

    def test_requires_inventoried_archive_revision(self) -> None:
        stale = ARCHIVE_INVENTORY.replace("`abc123`", "`def456`")
        errors = archive_dependency_errors(REPOSITORIES, VERSIONS, stale)
        self.assertEqual(len(errors), 1)
        self.assertIn("beta", errors[0])

    def test_requires_archive_checksum(self) -> None:
        unpinned = REPOSITORIES.replace('        sha256 = "ccc",\n', "")
        errors = archive_dependency_errors(unpinned, VERSIONS, ARCHIVE_INVENTORY)
        self.assertEqual(len(errors), 1)
        self.assertIn("no checksum", errors[0])

    def test_requires_declared_and_imported_archives_to_agree(self) -> None:
        self.assertEqual(
            archive_import_errors(MODULE_IMPORTS, REPOSITORIES, VERSIONS), []
        )
        missing_import = MODULE_IMPORTS.replace('    "gamma_sysroot",\n', "")
        errors = archive_import_errors(missing_import, REPOSITORIES, VERSIONS)
        self.assertEqual(len(errors), 1)
        self.assertIn("never imported", errors[0])
        extra_import = MODULE_IMPORTS.replace(
            '    "alpha",\n', '    "alpha",\n    "delta",\n'
        )
        errors = archive_import_errors(extra_import, REPOSITORIES, VERSIONS)
        self.assertEqual(len(errors), 1)
        self.assertIn("no pinned archive declares", errors[0])

    def test_reports_unwired_archive_extension(self) -> None:
        errors = archive_import_errors(
            'module(name = "kwaque")', REPOSITORIES, VERSIONS
        )
        self.assertEqual(len(errors), 1)
        self.assertIn("does not use the pinned archive extension", errors[0])


if __name__ == "__main__":
    unittest.main()


class ExactMatchingTest(unittest.TestCase):
    """A shorter name must not be satisfied by a longer inventoried one."""

    INVENTORY = "| Dependency | Version |\n" "|---|---|\n" "| foo-tools | 11.2.3 |\n"

    def test_a_shorter_name_does_not_match_a_longer_row(self) -> None:
        module = 'bazel_dep(name = "foo", version = "11.2.3")\n'
        errors = module_dependency_errors(module, self.INVENTORY)
        self.assertEqual(
            errors,
            ["direct module dependency 'foo' is missing from THIRD_PARTY.md"],
        )

    def test_a_shorter_version_does_not_match_a_longer_one(self) -> None:
        inventory = "| Dependency | Version |\n" "|---|---|\n" "| foo | 11.2.3 |\n"
        module = 'bazel_dep(name = "foo", version = "1.2.3")\n'
        errors = module_dependency_errors(module, inventory)
        self.assertEqual(
            errors,
            ["direct module dependency 'foo' version '1.2.3' is not inventoried"],
        )

    def test_a_backticked_identifier_is_accepted(self) -> None:
        """Rows may name the project and give the build identifier in backticks."""
        inventory = (
            "| Dependency | Version |\n"
            "|---|---|\n"
            "| Foo Project (`foo-cpp`) | 1.2.3 |\n"
        )
        module = 'bazel_dep(name = "foo-cpp", version = "1.2.3")\n'
        self.assertEqual(module_dependency_errors(module, inventory), [])

    def test_a_version_among_several_tokens_is_accepted(self) -> None:
        inventory = (
            "| Dependency | Version |\n"
            "|---|---|\n"
            "| foo | 1.7.0, archive override `abc123` |\n"
        )
        module = 'bazel_dep(name = "foo", version = "1.7.0")\n'
        self.assertEqual(module_dependency_errors(module, inventory), [])


class SplitImportTest(unittest.TestCase):
    """Imports spread over several use_repo calls must all be collected."""

    EXTENSION = (
        'native = use_extension("//bazel:extensions.bzl", "native_dependencies")\n'
    )

    def test_repositories_from_every_matching_call_are_collected(self) -> None:
        module = self.EXTENSION + (
            'use_repo(native, "alpha")\n'
            "use_repo(\n"
            "    native,\n"
            '    "beta",\n'
            '    "gamma_sysroot",\n'
            ")\n"
        )
        self.assertEqual(
            sorted(imported_archive_repositories(module)),
            ["alpha", "beta", "gamma_sysroot"],
        )

    def test_split_declarations_satisfy_the_import_check(self) -> None:
        module = self.EXTENSION + (
            'use_repo(native, "alpha")\n' 'use_repo(native, "beta", "gamma_sysroot")\n'
        )
        self.assertEqual(archive_import_errors(module, REPOSITORIES, VERSIONS), [])

    def test_calls_for_other_extensions_are_ignored(self) -> None:
        module = self.EXTENSION + (
            'use_repo(native, "alpha")\n'
            'use_repo(other, "unrelated")\n'
            'use_repo(native, "beta", "gamma_sysroot")\n'
        )
        self.assertNotIn("unrelated", imported_archive_repositories(module))
