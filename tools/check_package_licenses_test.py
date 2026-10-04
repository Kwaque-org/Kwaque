from __future__ import annotations

import unittest

from tools.check_package_licenses import (
    license_errors,
    packaged_licenses,
    repository_name,
    required_licenses,
)

PACKAGE = """
pkg_files(
    name = "zlib_license",
    prefix = "licenses/zlib",
)

pkg_files(
    name = "protobuf_license",
    prefix = "licenses/protobuf",
)

pkg_files(
    name = "utf8_range_license",
    prefix = "licenses/utf8_range",
)
"""

LABELS = [
    "//src/broker:application (a1b2c3)",
    "@@zlib+//:z (a1b2c3)",
    "@protobuf//:protobuf (a1b2c3)",
    "@protobuf//third_party/utf8_range:utf8_validity (a1b2c3)",
    "@rules_cc//:link_extra_lib (a1b2c3)",
]


class PackageLicenseTest(unittest.TestCase):
    def test_canonical_and_apparent_repository_names_resolve_to_modules(self):
        for name, expected in (
            ("zlib+", "zlib"),
            ("+native_dependencies+seastar", "seastar"),
            ("rules_boost++non_module_dependencies+boost", "boost"),
            ("abseil-cpp", "abseil-cpp"),
        ):
            with self.subTest(name=name):
                self.assertEqual(repository_name(name), expected)

    def test_every_shipped_dependency_and_component_has_a_license(self):
        self.assertEqual(packaged_licenses(PACKAGE), {"zlib", "protobuf", "utf8_range"})
        self.assertEqual(license_errors(LABELS, PACKAGE), [])

    def test_a_missing_license_fails(self):
        package = PACKAGE.replace('prefix = "licenses/utf8_range",', "")
        self.assertEqual(
            license_errors(LABELS, package),
            ["the package is missing licenses/utf8_range"],
        )

    def test_a_license_for_a_dependency_that_does_not_ship_fails(self):
        labels = [label for label in LABELS if "utf8_range" not in label]
        self.assertEqual(
            license_errors(labels, PACKAGE),
            [
                "the package ships licenses/utf8_range, but no packaged binary "
                "contains that dependency"
            ],
        )

    def test_an_unmapped_dependency_fails_instead_of_being_ignored(self):
        required, errors = required_licenses(["@@newdep+//:lib (a1b2c3)"])
        self.assertEqual(required, set())
        self.assertEqual(len(errors), 1)
        self.assertIn("'newdep' has no license mapping", errors[0])

    def test_first_party_and_code_free_repositories_need_no_license(self):
        required, errors = required_licenses(
            ["//src/base:base (a1b2c3)", "@rules_cc//:empty_lib (a1b2c3)"]
        )
        self.assertEqual((required, errors), (set(), []))


if __name__ == "__main__":
    unittest.main()
