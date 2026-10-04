"""Check that the package ships a license for every dependency it contains.

The C and C++ libraries reachable from the packaged binaries are compiled or
linked into them. Each one must map to a `licenses/<name>` directory in the
archive, and every such directory must belong to a dependency that ships.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

PACKAGED_BINARIES = (
    "//src/broker:kwaque",
    "//src/broker:kwaque_native",
    "@seastar//:iotune",
)
QUERY = 'kind("cc_library|cc_import|configure_make|cmake", deps({}))'.format(
    " + ".join(PACKAGED_BINARIES)
)

# Dependency repository, by its module or repository name, to the license
# directory that ships for it.
LICENSE_DIRECTORIES = {
    "abseil-cpp": "abseil",
    "boost": "boost",
    "c-ares": "c-ares",
    "crc32c": "crc32c",
    "fmt": "fmt",
    "hwloc": "hwloc",
    "liburing": "liburing",
    "lksctp": "lksctp-tools",
    "lz4": "lz4",
    "openssl": "openssl",
    "protobuf": "protobuf",
    "seastar": "seastar",
    "unordered_dense": "unordered_dense",
    "xxhash": "xxhash",
    "yaml-cpp": "yaml-cpp",
    "zlib": "zlib",
}
# Components inside a dependency repository that carry their own license.
COMPONENT_LICENSES = {
    ("protobuf", "third_party/utf8_range"): "utf8_range",
}
# Repositories whose reachable libraries contribute no code to the binaries.
NO_SHIPPED_CODE = {
    "rules_cc": "empty placeholder libraries that the C++ rules link by default",
}

_LABEL = re.compile(r"^@@?([^/]+)//([^:]*):")
_PACKAGED_LICENSE = re.compile(r'^\s*prefix = "licenses/([^"]+)",$', re.MULTILINE)
_BAZEL_WORKSPACE_ENV = "BUILD_WORKSPACE_DIRECTORY"


def repository_name(name: str) -> str:
    """Return the module name for an apparent or canonical repository name."""
    # Canonical names join extension and repository names with "+", as in
    # "zlib+" or "+native_dependencies+seastar".
    return [part for part in name.split("+") if part][-1]


def required_licenses(labels: list[str]) -> tuple[set[str], list[str]]:
    required = set()
    errors = []
    for label in labels:
        match = _LABEL.match(label)
        if match is None:
            continue  # A first-party target.
        repository, package = repository_name(match.group(1)), match.group(2)
        if repository in NO_SHIPPED_CODE:
            continue
        if repository not in LICENSE_DIRECTORIES:
            errors.append(
                f"{label.split()[0]} ships in the package but its repository "
                f"{repository!r} has no license mapping"
            )
            continue
        required.add(LICENSE_DIRECTORIES[repository])
        for (owner, prefix), directory in COMPONENT_LICENSES.items():
            if owner == repository and (
                package == prefix or package.startswith(prefix + "/")
            ):
                required.add(directory)
    return required, errors


def packaged_licenses(package_build: str) -> set[str]:
    return set(_PACKAGED_LICENSE.findall(package_build))


def license_errors(labels: list[str], package_build: str) -> list[str]:
    required, errors = required_licenses(labels)
    packaged = packaged_licenses(package_build)
    for directory in sorted(required - packaged):
        errors.append(f"the package is missing licenses/{directory}")
    for directory in sorted(packaged - required):
        errors.append(
            f"the package ships licenses/{directory}, but no packaged binary "
            "contains that dependency"
        )
    return errors


def query_workspace(workspace: Path) -> list[str]:
    result = subprocess.run(
        [
            "bazel",
            "cquery",
            "--noimplicit_deps",
            "--notool_deps",
            "--output=label",
            QUERY,
        ],
        cwd=workspace,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def resolve_workspace(workspace: Path | None) -> Path:
    invocation_workspace = os.environ.get(_BAZEL_WORKSPACE_ENV)
    if workspace is None:
        return Path(invocation_workspace) if invocation_workspace else Path.cwd()
    if workspace.is_absolute() or invocation_workspace is None:
        return workspace
    return Path(invocation_workspace) / workspace


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--labels",
        type=Path,
        help="read cquery labels from a file instead of invoking Bazel",
    )
    parser.add_argument("--workspace", type=Path)
    arguments = parser.parse_args()
    workspace = resolve_workspace(arguments.workspace)
    try:
        labels = (
            arguments.labels.read_text(encoding="utf-8").splitlines()
            if arguments.labels is not None
            else query_workspace(workspace)
        )
        package_build = (workspace / "bazel/packaging/BUILD").read_text(
            encoding="utf-8"
        )
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"unable to inspect the packaged dependencies: {error}", file=sys.stderr)
        return 2
    errors = license_errors(labels, package_build)
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("Every packaged dependency ships its license.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
