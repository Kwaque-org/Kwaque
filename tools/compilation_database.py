from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from tools.run_clang_tidy import PRODUCTION_DATABASE_DIRECTORY, production_targets
except ModuleNotFoundError:
    from run_clang_tidy import PRODUCTION_DATABASE_DIRECTORY, production_targets

CPP_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx"})

# Paths that the build records as compiled-in-place locations. They are dropped
# because the database presents every command as though it ran in the workspace.
PATH_REMAPPING_FLAGS = (
    "-fdebug-prefix-map",
    "-ffile-prefix-map",
    "-fmacro-prefix-map",
    "-ffile-compilation-dir",
)

# Traversing the output base through the workspace's own bazel-out symlink keeps
# the emitted paths position-independent: they follow whichever output base this
# workspace currently points at, instead of hard-coding one.
EXTERNAL_LINK_TARGET = "bazel-out/../../../external"


def workspace_root() -> Path:
    configured = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not configured:
        raise RuntimeError("run this generator with bazel run")
    return Path(configured).resolve()


def active_output_base(root: Path) -> Path:
    """Resolve the output base this workspace currently points at.

    The workspace's own bazel-out symlink is the authority. Deriving the base
    from it, rather than asking a nested bazel, keeps the generator correct no
    matter which startup flags produced the build being described.
    """
    bazel_out = root / "bazel-out"
    if not os.path.lexists(bazel_out):
        raise RuntimeError(
            "//bazel-out is missing; run a build before generating the database"
        )
    if not bazel_out.is_symlink():
        raise RuntimeError(
            f"{bazel_out} is not a symlink; expected Bazel's convenience link"
        )
    resolved = bazel_out.resolve()
    # The link points at <output base>/execroot/<workspace>/bazel-out. Confirm
    # that shape before taking a path out of it: a dangling link satisfies
    # lexists, and deriving a base from one would pin later commands to an
    # output base that does not exist.
    if (
        not resolved.is_dir()
        or len(resolved.parents) < 3
        or resolved.name != "bazel-out"
        or resolved.parents[1].name != "execroot"
    ):
        raise RuntimeError(f"unexpected bazel-out target: {resolved}")
    return resolved.parents[2]


def ensure_external_link(root: Path, external_root: Path) -> None:
    """Point <workspace>/external at the external roots of the active output base.

    External include arguments stay workspace-relative so that the database
    remains valid when the output base changes. That only works if the workspace
    exposes the external roots, which is what this link provides.
    """
    if not os.path.lexists(root / "bazel-out"):
        raise RuntimeError(
            "//bazel-out is missing; run a build before generating the database"
        )

    link = root / "external"
    if link.is_symlink():
        if os.readlink(link) != EXTERNAL_LINK_TARGET:
            link.unlink()
    elif os.path.lexists(link):
        raise RuntimeError(
            f"{link} exists and is not a symlink; refusing to replace it"
        )

    if not os.path.lexists(link):
        link.symlink_to(EXTERNAL_LINK_TARGET)
        print(f"Linked {link} -> {EXTERNAL_LINK_TARGET}")

    resolved = link.resolve()
    if resolved != external_root.resolve():
        raise RuntimeError(f"{link} resolves to {resolved}, expected {external_root}")


def compiler_arguments(
    action: dict[str, Any], execution_root: Path
) -> tuple[str, list[str]] | None:
    arguments = list(action.get("arguments", []))
    if not arguments:
        return None

    source = None
    for index, argument in enumerate(arguments[:-1]):
        if argument == "-c":
            source = arguments[index + 1]
            break
    if source is None or Path(source).suffix.lower() not in CPP_SUFFIXES:
        return None
    if source.startswith(("external/", "bazel-out/")):
        return None

    compiler = Path(arguments[0])
    if not compiler.is_absolute():
        compiler = execution_root / compiler
    if compiler.name == "cc_wrapper.sh":
        clang_cpp = compiler.parent / "clang-cpp"
        if clang_cpp.exists():
            clang = clang_cpp.resolve().parent / "clang"
            if clang.exists():
                compiler = clang
    # The compiler is addressed absolutely on purpose: it resolves into the
    # content-addressed repository cache, which is shared between output bases
    # and therefore more stable than any one execution root.
    arguments[0] = str(compiler)

    # Every remaining external/ and bazel-out/ path is left workspace-relative so
    # that it resolves through this workspace's own symlinks rather than through
    # whichever output base happened to answer the query.
    kept = [arguments[0]]
    for argument in arguments[1:]:
        if argument.startswith(PATH_REMAPPING_FLAGS):
            continue
        kept.append(argument)
    return source, kept


def select_commands(
    actions: list[dict],
    targets: list[dict],
    production: set[str],
    execution_root: Path,
    root: Path,
    *,
    production_only: bool = False,
) -> list[dict]:
    labels = {str(target["id"]): target["label"] for target in targets}
    candidates = {}
    for action in actions:
        parsed = compiler_arguments(action, execution_root)
        if parsed is None:
            continue
        source, arguments = parsed
        label = labels.get(str(action.get("targetId")))
        if label is None:
            raise RuntimeError("a workspace compilation action has no target identity")
        if production_only and label not in production:
            continue
        entry = {"directory": str(root), "file": source, "arguments": arguments}
        if "-o" in arguments:
            entry["output"] = arguments[arguments.index("-o") + 1]
        # Keep every distinct compile variant; native tooling supports multiple
        # commands per source. Strict analysis gets a separate production subset.
        candidates[(source, tuple(arguments))] = entry
    return [candidates[key] for key in sorted(candidates)]


def query_expression(fuzz_only: bool) -> str:
    roots = (
        'attr(tags, "fuzz", //...)'
        if fuzz_only
        else '//... except attr(tags, "manual|fuzz", //...)'
    )
    return f'mnemonic("CppCompile", deps({roots}))'


def generate(extra_bazel_args: list[str], *, fuzz_only: bool = False) -> int:
    root = workspace_root()
    # Pin every nested invocation to the base the workspace already points at.
    # Without this the nested bazel would silently pick its own default base and
    # describe a build that was never performed there.
    output_base = active_output_base(root)
    execution_root = (root / "bazel-out").resolve().parent
    ensure_external_link(root, output_base / "external")
    command = [
        "bazel",
        f"--output_base={output_base}",
        "aquery",
        query_expression(fuzz_only),
        "--output=jsonproto",
        "--include_artifacts=false",
        "--features=-compiler_param_file",
        "--host_features=-compiler_param_file",
        "--features=-layering_check",
        "--host_features=-layering_check",
        "--features=-parse_headers",
        "--host_features=-parse_headers",
        "--noshow_progress",
        *extra_bazel_args,
    ]
    result = subprocess.run(
        command,
        cwd=root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    response = json.loads(result.stdout)
    production = set() if fuzz_only else set(production_targets(root))
    output = select_commands(
        response.get("actions", []),
        response.get("targets", []),
        production,
        execution_root,
        root,
    )
    strict = None
    if not fuzz_only:
        strict = select_commands(
            response.get("actions", []),
            response.get("targets", []),
            production,
            execution_root,
            root,
            production_only=True,
        )
    if not output:
        raise RuntimeError("Bazel returned no workspace C++ compilation actions")
    destination = root / "compile_commands.json"
    destination.write_text(json.dumps(output, indent=2) + "\n")
    if strict is None:
        print(f"Wrote {len(output)} fuzz compilation commands")
    else:
        strict_directory = root / PRODUCTION_DATABASE_DIRECTORY
        strict_directory.mkdir(parents=True, exist_ok=True)
        (strict_directory / "compile_commands.json").write_text(
            json.dumps(strict, indent=2) + "\n"
        )
        print(
            f"Wrote {len(output)} ordinary and {len(strict)} production compilation commands"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate compile_commands.json from Bazel"
    )
    parser.add_argument(
        "--fuzz-only",
        action="store_true",
        help="describe fuzz targets and their dependencies",
    )
    arguments, bazel_args = parser.parse_known_args()
    return generate(bazel_args, fuzz_only=arguments.fuzz_only)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        OSError,
        RuntimeError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as error:
        print(f"compilation database generation failed: {error}", file=sys.stderr)
        sys.exit(1)
