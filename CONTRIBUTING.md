# Contributing to Kwaque

Kwaque is under active development and is not yet accepting external feature
contributions. The maintainers will publish the review and acceptance
requirements before that changes. Issue reports and questions are welcome now.

This file records what every change is expected to pass, whoever writes it.

## Before you start

Install the build prerequisites listed in the
[README requirements section](README.md#requirements). No project-specific
system packages are needed; the build fetches its own compiler and dependencies.

Optionally install the commit hooks, which run the same formatting and integrity
checks as continuous integration:

```bash
pre-commit install
```

## What every change must pass

Run these from the repository root. The
[README development section](README.md#development) documents each command and
the available build configurations in full.

```bash
# Build
bazel build --config=dev //:kwaque

# Tests, including subprocess smoke tests
bazel test --config=dev //...

# Same suite under AddressSanitizer and UndefinedBehaviorSanitizer
bazel test --config=debug //...

# Formatting
bazel run //tools:format_cpp_changed
bazel run //tools:buildifier_check

# Static analysis
bazel run //tools:compile_commands
bazel run //tools:clang_tidy
bazel run //tools:clang_tidy_strict

# Repository integrity
bazel run //tools:check_dependency_inventory
bazel run //tools:check_generated_artifacts
bazel run //tools:check_bazel_package_cycles
```

Changes that add or update a dependency must also run `bazel mod tidy` and
commit the resulting lockfile, and must follow the procedure in
[DEPENDENCIES.md](DEPENDENCIES.md).

## Expectations for a change

- **Tests come with the code.** New behavior needs a test at the lowest level
  that can observe it: a pure unit test where possible, a reactor test when
  Seastar is involved, a subprocess smoke test for process-level behavior.
- **Parsers get fuzz coverage.** Any new code that decodes untrusted bytes needs
  a bounded fuzz target and a seed corpus.
- **Persisted and wire formats are versioned from their first commit.** They need
  a magic value, version, length, checksum, bounds, and a committed byte fixture.
- **Resource use is bounded.** Every queue, cache, stream, and background job
  needs an explicit budget.
- **State transitions are observable.** Ship metrics and structured events with
  the code that owns the transition, not later.
- **Respect package boundaries.** Keep the dependency direction described in the
  README, expose the narrowest visibility that works, and do not add
  speculative interfaces to packages that hold only an ownership boundary.
- **Public files describe what exists.** Documentation, configuration, and
  comments describe current behavior and commands that currently work.
- **Never commit build output.** Generated artifacts such as
  `compile_commands.json` stay untracked; `//tools:check_generated_artifacts`
  enforces this.

## Commit and review

- Keep a commit to one reviewable change with a message that explains why, not
  only what.
- State in the description which of the commands above you ran, and note any you
  could not run and why.
- Report failures honestly. A known-failing check is not waived by documenting
  it.

## Security

Do not open a public issue for a suspected vulnerability. Follow
[SECURITY.md](SECURITY.md).

## Conduct

Participation is governed by the [code of conduct](CODE_OF_CONDUCT.md).
