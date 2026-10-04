# Contributing to Kwaque

Kwaque is under active development and is not yet accepting external feature
contributions. The maintainers will publish the review and acceptance
requirements before that changes. Issue reports and questions are welcome now.

This file records what every change is expected to pass, whoever writes it.

## Before you start

Install the build prerequisites listed in the
[README requirements section](README.md#requirements). No project-specific
system packages are needed; the build fetches its own compiler and dependencies.

Optionally install the commit hooks. They run the formatting, lint and
generated-artifact checks that continuous integration also enforces, plus
whitespace, line-ending and end-of-file fixes that it does not:

```bash
pre-commit install
```

## What every change must pass

The [README development section](README.md#development) contains the canonical
commands. Before landing a change, run the ordinary debug suite, release build,
sanitizer suite, determinism goldens, bounded fuzz smoke, C++, Bazel and Python
formatting and lint, and repository checks. Run ordinary and strict-production
clang-tidy with a fresh debug compilation database, then ordinary clang-tidy for
the separate fuzz configuration. Every enabled clang-tidy check is an error, in
headers as well as sources. Keep commands using the same configuration together.
Merging requires the `CI result` check, which passes only when every CI job
passes.

Changes to real adapters or environment ownership must also pass the ten-run
sandboxed contract suite. The scheduled stateful fuzz campaigns retain crash
inputs and logs; include a fixed failing input in the checked-in corpus with its
regression fix. Use the paired release benchmark protocol when changing the work
or overhead covered by an existing comparison.

For a quick check of repository-tool changes:

```bash
python3 -B -m unittest discover -s tools -p '*_test.py'
python3 -B -m unittest bazel.fuzz_test_wrapper_test bazel.stamp_template_test
```

Changes that add or update a dependency must also run `bazel mod tidy` and
commit the resulting lockfile, and must follow the procedure in
[DEPENDENCIES.md](DEPENDENCIES.md). A dependency compiled or linked into the
packaged binaries also needs its license under `licenses/` in the package;
`python3 tools/check_package_licenses.py` reports a missing or stale one.

## Expectations for a change

- **Tests come with the code.** New behavior needs a test at the lowest level
  that can observe it: a pure unit test where possible, a reactor test when
  Seastar is involved, a subprocess smoke test for process-level behavior.
- **Capturing lambda coroutines take `this auto`.** A coroutine lambda's
  captures live in the lambda object, which can be destroyed before the
  coroutine finishes. Declare the explicit object parameter `this auto` so the
  captures move into the coroutine frame; strict clang-tidy enforces this.
- **Parsers get fuzz coverage.** Any new code that decodes untrusted bytes needs
  a bounded fuzz target and a seed corpus. Tag it `fuzz-campaign` so the
  scheduled campaign runs it; the PR smoke and the corpus replay test pick it up
  without further changes.
- **Native processes run under the test macros.** A Python test that starts a
  native binary uses `kwaque_py_native_test`, so sanitizer reports are fatal
  and the selected reactor backend applies; a repository check enforces this.
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
