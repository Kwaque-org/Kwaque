#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace_dir="${script_dir}/.."

revision_pattern='^[0-9a-f]{40}$'
commit_time=""

if git -C "${workspace_dir}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  revision="$(git -C "${workspace_dir}" rev-parse HEAD)"
  commit_time="$(git -C "${workspace_dir}" log -1 --pretty=%ct)"
  # Do not take the index lock; another git process may hold it.
  if [[ -n "$(git -C "${workspace_dir}" --no-optional-locks status --porcelain --untracked-files=normal)" ]]; then
    dirty="1"
  else
    dirty="0"
  fi
elif [[ -n "${KWAQUE_SOURCE_REVISION:-}" ]]; then
  # A source archive has no repository; its builder names the revision.
  revision="${KWAQUE_SOURCE_REVISION}"
  dirty="0"
else
  echo "stamped builds need a git work tree or KWAQUE_SOURCE_REVISION" >&2
  exit 1
fi

if [[ ! "${revision}" =~ ${revision_pattern} ]]; then
  echo "source revision must be 40 lowercase hexadecimal digits" >&2
  exit 1
fi

# Reproducible builds default to the time of the last commit.
build_timestamp="${SOURCE_DATE_EPOCH:-${commit_time:-0}}"
if [[ ! "${build_timestamp}" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be an unsigned integer" >&2
  exit 1
fi

echo "STABLE_KWAQUE_GIT_REVISION ${revision}"
echo "STABLE_KWAQUE_GIT_DIRTY ${dirty}"
echo "STABLE_KWAQUE_BUILD_TIMESTAMP ${build_timestamp}"
