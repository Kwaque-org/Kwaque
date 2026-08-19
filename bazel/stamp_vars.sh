#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace_dir="${script_dir}/.."

revision="unknown"
dirty="0"

if git -C "${workspace_dir}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  revision="$(git -C "${workspace_dir}" rev-parse HEAD)"
  if [[ -n "$(git -C "${workspace_dir}" status --porcelain --untracked-files=normal)" ]]; then
    dirty="1"
  fi
fi

build_timestamp="${SOURCE_DATE_EPOCH:-0}"
if [[ ! "${build_timestamp}" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be an unsigned integer" >&2
  exit 1
fi

echo "STABLE_KWAQUE_GIT_REVISION ${revision}"
echo "STABLE_KWAQUE_GIT_DIRTY ${dirty}"
echo "STABLE_KWAQUE_BUILD_TIMESTAMP ${build_timestamp}"
