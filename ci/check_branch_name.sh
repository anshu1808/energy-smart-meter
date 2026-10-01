#!/usr/bin/env bash
# Feature branches must be: feature/<WORK-ITEM-ID>-<kebab-case-text>   e.g. feature/12-add-liquibase-dbcr
set -euo pipefail
B="${1:-}"
[[ -z "$B" ]] && { echo "ERROR: no branch name passed"; exit 1; }
if [[ ! "$B" =~ ^feature/([0-9]+)-([a-z0-9-]+)$ ]]; then
  echo "BLOCKED: $B must match feature/<WORK-ITEM-ID>-<kebab-case-text>"; exit 1
fi
echo "Branch name $B OK"
