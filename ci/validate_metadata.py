#!/usr/bin/env python3
"""
ci/validate_metadata.py — validates every changed project's
metadata.yml against the required field list (architecture doc
Section 9.1). CI fails the MR if metadata.yml is missing, malformed,
or has an unfilled placeholder left in it.

Usage:
    python ci/validate_metadata.py --changed
    python ci/validate_metadata.py --path projects/valmont_poc/metadata.yml
"""

import argparse
import os
import subprocess
import sys

import yaml

REQUIRED_FIELDS = [
    "project_name",
    "project_type",
    "purpose",
    "source",
    "ingestion_type",
    "frequency",
    "layers",
    "tables_impacted",
    "upstream_pipelines",
    "downstream_pipelines",
    "developer_name",
    "developer_team",
    "consumer_teams",
    "data_classification",
    "ticket_id",
    "sla",
    "contains_pii",
]

VALID_PROJECT_TYPES = {"ingestion", "analytical"}
VALID_INGESTION_TYPES = {"batch", "streaming", "cdc", "incremental", "full"}
VALID_CLASSIFICATIONS = {"public", "internal", "confidential", "confidential-pii", "pii"}
PLACEHOLDER_MARKER = "<FILL_IN"


def get_changed_project_dirs() -> list[str]:
    try:
        diff = subprocess.run(
            ["git", "diff", "--name-only", os.environ.get("BASE_REF", "origin/dev") + "...HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        dirs = set()
        for f in diff.stdout.splitlines():
            if f.startswith("projects/") and "/" in f[len("projects/") :]:
                dirs.add("/".join(f.split("/")[:2]))
        return sorted(dirs)
    except subprocess.CalledProcessError:
        return []


def validate_one(path: str) -> list[str]:
    errors = []
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        return [f"{path}: metadata.yml is missing"]
    except yaml.YAMLError as e:
        return [f"{path}: not valid YAML ({e})"]

    if not isinstance(data, dict):
        return [f"{path}: metadata.yml did not parse to a mapping"]

    for field in REQUIRED_FIELDS:
        if field not in data:
            errors.append(f"{path}: missing required field '{field}'")

    raw_text = yaml.dump(data)
    if PLACEHOLDER_MARKER in raw_text:
        errors.append(f"{path}: contains unfilled <FILL_IN ...> placeholder(s)")

    if data.get("project_type") not in VALID_PROJECT_TYPES:
        errors.append(f"{path}: project_type must be one of {VALID_PROJECT_TYPES}")

    if data.get("ingestion_type") not in VALID_INGESTION_TYPES:
        errors.append(f"{path}: ingestion_type must be one of {VALID_INGESTION_TYPES}")

    if data.get("data_classification") not in VALID_CLASSIFICATIONS:
        errors.append(f"{path}: data_classification must be one of {VALID_CLASSIFICATIONS}")

    if not isinstance(data.get("contains_pii"), bool):
        errors.append(f"{path}: contains_pii must be true or false (boolean)")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed", action="store_true")
    parser.add_argument("--path", help="validate a single metadata.yml directly")
    args = parser.parse_args()

    if args.path:
        paths = [args.path]
    else:
        paths = [f"{d}/metadata.yml" for d in get_changed_project_dirs()]

    if not paths:
        print("No changed project folders — nothing to validate.")
        return 0

    all_errors = []
    for path in paths:
        all_errors.extend(validate_one(path))

    if all_errors:
        print("metadata.yml validation FAILED:")
        for e in all_errors:
            print(f"  - {e}")
        return 1

    print(f"metadata.yml validation passed for: {', '.join(paths)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
