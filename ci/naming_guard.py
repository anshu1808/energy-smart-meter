#!/usr/bin/env python3
"""
ci/naming_guard.py — parses changed Liquibase changeset files (YAML)
and checks every table/column identifier against
config/schema/conventions.yml.

Usage:
    python ci/naming_guard.py --changed --rules <path-to-conventions.yml>

Exit codes:
    0 = all identifiers compliant
    1 = one or more violations found, MR blocked
"""

import argparse
import os
import re
import subprocess
import sys

import yaml


def get_changed_files() -> list[str]:
    """Return changed changelog YAML files vs the MR target branch."""
    base = os.environ.get("BASE_REF", "origin/dev")  # GitHub Actions sets origin/<PR base branch>
    try:
        diff = subprocess.run(
            ["git", "diff", "--name-only", f"{base}...HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return [f for f in diff.stdout.splitlines() if f.endswith(".yml") and "changelogs/" in f]
    except subprocess.CalledProcessError:
        return []


def load_rules(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def check_identifier(name: str, case_regex: str) -> list[str]:
    errors = []
    if not re.match(case_regex, name):
        errors.append(f"case violation (expected lower_snake) fix: {name.lower().replace('-', '_')}")
    if len(name) > 63:
        errors.append("exceeds 63 character limit")
    return errors


def check_table_name(table: str, catalog: str, rules: dict) -> list[str]:
    errors = []
    layer = None
    for candidate in ("bronze", "silver", "gold"):
        if candidate in catalog.lower():
            layer = candidate
            break
    if layer and layer in rules.get("tables", {}):
        regex = rules["tables"][layer]["regex"]
        if not re.match(regex, table):
            errors.append(f"table '{table}' does not match {layer} pattern {regex}")
    return errors


def check_column_name(column: str, col_type: str, rules: dict) -> list[str]:
    errors = []
    if column in rules.get("audit_allowed", []):
        return errors
    if column.startswith("_"):
        errors.append(
            f"column '{column}' uses reserved audit-column prefix without being one of {rules.get('audit_allowed')}"
        )
    if col_type and col_type.upper() == "BOOLEAN" and not re.match(rules["columns"]["boolean"], column):
        errors.append(f"column '{column}' is boolean but must start is_/has_")
    return errors


def iter_changesets(doc) -> list[dict]:
    """databaseChangeLog is a list of items; changeSets may be nested
    inside include/includeAll (skip those) or given directly."""
    entries = doc.get("databaseChangeLog", [])
    for entry in entries:
        if isinstance(entry, dict) and "changeSet" in entry:
            yield entry["changeSet"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed", action="store_true")
    parser.add_argument("--rules", required=True)
    args = parser.parse_args()

    rules = load_rules(args.rules)
    files = get_changed_files() if args.changed else []

    violations = 0
    for path in files:
        try:
            with open(path) as f:
                doc = yaml.safe_load(f)
        except (yaml.YAMLError, FileNotFoundError):
            continue
        if not doc:
            continue

        for changeset in iter_changesets(doc):
            for change in changeset.get("changes", []):
                if "createTable" in change:
                    ct = change["createTable"]
                    table = ct.get("tableName", "")
                    # layer comes from catalog OR schema (single-catalog design)
                    catalog = f"{ct.get('catalogName', '')} {ct.get('schemaName', '')}"
                    for err in check_identifier(table, rules["identifiers"]["case"]):
                        print(f"FAIL {path}\n  table '{table}' -> {err}")
                        violations += 1
                    for err in check_table_name(table, catalog, rules):
                        print(f"FAIL {path}\n  {err}")
                        violations += 1
                    for col_entry in ct.get("columns", []):
                        col = col_entry.get("column", {})
                        col_name = col.get("name", "")
                        col_type = col.get("type", "")
                        for err in check_identifier(col_name, rules["identifiers"]["case"]):
                            print(f"FAIL {path}\n  column '{col_name}' -> {err}")
                            violations += 1
                        for err in check_column_name(col_name, col_type, rules):
                            print(f"FAIL {path}\n  {err}")
                            violations += 1

                if "addColumn" in change:
                    ac = change["addColumn"]
                    for col_entry in ac.get("columns", []):
                        col = col_entry.get("column", {})
                        col_name = col.get("name", "")
                        for err in check_identifier(col_name, rules["identifiers"]["case"]):
                            print(f"FAIL {path}\n  column '{col_name}' -> {err}")
                            violations += 1

    if violations:
        print(f"\n{violations} violation(s). Merge request blocked.")
        return 1

    print("Naming guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
