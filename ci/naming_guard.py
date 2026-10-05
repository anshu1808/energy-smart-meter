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

BASELINE_LEGACY_COLUMNS = {
    ("bronze-eia-001", "bronze_eia"): {
        "respondent-name": "STRING",
        "type-name": "STRING",
        "value-units": "STRING",
        "_source_system": "STRING",
        "_load_id": "STRING",
        "_ingestion_ts": "TIMESTAMP",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("bronze-meter-readings-001", "bronze_meter_readings"): {
        "LCLid": "STRING",
        "stdorToU": "STRING",
        "DateTime": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("bronze-tariff-001", "bronze_tariff"): {
        "TariffDateTime": "TIMESTAMP",
        "Tariff": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("bronze-weather-001", "bronze_weather"): {
        "_source_system": "STRING",
        "_ingestion_ts": "TIMESTAMP",
        "_load_id": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("feeder-readings-001", "bronze_feeder_readings"): {
        "DateTime": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("meter-events-001", "bronze_meter_events"): {
        "LCLid": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("dim-customer-001", "dim_customer"): {
        "LCLid": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("dim-date-001", "dim_date"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("dim-meter-001", "dim_meter"): {
        "LCLid": "STRING",
        "stdorToU": "STRING",
        "DateTime": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("dim-tariff-001", "dim_tariff"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-billing-001", "fact_billing"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-consumption-001", "fact_consumption"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-eia-demand-001", "fact_eia_demand"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-feeder-readings-001", "fact_feeder_readings"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-meter-events-001", "fact_meter_events"): {
        "LCLid": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("fact-weather-001", "fact_weather"): {
        "schema_drift_detected": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("gold-peak-load-001", "gold_peak_load"): {
        "dq_passed": "BOOLEAN",
    },
    ("gold-theft-detection-001", "gold_theft_detection"): {
        "LCLid": "STRING",
        "theft_flag": "BOOLEAN",
        "dq_passed": "BOOLEAN",
    },
    ("gold-weather-impact-001", "gold_weather_impact"): {
        "dq_passed": "BOOLEAN",
    },
}


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
        return [
            path
            for path in diff.stdout.splitlines()
            if path.replace("\\", "/").endswith(".yml")
            and path.replace("\\", "/").startswith(
                (
                    "sql/ddl/changelogs/",
                    "sql/ddl/audit/",
                    "sql/ddl/bronze/",
                    "sql/ddl/silver/",
                    "sql/ddl/gold/",
                    "sql/ddl/security/",
                )
            )
        ]
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


def is_approved_legacy_baseline_column(changeset: dict, table: str, column: str, col_type: str) -> bool:
    """Allow only verified existing column names in their matching table baseline."""
    labels = changeset.get("labels", "")
    label_set = {label.strip() for label in labels.split(",")} if isinstance(labels, str) else set(labels)
    if "baseline" not in label_set:
        return False

    expected_type = BASELINE_LEGACY_COLUMNS.get((changeset.get("id"), table), {}).get(column)
    return expected_type == col_type.upper()


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
                        if is_approved_legacy_baseline_column(changeset, table, col_name, col_type):
                            continue
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
