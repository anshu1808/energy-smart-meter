#!/usr/bin/env python3
"""
scripts/bootstrap_environment.py - ONE-TIME admin setup of an environment's catalog and schemas.

Why this is not a Liquibase changeset: CI is deliberately blocked from CREATE CATALOG / CREATE SCHEMA
(ci/dbcr_guard.py) and the deploy service principal should not hold metastore-level rights.
Everything inside the schemas (tables, volumes, grants, masks, filters, views) IS Liquibase.

Usage (your own login, needs CREATE CATALOG on the metastore):
  python scripts/bootstrap_environment.py --catalog energy_dev --warehouse-id <id> --dry-run
  python scripts/bootstrap_environment.py --catalog energy_dev --warehouse-id <id>
Run it for energy_dev and energy_qa. For prod (energy) everything already exists, running it is a no-op.
"""

import argparse
import re

SCHEMAS = ["bronze", "silver", "gold", "audit", "security", "liquibase"]
CATALOG_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def bootstrap_statements(catalog: str) -> list[str]:
    if not CATALOG_RE.match(catalog):
        raise ValueError(f"invalid catalog name: {catalog!r} (lower_snake_case only)")
    return [f"CREATE CATALOG IF NOT EXISTS {catalog}"] + [f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}" for s in SCHEMAS]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    statements = bootstrap_statements(a.catalog)
    if a.dry_run:
        print("\n".join(s + ";" for s in statements))
        return

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()
    for s in statements:
        res = w.statement_execution.execute_statement(warehouse_id=a.warehouse_id, statement=s, wait_timeout="30s")
        if res.status.state != StatementState.SUCCEEDED:
            raise SystemExit(f"FAILED: {s}\n{res.status.error}")
        print("ok:", s)

    print(
        "\nManual steps still needed (not automatable here):\n"
        "  1. Secret scope (once per workspace):  databricks secrets create-scope energy-secrets\n"
        "  2. Add the environment's service principal to account group 'energy_data_engineer' (see docs/LIQUIBASE.md)\n"
        f"  3. Let the deploy principal manage the catalog:\n"
        f"     ALTER CATALOG {a.catalog} OWNER TO `<service-principal-application-id>`"
    )


if __name__ == "__main__":
    main()
