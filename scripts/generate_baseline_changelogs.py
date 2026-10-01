#!/usr/bin/env python3
"""
scripts/generate_baseline_changelogs.py

Reads the tables that ALREADY exist in your workspace and writes one Liquibase baseline
changeset per table into sql/ddl/changelogs/. Existing tables are recorded as "already applied"
(preCondition + MARK_RAN); fresh environments (qa/prod) get the table created.

Run locally (uses your Databricks CLI profile, e.g. DEFAULT):
  python scripts/generate_baseline_changelogs.py --catalog energy \
      --schemas bronze silver gold audit --catalog-template '${energy_catalog}' \
      --author anshu --work-item 12 --start 2

  --catalog           catalog where your tables live today
  --schemas           schemas to baseline
  --catalog-template  catalog written into changelogs. Use '${energy_catalog}' (single quotes);
                      CI resolves it per environment
"""

import argparse
import datetime
import pathlib


def render_changeset(seq, schema, table, columns, catalog_tpl, author, work_item, layer, yyyymm):
    cs_id = f"{yyyymm}.{seq:03d}-baseline-{schema}-{table}".lower()
    cat = f'"{catalog_tpl}"'
    col_lines = []
    for c in columns:
        line = f'              - column: {{ name: {c["name"]}, type: "{c["type"].upper()}"'
        if not c.get("nullable", True):
            line += ", constraints: { nullable: false }"
        col_lines.append(line + " }")
    text = (
        "# Baseline of an EXISTING table (generated). IMMUTABLE once merged: never edit, add a new changeset.\n"
        "databaseChangeLog:\n"
        "  - changeSet:\n"
        f"      id: {cs_id}\n"
        f"      author: {author}\n"
        f"      labels: ddl,create,{layer},baseline\n"
        "      context: all\n"
        "      runInTransaction: false\n"
        f'      comment: "{work_item} Baseline {schema}.{table}"\n'
        "      preConditions:\n"
        "        - onFail: MARK_RAN\n"
        "        - not:\n"
        f"            - tableExists: {{ tableName: {table}, schemaName: {schema}, catalogName: {cat} }}\n"
        "      changes:\n"
        "        - createTable:\n"
        f"            tableName: {table}\n"
        f"            schemaName: {schema}\n"
        f"            catalogName: {cat}\n"
        "            columns:\n" + "\n".join(col_lines) + "\n"
        "      rollback:\n"
        f"        - dropTable: {{ tableName: {table}, schemaName: {schema}, catalogName: {cat} }}\n"
    )
    return f"{cs_id}.yml", text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schemas", nargs="+", required=True)
    ap.add_argument("--catalog-template", required=True)
    ap.add_argument("--author", default="data-engineering")
    ap.add_argument("--work-item", default="TICKET")
    ap.add_argument("--start", type=int, default=2, help="first sequence number (001 is the audit table)")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--out", default="sql/ddl/changelogs")
    a = ap.parse_args()

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    yyyymm = datetime.date.today().strftime("%Y.%m")
    seq = a.start
    for schema in a.schemas:
        for t in sorted(w.tables.list(catalog_name=a.catalog, schema_name=schema), key=lambda x: x.name):
            if str(t.table_type).upper().endswith("VIEW"):
                print(f"skip view {schema}.{t.name} (add a createView changeset by hand)")
                continue
            cols = [
                {"name": c.name, "type": c.type_text or "STRING", "nullable": c.nullable is not False}
                for c in (t.columns or [])
            ]
            layer = next(
                (x for x in ("bronze", "silver", "gold") if x in schema.lower() or x in a.catalog.lower()), "table"
            )
            name, text = render_changeset(
                seq, schema, t.name, cols, a.catalog_template, a.author, a.work_item, layer, yyyymm
            )
            (out / name).write_text(text, encoding="utf-8", newline="\n")
            print("wrote", out / name)
            seq += 1


if __name__ == "__main__":
    main()
