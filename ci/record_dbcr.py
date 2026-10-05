#!/usr/bin/env python3
"""
ci/record_dbcr.py - after `liquibase update`, write one business-readable row per
applied changeset to <catalog>.audit.dbcr (the DBCR audit table).

Input : plan.sql produced by `liquibase updateSQL` BEFORE the update (it lists exactly
        the changesets that were pending, so only what this run applied is recorded).
Env   : DBX_HOST, DBX_WAREHOUSE_ID, DBX_SP_CLIENT_ID, DBX_SP_SECRET, ENV, CATALOG,
        WORK_ITEM_ID, GITHUB_RUN_ID, GITHUB_SHA, GITHUB_ACTOR, MR_APPROVERS (optional)
"""

import datetime
import os
import re
import sys

CHANGESET_MARKER = re.compile(r"^--\s*Changeset\s+(?P<file>.+?)::(?P<id>.+?)::(?P<author>.+?)\s*$", re.M)
DESTRUCTIVE = re.compile(r"\b(DROP|DELETE\s+FROM|TRUNCATE)\b", re.I)
OPERATIONS = {
    "CREATE",
    "ALTER",
    "DROP",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "COMMENT",
    "SET",
    "GRANT",
    "REVOKE",
    "MERGE",
    "REORG",
}
OBJECTS = re.compile(r"(?:TABLE|VIEW|FUNCTION|VOLUME)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?([`\w.\-]+)", re.I)
INTERNAL = re.compile(r"DATABASECHANGELOG", re.I)


def parse_plan(plan_sql: str) -> list[dict]:
    """Split Liquibase updateSQL output into one dict per pending changeset."""
    matches = list(CHANGESET_MARKER.finditer(plan_sql))
    rows = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(plan_sql)
        body = re.sub(r"--[^\n\r]*", "", plan_sql[m.end() : end])
        statements = [s.strip() for s in body.split(";") if s.strip() and not INTERNAL.search(s)]
        text = "\n".join(statements)
        first = statements[0].split(None, 1)[0].upper() if statements else ""
        op = first if first in OPERATIONS else "UNKNOWN"
        rows.append(
            {
                "changeset_id": m.group("id"),
                "author": m.group("author"),
                "operation": op,
                "objects": ",".join(dict.fromkeys(OBJECTS.findall(text))) or "unknown",
                "is_destructive": bool(DESTRUCTIVE.search(text)),
            }
        )
    return rows


def get_connection():
    # imported lazily so unit tests run without the connector installed
    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    host = os.environ["DBX_HOST"].replace("https://", "")
    cfg = Config(
        host=f"https://{host}",
        client_id=os.environ["DBX_SP_CLIENT_ID"],
        client_secret=os.environ["DBX_SP_SECRET"],
    )
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{os.environ['DBX_WAREHOUSE_ID']}",
        credentials_provider=lambda: oauth_service_principal(cfg),
    )


def main(plan_path: str = "sql/ddl/plan.sql") -> int:
    with open(plan_path, encoding="utf-8") as f:
        rows = parse_plan(f.read())
    if not rows:
        print("No pending changesets were applied - nothing to record.")
        return 0

    env = os.environ["ENV"]
    table = os.environ.get("DBCR_AUDIT_TABLE", f"{os.environ['CATALOG']}.audit.dbcr")
    dbcr_id = f"{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_SHA', 'unknown')[:7]}"
    with get_connection() as con, con.cursor() as cur:
        for r in rows:
            cur.execute(
                f"INSERT INTO {table} (dbcr_id, work_item_id, changeset_id, author, approvers, environment, "
                "operation, objects, is_destructive, is_rollback_available, checksum, git_commit, applied_at, status) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    dbcr_id,
                    os.environ.get("WORK_ITEM_ID", "UNSET"),
                    r["changeset_id"],
                    os.environ.get("GITHUB_ACTOR", "ci"),
                    os.environ.get("MR_APPROVERS", ""),
                    env,
                    r["operation"],
                    r["objects"],
                    r["is_destructive"],
                    True,
                    "",
                    os.environ.get("GITHUB_SHA", ""),
                    datetime.datetime.now(datetime.timezone.utc),
                    "applied",
                ],
            )
    print(f"DBCR: recorded {len(rows)} changeset(s) as {dbcr_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:2]))
