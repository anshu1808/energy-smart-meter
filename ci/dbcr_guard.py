#!/usr/bin/env python3
"""
ci/dbcr_guard.py — runs on the output of `liquibase updateSQL`.

Enforces the DBCR policy:

- CREATE CATALOG / SCHEMA / DATABASE -> hard blocked
- CREATE TABLE / ALTER TABLE -> allowed
- DROP / DELETE / TRUNCATE -> held for approval

Liquibase-generated DATABASECHANGELOG and
DATABASECHANGELOGLOCK statements are ignored because they
are internal Liquibase bookkeeping operations.

Exit codes:
    0 = passed
    1 = blocked or held
"""

import os
import re
import sys

# -------------------------------------------------------------------
# Catalog / schema creation
# -------------------------------------------------------------------

BLOCK_PATTERNS = [
    r"\bCREATE\s+CATALOG\b",
    r"\bCREATE\s+SCHEMA\b",
    r"\bCREATE\s+DATABASE\b",
]


# -------------------------------------------------------------------
# Destructive operations
# -------------------------------------------------------------------

HOLD_PATTERNS = [
    r"\bDROP\b",
    r"\bDELETE\s+FROM\b",
    r"\bTRUNCATE\b",
]


# -------------------------------------------------------------------
# Liquibase internal tables
# -------------------------------------------------------------------

LIQUIBASE_INTERNAL_TABLES = (
    "DATABASECHANGELOG",
    "DATABASECHANGELOGLOCK",
)


def remove_sql_comments(sql: str) -> str:
    """
    Remove SQL single-line comments beginning with --.

    This is important because Liquibase updateSQL contains comments
    such as:

        -- Create Database Lock Table

    which would otherwise match CREATE DATABASE.
    """

    return re.sub(r"--[^\n\r]*", "", sql)


def is_liquibase_internal_statement(statement: str) -> bool:
    """
    Ignore statements that operate only on Liquibase's own
    DATABASECHANGELOG / DATABASECHANGELOGLOCK tables.
    """

    normalized = re.sub(r"\s+", " ", statement.upper()).strip()

    return any(
        re.search(
            rf"\b{re.escape(table)}\b",
            normalized,
        )
        for table in LIQUIBASE_INTERNAL_TABLES
    )


def get_application_statements(sql: str) -> list[str]:
    """
    Extract SQL statements after removing comments and Liquibase
    internal bookkeeping statements.
    """

    sql_without_comments = remove_sql_comments(sql)

    statements = sql_without_comments.split(";")

    application_statements = []

    for statement in statements:
        statement = statement.strip()

        if not statement:
            continue

        if is_liquibase_internal_statement(statement):
            continue

        application_statements.append(statement)

    return application_statements


def main() -> int:
    raw_sql = sys.stdin.read()

    if not raw_sql.strip():
        print("No pending changesets — nothing to guard.")
        return 0

    statements = get_application_statements(raw_sql)

    if not statements:
        print("No application DDL detected — Liquibase metadata only.")
        return 0

    application_sql = "\n".join(statements).upper()

    # ---------------------------------------------------------------
    # Hard block: catalog/schema/database creation
    # ---------------------------------------------------------------

    for pattern in BLOCK_PATTERNS:
        match = re.search(pattern, application_sql)

        if match:
            print(
                "BLOCKED: catalog/schema/database creation is "
                "Databricks-Admin-only and cannot be created via CI "
                f"(matched: {match.group(0)}).\n"
                "If a new catalog or schema is genuinely needed, "
                "raise it with the platform team — it is provisioned "
                "out of band.",
                file=sys.stderr,
            )
            return 1

    # ---------------------------------------------------------------
    # Hold: destructive operations
    # ---------------------------------------------------------------

    for pattern in HOLD_PATTERNS:
        match = re.search(pattern, application_sql)

        if match:
            if os.environ.get("CONFIRM_DESTRUCTIVE", "").lower() != "true":
                print(
                    "HOLD: destructive DDL detected "
                    "(DROP/DELETE/TRUNCATE).\n"
                    "This requires the confirm-destructive MR label "
                    "plus the required approvers (see Section 10 of "
                    "the DBCR doc) before re-running this job with "
                    'CONFIRM_DESTRUCTIVE="true".',
                    file=sys.stderr,
                )
                return 1

    print("DDL guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
