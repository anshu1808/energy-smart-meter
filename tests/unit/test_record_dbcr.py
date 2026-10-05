import record_dbcr

PLAN = """-- Lock Database
UPDATE DATABASECHANGELOGLOCK SET LOCKED = 1;

-- Changeset changelogs/2026.09.001-create-audit-dbcr.yml::2026.09.001-create-audit-dbcr::anshu
CREATE TABLE dev_energy.audit.dbcr (dbcr_id STRING);
INSERT INTO DATABASECHANGELOG (ID) VALUES (x);

-- Changeset changelogs/2026.09.009-drop-old.yml::2026.09.009-drop-old::anshu
DROP TABLE dev_energy.gold.old_table;
"""


def test_parse_plan_two_changesets():
    rows = record_dbcr.parse_plan(PLAN)
    assert [r["changeset_id"] for r in rows] == ["2026.09.001-create-audit-dbcr", "2026.09.009-drop-old"]
    assert rows[0]["operation"] == "CREATE" and not rows[0]["is_destructive"]
    assert rows[0]["objects"] == "dev_energy.audit.dbcr"
    assert rows[1]["operation"] == "DROP" and rows[1]["is_destructive"]


def test_empty_plan():
    assert record_dbcr.parse_plan("-- nothing pending") == []


def test_operation_classification_for_security_statements():
    plan = """-- Changeset changelogs/a.yml::a::anshu
GRANT USE CATALOG ON CATALOG c TO `g`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA c.silver TO `g`;
-- Changeset changelogs/b.yml::b::anshu
REVOKE SELECT ON TABLE c.gold.t FROM `g`;
-- Changeset changelogs/c.yml::c::anshu
MERGE INTO c.silver.f AS f USING c.silver.d AS d ON f.k = d.k WHEN MATCHED THEN UPDATE SET f.r = d.r;
-- Changeset changelogs/d.yml::d::anshu
CREATE OR REPLACE FUNCTION c.security.fn(x STRING) RETURN x;
"""
    ops = {r["changeset_id"]: r["operation"] for r in record_dbcr.parse_plan(plan)}
    assert ops == {"a": "GRANT", "b": "REVOKE", "c": "MERGE", "d": "CREATE"}


def test_real_liquibase_plan_is_accepted_by_guard_and_parser():
    """tests/fixtures/liquibase_plan_sample.sql is genuine `liquibase updateSQL` output for sql/ddl."""
    import pathlib
    import subprocess
    import sys

    root = pathlib.Path(__file__).resolve().parents[2]
    plan = (root / "tests" / "fixtures" / "liquibase_plan_sample.sql").read_text(encoding="utf-8")
    assert "${" not in plan, "unsubstituted changelog parameter reached the SQL"
    r = subprocess.run(
        [sys.executable, str(root / "ci" / "dbcr_guard.py")],
        input=plan,
        text=True,
        capture_output=True,
        env={"PATH": ""},
    )
    assert r.returncode == 0, r.stderr
    rows = record_dbcr.parse_plan(plan)
    assert len(rows) == 17 and not any(x["is_destructive"] for x in rows)
    assert all(x["operation"] != "UNKNOWN" for x in rows)
