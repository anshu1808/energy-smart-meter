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
