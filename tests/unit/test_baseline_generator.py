import yaml
from generate_baseline_changelogs import render_changeset


def test_generated_changeset_is_valid_yaml_and_immutable_shape():
    name, text = render_changeset(
        2,
        "bronze",
        "meter_readings",
        [{"name": "meter_id", "type": "string", "nullable": False}, {"name": "kwh", "type": "decimal(18,4)"}],
        "${energy_catalog}",
        "anshu",
        "12",
        "bronze",
        "2026.09",
    )
    assert name == "2026.09.002-baseline-bronze-meter_readings.yml"
    cs = yaml.safe_load(text)["databaseChangeLog"][0]["changeSet"]
    assert cs["runInTransaction"] is False
    assert cs["preConditions"][0]["onFail"] == "MARK_RAN"
    assert cs["changes"][0]["createTable"]["catalogName"] == "${energy_catalog}"
    assert cs["rollback"][0]["dropTable"]["tableName"] == "meter_readings"
    cols = cs["changes"][0]["createTable"]["columns"]
    assert cols[0]["column"]["constraints"] == {"nullable": False} and cols[1]["column"]["type"] == "DECIMAL(18,4)"
