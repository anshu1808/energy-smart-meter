import sys
from pathlib import Path

import naming_guard
import yaml

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "sql" / "ddl"
BRONZE_SCHEMAS = {
    "bronze-eia-001.yml": {
        "period": "STRING",
        "respondent": "STRING",
        "respondent-name": "STRING",
        "type": "STRING",
        "type-name": "STRING",
        "value": "STRING",
        "value-units": "STRING",
        "_source_system": "STRING",
        "_load_id": "STRING",
        "_ingestion_ts": "TIMESTAMP",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "source_file": "STRING",
        "source_file_name": "STRING",
        "source_file_size": "BIGINT",
        "source_file_modification_time": "TIMESTAMP",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
    "bronze-meter-readings-001.yml": {
        "LCLid": "STRING",
        "stdorToU": "STRING",
        "DateTime": "STRING",
        "kwh_hh": "STRING",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
    "bronze-tariff-001.yml": {
        "TariffDateTime": "TIMESTAMP",
        "Tariff": "STRING",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "source_file": "STRING",
        "source_file_name": "STRING",
        "source_file_size": "BIGINT",
        "source_file_modification_time": "TIMESTAMP",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
    "bronze-weather-001.yml": {
        "humidity": "BIGINT",
        "precipitation": "DOUBLE",
        "temperature": "DOUBLE",
        "timestamp": "STRING",
        "wind_speed": "DOUBLE",
        "_source_system": "STRING",
        "_ingestion_ts": "TIMESTAMP",
        "_load_id": "STRING",
        "load_timestamp": "TIMESTAMP",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "source_file": "STRING",
        "source_file_name": "STRING",
        "source_file_size": "BIGINT",
        "source_file_modification_time": "TIMESTAMP",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
    "feeder-readings-001.yml": {
        "feeder_id": "STRING",
        "substation_id": "STRING",
        "DateTime": "STRING",
        "voltage_v": "DOUBLE",
        "current_a": "DOUBLE",
        "active_power_kw": "DOUBLE",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
    "meter-events-001.yml": {
        "event_id": "STRING",
        "LCLid": "STRING",
        "event_type": "STRING",
        "severity": "STRING",
        "event_timestamp": "TIMESTAMP",
        "load_ts": "TIMESTAMP",
        "source_system": "STRING",
        "schema_drift_detected": "BOOLEAN",
        "schema_drift_columns": "STRING",
        "dq_passed": "BOOLEAN",
        "dq_failed_checks": "STRING",
    },
}
SILVER_TABLES = {
    "dim-customer-001.yml": ("dim_customer", 12),
    "dim-date-001.yml": ("dim_date", 15),
    "dim-meter-001.yml": ("dim_meter", 16),
    "dim-tariff-001.yml": ("dim_tariff", 13),
    "fact-billing-001.yml": ("fact_billing", 20),
    "fact-consumption-001.yml": ("fact_consumption", 12),
    "fact-eia-demand-001.yml": ("fact_eia_demand", 15),
    "fact-feeder-readings-001.yml": ("fact_feeder_readings", 14),
    "fact-meter-events-001.yml": ("fact_meter_events", 13),
    "fact-weather-001.yml": ("fact_weather", 13),
    "dq-failures-001.yml": ("dq_failures", 7),
}
GOLD_TABLES = {
    "gold-peak-load-001.yml": ("gold_peak_load", 10),
    "gold-theft-detection-001.yml": ("gold_theft_detection", 13),
    "gold-weather-impact-001.yml": ("gold_weather_impact", 11),
}
TABLE_NAMES = {
    "bronze-eia-001.yml": "bronze_eia",
    "bronze-meter-readings-001.yml": "bronze_meter_readings",
    "bronze-tariff-001.yml": "bronze_tariff",
    "bronze-weather-001.yml": "bronze_weather",
    "feeder-readings-001.yml": "bronze_feeder_readings",
    "meter-events-001.yml": "bronze_meter_events",
}


def changeset_at(path):
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return document["databaseChangeLog"][0]["changeSet"]


def test_master_has_explicit_layer_ordered_includes():
    document = yaml.safe_load((DDL / "changelog-master.yml").read_text(encoding="utf-8"))
    included = [entry["include"]["file"] for entry in document["databaseChangeLog"]]
    expected = (
        ["audit/audit-dbcr-001.yml"]
        + [f"bronze/{name}" for name in BRONZE_SCHEMAS]
        + ["bronze/2026.10.100-create-bronze-raw-volume.yml"]
        + [f"silver/{name}" for name in SILVER_TABLES]
        + [
            "silver/2026.10.130-add-region-to-fact-consumption.yml",
            "silver/2026.10.131-add-region-to-fact-billing.yml",
        ]
        + [f"gold/{name}" for name in GOLD_TABLES]
    )

    assert included == expected
    assert all((DDL / path).is_file() for path in included)
    assert not any("security/" in path for path in included)
    assert "gold/gold-revenue-summary-001.yml" not in included
    assert not (DDL / "gold" / "gold-revenue-summary-001.yml").exists()


def test_bronze_baselines_match_live_catalog_schemas():
    for filename, expected_columns in BRONZE_SCHEMAS.items():
        path = DDL / "bronze" / filename
        changeset = changeset_at(path)
        table = changeset["changes"][0]["createTable"]
        columns = table["columns"]

        assert changeset["id"] == path.stem
        assert changeset["author"] == "anshu"
        assert changeset["runInTransaction"] is False
        assert changeset["preConditions"][0]["onFail"] == "MARK_RAN"
        assert table["catalogName"] == "${energy_catalog}"
        assert table["schemaName"] == "bronze"
        assert table["tableName"] == TABLE_NAMES[filename]
        assert {column["column"]["name"]: column["column"]["type"] for column in columns} == expected_columns
        assert all("constraints" not in column["column"] for column in columns)


def test_dbcr_changeset_is_relocated_and_retains_table_definition():
    path = DDL / "audit" / "audit-dbcr-001.yml"
    changeset = changeset_at(path)
    table = changeset["changes"][0]["createTable"]

    assert changeset["id"] == "audit-dbcr-001"
    assert table["tableName"] == "dbcr"
    assert table["schemaName"] == "audit"
    assert table["catalogName"] == "${energy_catalog}"
    assert not (DDL / "changelogs" / "2026.09.001-create-audit-dbcr.yml").exists()


def test_silver_and_gold_baselines_match_live_table_counts_and_names():
    for directory, expected_tables in (("silver", SILVER_TABLES), ("gold", GOLD_TABLES)):
        for filename, (table_name, column_count) in expected_tables.items():
            path = DDL / directory / filename
            changeset = changeset_at(path)
            table = changeset["changes"][0]["createTable"]

            assert changeset["id"] == path.stem
            assert changeset["author"] == "anshu"
            assert changeset["runInTransaction"] is False
            assert changeset["preConditions"][0]["onFail"] == "MARK_RAN"
            assert table["catalogName"] == "${energy_catalog}"
            assert table["schemaName"] == directory
            assert table["tableName"] == table_name
            assert len(table["columns"]) == column_count
            assert all("constraints" not in column["column"] for column in table["columns"])


def test_table_baselines_pass_naming_guard(monkeypatch):
    paths = [str(DDL / "audit" / "audit-dbcr-001.yml")]
    paths.extend(
        str(path) for directory in ("bronze", "silver", "gold") for path in sorted((DDL / directory).glob("*-001.yml"))
    )
    monkeypatch.setattr(naming_guard, "get_changed_files", lambda: paths)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "naming_guard.py",
            "--changed",
            "--rules",
            str(ROOT / "config" / "schema" / "conventions.yml"),
        ],
    )

    assert naming_guard.main() == 0
