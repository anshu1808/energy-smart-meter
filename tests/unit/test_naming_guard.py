from types import SimpleNamespace

import naming_guard


def rules():
    import yaml

    return yaml.safe_load(open("config/schema/conventions.yml"))


def test_layer_is_detected_from_schema_when_catalog_is_shared():
    # single-catalog design: catalog gives no layer hint, schema does
    errs = naming_guard.check_table_name("customers", "${energy_catalog} silver", rules())
    assert errs and "silver" in errs[0]


def test_existing_table_names_are_compliant():
    r = rules()
    for tbl, layer in [
        ("fact_billing", "silver"),
        ("dim_date", "silver"),
        ("dq_failures", "silver"),
        ("gold_peak_load", "gold"),
        ("bronze_meter_readings", "bronze"),
    ]:
        assert naming_guard.check_table_name(tbl, f"x {layer}", r) == [], tbl


def test_changed_files_includes_new_layer_directories(monkeypatch):
    files = "\n".join(
        [
            "sql/ddl/audit/audit-dbcr-001.yml",
            "sql/ddl/bronze/bronze-meter-readings-001.yml",
            "sql/ddl/silver/dim-meter-001.yml",
            "sql/ddl/gold/gold-weather-impact-001.yml",
            "sql/ddl/security/masks.yml",
            "sql/ddl/changelogs/2026.10.130-add-region.yml",
            "sql/ddl/liquibase.properties.template",
        ]
    )
    monkeypatch.setattr(naming_guard.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=files))

    assert naming_guard.get_changed_files() == files.splitlines()[:-1]


def test_legacy_baseline_columns_are_exactly_scoped():
    changeset = {"id": "bronze-meter-readings-001", "labels": "bronze,baseline"}

    assert naming_guard.is_approved_legacy_baseline_column(changeset, "bronze_meter_readings", "LCLid", "STRING")
    assert not naming_guard.is_approved_legacy_baseline_column(changeset, "bronze_weather", "LCLid", "STRING")
    assert not naming_guard.is_approved_legacy_baseline_column(changeset, "bronze_meter_readings", "LCLid", "BOOLEAN")
    assert not naming_guard.is_approved_legacy_baseline_column(
        changeset, "bronze_meter_readings", "new-legacy-name", "STRING"
    )
    assert not naming_guard.is_approved_legacy_baseline_column(
        {"id": "bronze-meter-readings-002", "labels": "bronze,baseline"},
        "bronze_meter_readings",
        "LCLid",
        "STRING",
    )
