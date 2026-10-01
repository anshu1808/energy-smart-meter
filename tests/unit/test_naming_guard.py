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
