import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    concat,
    lpad,
    current_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.silver.dim_meter"

# =====================================================
# SOURCE
# =====================================================

meter_df = spark.table(
    f"{catalog}.bronze.bronze_meter_readings"
)

# =====================================================
# DQ
# =====================================================

meter_df = (
    meter_df
    .filter(col("LCLid").isNotNull())
)

# =====================================================
# DIM METER
# =====================================================

window_spec = Window.orderBy("LCLid")

dim_meter = (
    meter_df
    .select("LCLid", "stdorToU","DateTime","kwh_hh")
    .dropDuplicates(["LCLid"])
    .withColumn(
        "meter_key",
        row_number().over(window_spec)
    )
    .withColumn(
        "meter_id",
        concat(
            lit("MTR"),
            lpad(
                col("meter_key"),
                6,
                "0"
            )
        )
    )
    .withColumn(
        "meter_type",
        lit("SMART")
    )
    .withColumn(
        "manufacturer",
        lit("LANDIS_GYR")
    )
    .withColumn(
        "status",
        lit("ACTIVE")
    )
    .withColumn(
        "install_date",
        lit("2012-01-01")
    )
    .withColumn(
        "region",
        lit("LONDON")
    )
    .withColumn(
        "load_ts",
        current_timestamp()
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

dim_meter = dim_meter.select(
    "meter_key",
    "meter_id",
    "LCLid",
    "stdorToU",
    "DateTime",
    "kwh_hh",
    "meter_type",
    "manufacturer",
    "status",
    "install_date",
    "region",
    "load_ts"
)

dim_meter = validate_data_quality(
    spark,
    dim_meter,
    TARGET_TABLE,
    [
        {"name": "meter_key_not_null", "check_type": "not_null", "column": "meter_key"},
        {"name": "meter_id_not_null", "check_type": "not_null", "column": "meter_id"},
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "meter_key_unique", "check_type": "unique", "column": "meter_key"},
        {"name": "lclid_unique", "check_type": "unique", "column": "LCLid"},
    ],
)

dim_meter = add_schema_drift_metadata(
    spark, dim_meter, TARGET_TABLE
)

dim_meter = dim_meter.dropDuplicates(["LCLid"])

# ==========================================
# INITIAL LOAD
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        dim_meter.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial dim_meter load complete")

# ==========================================
# INCREMENTAL MERGE
# ==========================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            dim_meter.alias("s"),
            "t.LCLid = s.LCLid"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("dim_meter merged successfully")

print(
    f"dim_meter rows: {dim_meter.count()}"
)