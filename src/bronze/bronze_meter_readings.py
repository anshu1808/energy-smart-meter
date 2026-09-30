import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col
)
from delta.tables import DeltaTable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.bronze.bronze_meter_readings"

# ==========================================
# READ RAW SOURCE
# ==========================================

meter_df = (
    spark.read
    .option("header", True)
    .csv(
        "/Volumes/energy/bronze/raw/meter_readings/*.csv"
    )
)

meter_df = meter_df.withColumnRenamed("KWH/hh (per half hour) ", "kwh_hh")

# ==========================================
# AUDIT COLUMNS
# ==========================================

meter_df = (
    meter_df
    .withColumn(
        "load_ts",
        current_timestamp()
    )
    .withColumn(
        "source_system",
        lit("LONDON_SMART_METER")
    )
)

# ==========================================
# SCHEMA DRIFT
# ==========================================

meter_df = add_schema_drift_metadata(
    spark,
    meter_df,
    TARGET_TABLE
)

# ==========================================
# DATA QUALITY
# ==========================================

meter_df = meter_df.dropDuplicates(["LCLid", "DateTime"])

meter_df = validate_data_quality(
    spark,
    meter_df,
    TARGET_TABLE,
    [
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "datetime_not_null", "check_type": "not_null", "column": "DateTime"},
        {"name": "kwh_hh_not_null", "check_type": "not_null", "column": "kwh_hh"},
        {"name": "reading_unique", "check_type": "unique", "columns": ["LCLid", "DateTime"]},
    ],
)

# ==========================================
# INITIAL LOAD
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        meter_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial bronze_meter_readings load complete")

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
            meter_df.alias("s"),
            """
            t.LCLid = s.LCLid
            AND t.DateTime = s.DateTime
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("bronze_meter_readings merged successfully")

print(
    f"Rows processed: {meter_df.count()}"
)