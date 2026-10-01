import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, rand, round

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.bronze.bronze_feeder_readings"

# ==========================================
# 1. READ DISTINCT DATETIME SLOTS FROM BRONZE
# ==========================================

# Align feeder timestamps directly with actual half-hourly meter reading windows
reading_timestamps = (
    spark.table(f"{catalog}.bronze.bronze_meter_readings")
    .filter(col("DateTime").isNotNull())
    .select(col("DateTime").alias("reading_datetime"))
    .distinct()
)

# Define regional London feeder network substations
feeders = spark.createDataFrame(
    [
        ("FDR_LDN_NORTH_01", "SUB_NORTH_HIGHBURY"),
        ("FDR_LDN_SOUTH_01", "SUB_SOUTH_BRIXTON"),
        ("FDR_LDN_EAST_01", "SUB_EAST_STRATFORD"),
        ("FDR_LDN_WEST_01", "SUB_WEST_ACTON"),
    ],
    ["feeder_id", "substation_id"],
)

# ==========================================
# 2. GENERATE FEEDER TELEMETRY DATA
# ==========================================

feeder_readings = (
    reading_timestamps.crossJoin(feeders)
    .withColumn("voltage_v", round(lit(230.0) + (rand(seed=123) * 10 - 5), 2))
    .withColumn("current_a", round(lit(150.0) + (rand(seed=456) * 50), 2))
    .withColumn("active_power_kw", round((col("voltage_v") * col("current_a") * lit(0.95)) / lit(1000.0), 3))
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("SCADA_GRID_MONITOR"))
    .select(
        "feeder_id",
        "substation_id",
        col("reading_datetime").alias("DateTime"),
        "voltage_v",
        "current_a",
        "active_power_kw",
        "load_ts",
        "source_system",
    )
)

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

feeder_readings = add_schema_drift_metadata(spark, feeder_readings, TARGET_TABLE)

# ==========================================
# 4. DATA QUALITY CHECKS
# ==========================================

feeder_readings = feeder_readings.dropDuplicates(["feeder_id", "DateTime"])

feeder_readings = validate_data_quality(
    spark,
    feeder_readings,
    TARGET_TABLE,
    [
        {"name": "feeder_id_not_null", "check_type": "not_null", "column": "feeder_id"},
        {"name": "datetime_not_null", "check_type": "not_null", "column": "DateTime"},
        {"name": "active_power_not_null", "check_type": "not_null", "column": "active_power_kw"},
        {"name": "feeder_reading_unique", "check_type": "unique", "columns": ["feeder_id", "DateTime"]},
    ],
)

# ==========================================
# 5. WRITE TO BRONZE
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
    (feeder_readings.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE))
    print(f"Initial load complete for {TARGET_TABLE}")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("t")
        .merge(
            feeder_readings.alias("s"),
            "t.feeder_id = s.feeder_id AND t.DateTime = s.DateTime",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged records into {TARGET_TABLE}")

print(f"Rows processed: {feeder_readings.count()}")
