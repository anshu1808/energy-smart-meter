import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, to_timestamp, trim, upper, when

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

BRONZE_TABLE = f"{catalog}.bronze.bronze_meter_events"
TARGET_TABLE = f"{catalog}.silver.fact_meter_events"

# ==========================================
# 1. READ BRONZE & FILTER DQ ERRORS
# ==========================================

bronze_df = spark.table(BRONZE_TABLE)

# Filter out failed bronze DQ records if metadata column exists
if "dq_passed" in bronze_df.columns:
    bronze_df = bronze_df.filter(col("dq_passed") == True)

# ==========================================
# 2. TRANSFORMATIONS & TYPE CASTING
# ==========================================

silver_events = (
    bronze_df.select(
        trim(col("event_id")).alias("event_id"),
        trim(col("LCLid")).alias("LCLid"),
        upper(trim(col("event_type"))).alias("event_type"),
        upper(trim(col("severity"))).alias("severity"),
        to_timestamp(col("event_timestamp")).alias("event_timestamp"),
        trim(col("source_system")).alias("source_system"),
    )
    .filter(col("event_id").isNotNull() & col("LCLid").isNotNull() & col("event_timestamp").isNotNull())
    # Standardize/validate severity categories
    .withColumn(
        "severity",
        when(col("severity").isin("CRITICAL", "HIGH", "MEDIUM", "LOW"), col("severity")).otherwise("UNKNOWN"),
    )
    # Derive calendar attributes for analytical querying
    .withColumn("event_date", col("event_timestamp").cast("date"))
    .withColumn("event_hour", date_format(col("event_timestamp"), "HH").cast("int"))
    .withColumn("processed_ts", current_timestamp())
)

# Deduplicate by primary key keeping the latest load
silver_events = silver_events.dropDuplicates(["event_id"])

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

silver_events = add_schema_drift_metadata(spark, silver_events, TARGET_TABLE)

# ==========================================
# 4. DATA QUALITY CHECKS
# ==========================================

silver_events = validate_data_quality(
    spark,
    silver_events,
    TARGET_TABLE,
    [
        {"name": "event_id_not_null", "check_type": "not_null", "column": "event_id"},
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "event_timestamp_not_null", "check_type": "not_null", "column": "event_timestamp"},
        {"name": "event_id_unique", "check_type": "unique", "column": "event_id"},
    ],
)

# Select final Silver attributes
silver_events = silver_events.select(
    "event_id",
    "LCLid",
    "event_type",
    "severity",
    "event_timestamp",
    "event_date",
    "event_hour",
    "source_system",
    "processed_ts",
    "dq_passed",
    "dq_failed_checks",
    "schema_drift_detected",
    "schema_drift_columns",
)

# ==========================================
# 5. INITIAL LOAD & INCREMENTAL MERGE
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
    (
        silver_events.write.format("delta")
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"✅ Initial load complete for {TARGET_TABLE}")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("t")
        .merge(silver_events.alias("s"), "t.event_id = s.event_id AND t.event_date = s.event_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Merged records successfully into {TARGET_TABLE}")

print(f"Rows processed into Silver: {silver_events.count()}")
