from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    trim,
    upper,
    current_timestamp,
    date_format,
    round
)
from delta.tables import DeltaTable

from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "energy.bronze.bronze_feeder_readings"
TARGET_TABLE = "energy.silver.fact_feeder_readings"

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

silver_feeder = (
    bronze_df
    .select(
        upper(trim(col("feeder_id"))).alias("feeder_id"),
        upper(trim(col("substation_id"))).alias("substation_id"),
        to_timestamp(col("DateTime")).alias("reading_timestamp"),
        col("voltage_v").cast("double"),
        col("current_a").cast("double"),
        col("active_power_kw").cast("double"),
        trim(col("source_system")).alias("source_system")
    )
    .filter(
        col("feeder_id").isNotNull() & 
        col("substation_id").isNotNull() & 
        col("reading_timestamp").isNotNull()
    )
    # Derive app-level calculations and metrics
    .withColumn("voltage_v", round(col("voltage_v"), 2))
    .withColumn("current_a", round(col("current_a"), 2))
    .withColumn("active_power_kw", round(col("active_power_kw"), 3))
    # Derive calendar attributes for partitioning and downstream joins
    .withColumn("reading_date", col("reading_timestamp").cast("date"))
    .withColumn("reading_hour", date_format(col("reading_timestamp"), "HH").cast("int"))
    .withColumn("processed_ts", current_timestamp())
)

# Deduplicate based on primary composite key
silver_feeder = silver_feeder.dropDuplicates(["feeder_id", "reading_timestamp"])

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

silver_feeder = add_schema_drift_metadata(
    spark,
    silver_feeder,
    TARGET_TABLE
)

# ==========================================
# 4. DATA QUALITY CHECKS
# ==========================================

silver_feeder = validate_data_quality(
    spark,
    silver_feeder,
    TARGET_TABLE,
    [
        {"name": "feeder_id_not_null", "check_type": "not_null", "column": "feeder_id"},
        {"name": "substation_id_not_null", "check_type": "not_null", "column": "substation_id"},
        {"name": "reading_timestamp_not_null", "check_type": "not_null", "column": "reading_timestamp"},
        {"name": "active_power_not_null", "check_type": "not_null", "column": "active_power_kw"},
        {"name": "feeder_reading_unique", "check_type": "unique", "columns": ["feeder_id", "reading_timestamp"]},
    ],
)

# Select final Silver attributes
silver_feeder = silver_feeder.select(
    "feeder_id",
    "substation_id",
    "reading_timestamp",
    "reading_date",
    "reading_hour",
    "voltage_v",
    "current_a",
    "active_power_kw",
    "source_system",
    "processed_ts",
    "dq_passed",
    "dq_failed_checks",
    "schema_drift_detected",
    "schema_drift_columns"
)

# ==========================================
# 5. INITIAL LOAD & INCREMENTAL MERGE
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
    (
        silver_feeder.write
        .format("delta")
        .partitionBy("reading_date")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"✅ Initial load complete for {TARGET_TABLE}")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
    
    (
        target.alias("t")
        .merge(
            silver_feeder.alias("s"),
            """
            t.feeder_id = s.feeder_id 
            AND t.reading_timestamp = s.reading_timestamp 
            AND t.reading_date = s.reading_date
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Merged records successfully into {TARGET_TABLE}")

print(f"Rows processed into Silver: {silver_feeder.count()}")