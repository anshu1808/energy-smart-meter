import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, rand, when

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.bronze.bronze_meter_events"

# ==========================================
# 1. READ DISTINCT METER KEYS FROM BRONZE
# ==========================================

# Extract LCLid values to map synthetic event telemetry directly to existing meters
raw_meters = (
    spark.table(f"{catalog}.bronze.bronze_meter_readings")
    .filter(col("LCLid").isNotNull())
    .select(col("LCLid"), col("DateTime"))
    .distinct()
)

# ==========================================
# 2. GENERATE SYNTHETIC TAMPER & STATUS EVENTS
# ==========================================

raw_events = (
    raw_meters
    # .withColumn("event_multiplier", expr("explode(array(1, 2, 3))"))
    .withColumn("event_probability", rand(seed=42))
    .filter(col("event_probability") < 0.05)  # Generate low-frequency events (~5% chance per meter)
    .withColumn(
        "event_type",
        expr("""
        element_at(
            array('MAGNETIC_INTERFERENCE', 'COVER_REMOVAL', 'REVERSE_ENERGY_FLOW', 'SEAL_BROKEN', 'POWER_OUTAGE'),
            cast(floor(rand() * 5) + 1 as int)
        )
    """),
    )
    .withColumn(
        "severity",
        when(col("event_type").isin("MAGNETIC_INTERFERENCE", "COVER_REMOVAL"), "CRITICAL")
        .when(col("event_type") == "REVERSE_ENERGY_FLOW", "HIGH")
        .otherwise("MEDIUM"),
    )
    .withColumn(
        "event_timestamp", expr("current_timestamp() - make_interval(0, 0, 0, cast(floor(rand() * 30) as int))")
    )
    .withColumn("event_id", expr("concat('EVT_', LCLid, '_', cast(floor(rand() * 1000000) as int))"))
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("AMI_HEADEND_SIMULATOR"))
    .select("event_id", "LCLid", "event_type", "severity", "event_timestamp", "load_ts", "source_system")
)

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

raw_events = add_schema_drift_metadata(spark, raw_events, TARGET_TABLE)

# ==========================================
# 4. DATA QUALITY CHECKS
# ==========================================

raw_events = raw_events.dropDuplicates(["event_id"])

raw_events = validate_data_quality(
    spark,
    raw_events,
    TARGET_TABLE,
    [
        {"name": "event_id_not_null", "check_type": "not_null", "column": "event_id"},
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "event_timestamp_not_null", "check_type": "not_null", "column": "event_timestamp"},
        {"name": "event_id_unique", "check_type": "unique", "column": "event_id"},
    ],
)

# ==========================================
# 5. INITIAL LOAD & APPEND WRITE
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
    (raw_events.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE))
    print(f"✅ Initial {TARGET_TABLE} load complete.")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("t")
        .merge(
            raw_events.alias("s"),
            "t.LCLid = s.LCLid AND t.event_timestamp = s.event_timestamp",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Successfully appended raw events into {TARGET_TABLE}.")

print(f"Rows processed: {raw_events.count()}")
