# Complete Python Source Compendium

This document contains the complete Python source currently present under src/. It is generated from the repository files; edit the source files rather than this compendium.

## Files

- [src/audit/pipeline_lineage.py](#src-audit-pipeline_lineagepy)
- [src/bronze/bronze_eia.py](#src-bronze-bronze_eiapy)
- [src/bronze/bronze_meter_readings.py](#src-bronze-bronze_meter_readingspy)
- [src/bronze/bronze_tariff.py](#src-bronze-bronze_tariffpy)
- [src/bronze/bronze_weather.py](#src-bronze-bronze_weatherpy)
- [src/bronze/data_quality.py](#src-bronze-data_qualitypy)
- [src/bronze/ingest_feeder_readings.py](#src-bronze-ingest_feeder_readingspy)
- [src/bronze/ingest_meter_events.py](#src-bronze-ingest_meter_eventspy)
- [src/bronze/schema_drift.py](#src-bronze-schema_driftpy)
- [src/config/config.py](#src-config-configpy)
- [src/gold/gold_peak_load.py](#src-gold-gold_peak_loadpy)
- [src/gold/gold_revenue_summary.py](#src-gold-gold_revenue_summarypy)
- [src/gold/gold_theft_detection.py](#src-gold-gold_theft_detectionpy)
- [src/gold/gold_weather_impact.py](#src-gold-gold_weather_impactpy)
- [src/ingestion/eia_ingestion.py](#src-ingestion-eia_ingestionpy)
- [src/ingestion/weather_ingestion.py](#src-ingestion-weather_ingestionpy)
- [src/silver/data_quality.py](#src-silver-data_qualitypy)
- [src/silver/dim_customer.py](#src-silver-dim_customerpy)
- [src/silver/dim_date.py](#src-silver-dim_datepy)
- [src/silver/dim_meter.py](#src-silver-dim_meterpy)
- [src/silver/dim_tariff.py](#src-silver-dim_tariffpy)
- [src/silver/fact_billing.py](#src-silver-fact_billingpy)
- [src/silver/fact_consumption.py](#src-silver-fact_consumptionpy)
- [src/silver/fact_eia_demand.py](#src-silver-fact_eia_demandpy)
- [src/silver/fact_feeder_readings.py](#src-silver-fact_feeder_readingspy)
- [src/silver/fact_meter_events.py](#src-silver-fact_meter_eventspy)
- [src/silver/fact_weather.py](#src-silver-fact_weatherpy)
- [src/silver/schema_drift.py](#src-silver-schema_driftpy)

## src/audit/pipeline_lineage.py

```python
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

AUDIT_TABLE = "energy.audit.pipeline_lineage"
PIPELINE_SCHEMAS = ("bronze", "silver", "gold", "audit")

def get_databricks_context():
    dbutils = globals().get("dbutils")
    if dbutils is None:
        return None

    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return None


def get_context_tag(context, tag_name):
    if context is None:
        return None

    try:
        value = context.tags().apply(tag_name)
        return value.get() if value.isDefined() else None
    except Exception:
        return None


def get_run_id(context):
    try:
        return str(context.currentRunId().get().id())
    except Exception:
        return get_context_tag(context, "jobRunId") or "manual"

def get_pipeline_start_ts(context):
    context_start = get_context_tag(context, "startTime")
    if context_start:
        try:
            numeric_value = float(context_start)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            pass

    for config_key in (
        "spark.databricks.job.startTime",
        "spark.databricks.job.startTimeMs",
        "spark.databricks.job.runStartTime",
    ):
        try:
            value = spark.conf.get(config_key)
            numeric_value = float(value)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (Exception, TypeError, ValueError, OverflowError):
            continue

    return datetime.now(timezone.utc).replace(tzinfo=None)


context = get_databricks_context()
run_id = get_run_id(context)
audit_task_start_ts = datetime.now(timezone.utc).replace(tzinfo=None)
pipeline_start_ts = get_pipeline_start_ts(context)

spark.sql("CREATE SCHEMA IF NOT EXISTS energy.audit")

schema_filter = ", ".join(f"'{schema_name}'" for schema_name in PIPELINE_SCHEMAS)
tables_df = spark.sql(
    f"""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type
    FROM energy.information_schema.tables
    WHERE table_schema IN ({schema_filter})
    """
)

lineage_df = (
    tables_df
    .withColumn("run_id", lit(run_id))
    .withColumn("run_ts", lit(pipeline_start_ts).cast("timestamp"))
    .withColumn("pipeline_start_ts", lit(pipeline_start_ts).cast("timestamp"))
    .withColumn("pipeline_end_ts", current_timestamp())
    .withColumn("audit_task_start_ts", lit(audit_task_start_ts).cast("timestamp"))
    .withColumn("audit_ts", current_timestamp())
    .select(
        "run_id",
        "run_ts",
        "pipeline_start_ts",
        "pipeline_end_ts",
        "audit_task_start_ts",
        "audit_ts",
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
    )
)

(
    lineage_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(AUDIT_TABLE)
)

print(
    f"Pipeline lineage recorded: run_id={run_id}, "
    f"tables={lineage_df.count()}, "
    f"pipeline_start_ts={pipeline_start_ts.isoformat()}Z"
)
```

## src/bronze/bronze_eia.py

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality
spark = SparkSession.builder.getOrCreate() 

eia_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet("/Volumes/energy/bronze/raw/eia")
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("EIA_API"))
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_file_name", col("_metadata.file_name"))
    .withColumn("source_file_size", col("_metadata.file_size"))
    .withColumn(
        "source_file_modification_time",
        col("_metadata.file_modification_time")
    )
)
eia_df = add_schema_drift_metadata(
    spark, eia_df, "energy.bronze.bronze_eia"
)

eia_df = eia_df.dropDuplicates(["period", "respondent", "type"])

eia_df = validate_data_quality(
    spark,
    eia_df,
    "energy.bronze.bronze_eia",
    [
        {"name": "period_not_null", "check_type": "not_null", "column": "period"},
        {"name": "respondent_not_null", "check_type": "not_null", "column": "respondent"},
        {"name": "type_not_null", "check_type": "not_null", "column": "type"},
        {"name": "value_not_null", "check_type": "not_null", "column": "value"},
    ],
)

eia_df.show()

target_table = "energy.bronze.bronze_eia"

if spark.catalog.tableExists(target_table):

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target.alias("t")
        .merge(
            eia_df.alias("s"),
            """
            t.period = s.period
            AND t.respondent = s.respondent
            AND t.type = s.type
            AND t.source_system = s.source_system
            AND t.`type-name` = s.`type-name`
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        eia_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
```

## src/bronze/bronze_meter_readings.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col
)
from delta.tables import DeltaTable

from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.bronze.bronze_meter_readings"

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
```

## src/bronze/bronze_tariff.py

```python
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality
spark = SparkSession.builder.getOrCreate()

tariff_pd = pd.read_excel("/Volumes/energy/bronze/raw/tariff/Tariffs.xlsx")
tariff_df = (
    spark.createDataFrame(tariff_pd)
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("TARIFF_FILE"))
    .withColumn(
        "source_file",
        lit("/Volumes/energy/bronze/raw/tariff/Tariffs.xlsx")
    )
    .withColumn("source_file_name", lit("Tariffs.xlsx"))
    .withColumn("source_file_size", lit(None).cast("long"))
    .withColumn(
        "source_file_modification_time",
        lit(None).cast("timestamp"),
    )
)
tariff_df = add_schema_drift_metadata(
    spark, tariff_df, "energy.bronze.bronze_tariff"
)

tariff_df = validate_data_quality(
    spark,
    tariff_df,
    "energy.bronze.bronze_tariff",
    [
        {"name": "tariff_datetime_not_null", "check_type": "not_null", "column": "TariffDateTime"},
        {"name": "tariff_not_null", "check_type": "not_null", "column": "Tariff"},
        {"name": "tariff_unique", "check_type": "unique", "column": "TariffDateTime", "columns": ["TariffDateTime", "Tariff"]},
    ],
)

tariff_df.show()

target_table = "energy.bronze.bronze_tariff"

if spark.catalog.tableExists(target_table):

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target.alias("t")
        .merge(
            tariff_df.alias("s"),
            """
            t.TariffDateTime = s.TariffDateTime
            and t.Tariff = s.Tariff
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        tariff_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

print("bronze_tariff loaded")
```

## src/bronze/bronze_weather.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality
from delta.tables import DeltaTable


spark = SparkSession.builder.getOrCreate()

df = spark.read.option("recursiveFileLookup", "true").option(
    "mergeSchema", "true"
    ).parquet(
        "/Volumes/energy/bronze/raw/weather/"
)

bronze_df = (
    df.withColumn("load_timestamp", current_timestamp())
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("WEATHER_API"))
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_file_name", col("_metadata.file_name"))
    .withColumn("source_file_size", col("_metadata.file_size"))
    .withColumn(
        "source_file_modification_time",
        col("_metadata.file_modification_time")
    )
)
bronze_df = add_schema_drift_metadata(
    spark, bronze_df, "energy.bronze.bronze_weather"
)

bronze_df = bronze_df.dropDuplicates(["timestamp"])

bronze_df = validate_data_quality(
    spark,
    bronze_df,
    "energy.bronze.bronze_weather",
    [
        {"name": "timestamp_not_null", "check_type": "not_null", "column": "timestamp"},
        {"name": "humidity_range", "check_type": "range", "column": "humidity", "min_val": 0, "max_val": 100},
        {"name": "temperature_range", "check_type": "range", "column": "temperature", "min_val": -50, "max_val": 60},
        {"name": "wind_speed_non_negative", "check_type": "range", "column": "wind_speed", "min_val": 0},
        {"name": "precipitation_non_negative", "check_type": "range", "column": "precipitation", "min_val": 0},
    ],
)

bronze_df.show()

target_table = "energy.bronze.bronze_weather"

if spark.catalog.tableExists(target_table):

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target.alias("t")
        .merge(
            bronze_df.alias("s"),
            """
            t.timestamp = s.timestamp
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        bronze_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

print("bronze_weather loaded")
```

## src/bronze/data_quality.py

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

DQ_FAILURE_TABLE = "energy.silver.dq_failures"


def _log_dq_failures(spark, table_name, failure_details):
    """Write DQ failure records to the DQ failure tracking table."""
    schema = StructType([
        StructField("dq_table", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("failure_reason", StringType(), False),
        StructField("failed_value", LongType(), False),
    ])

    records = [
        (
            table_name,
            d["check_name"],
            d["check_type"],
            d["column_name"],
            d["failure_reason"],
            d["failed_value"],
        )
        for d in failure_details
    ]

    failure_df = spark.createDataFrame(records, schema)
    failure_df = failure_df.withColumn("check_ts", current_timestamp())

    if not spark.catalog.tableExists(DQ_FAILURE_TABLE):
        (
            failure_df.write
            .format("delta")
            .option("mergeSchema", "true")
            .saveAsTable(DQ_FAILURE_TABLE)
        )
    else:
        (
            failure_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(DQ_FAILURE_TABLE)
        )


def validate_data_quality(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    checks: list,
    fail_on_error: bool = False,
) -> DataFrame:
    """Run data quality checks on a DataFrame and add metadata columns.

    When any check fails, the failure details (table name, check name, check
    type, column name, failure reason, failed value count, and timestamp)
    are written to the energy.silver.dq_failures table.

    Args:
        spark: SparkSession
        dataframe: DataFrame to validate
        table_name: Name of the target table (for logging)
        checks: List of dicts with keys:
            - name: str (check name)
            - check_type: str ("not_null", "unique", "range", "not_empty")
            - column: str (column to check; for "unique" can be a single column)
            - columns: list (for multi-column "unique" check)
            - min_val: numeric (optional, for "range" check)
            - max_val: numeric (optional, for "range" check)
        fail_on_error: If True, raise RuntimeError when any check fails.

    Returns:
        DataFrame with dq_passed and dq_failed_checks columns added.
    """
    total_rows = dataframe.count()
    failed_checks = []
    failure_details = []

    for check in checks:
        check_name = check["name"]
        check_type = check["check_type"]

        if check_type == "not_null":
            column = check["column"]
            null_count = dataframe.filter(col(column).isNull()).count()
            if null_count > 0:
                reason = f"{null_count} nulls in {column}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": column,
                    "failure_reason": reason,
                    "failed_value": null_count,
                })

        elif check_type == "unique":
            columns = check.get("columns") or [check["column"]]
            dup_count = total_rows - dataframe.dropDuplicates(columns).count()
            if dup_count > 0:
                reason = f"{dup_count} duplicates on {columns}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": ", ".join(str(c) for c in columns),
                    "failure_reason": reason,
                    "failed_value": dup_count,
                })

        elif check_type == "range":
            column = check["column"]
            min_val = check.get("min_val")
            max_val = check.get("max_val")
            conditions = []
            if min_val is not None:
                conditions.append(col(column) < min_val)
            if max_val is not None:
                conditions.append(col(column) > max_val)
            if conditions:
                range_filter = conditions[0]
                for c in conditions[1:]:
                    range_filter = range_filter | c
                range_filter = range_filter & col(column).isNotNull()
                out_of_range = dataframe.filter(range_filter).count()
                if out_of_range > 0:
                    reason = f"{out_of_range} out-of-range values in {column}"
                    failed_checks.append(f"{check_name}: {reason}")
                    failure_details.append({
                        "check_name": check_name,
                        "check_type": check_type,
                        "column_name": column,
                        "failure_reason": reason,
                        "failed_value": out_of_range,
                    })

        elif check_type == "not_empty":
            if total_rows == 0:
                reason = "0 rows in dataframe"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": "",
                    "failure_reason": reason,
                    "failed_value": 0,
                })

    if failure_details:
        _log_dq_failures(spark, table_name, failure_details)

    dq_summary = "; ".join(failed_checks)
    status = "PASSED" if not failed_checks else "FAILED"
    print(f"[DQ] {table_name}: {status}")
    if failed_checks:
        print(f"[DQ] {table_name}: {dq_summary}")

    if fail_on_error and failed_checks:
        raise RuntimeError(
            f"Data quality checks failed for {table_name}: {dq_summary}"
        )

    return (
        dataframe
        .withColumn("dq_passed", lit(len(failed_checks) == 0))
        .withColumn("dq_failed_checks", lit(dq_summary))
    )
```

## src/bronze/ingest_feeder_readings.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, rand, when, lit, current_timestamp, round
)
from delta.tables import DeltaTable

from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.bronze.bronze_feeder_readings"

# ==========================================
# 1. READ DISTINCT DATETIME SLOTS FROM BRONZE
# ==========================================

# Align feeder timestamps directly with actual half-hourly meter reading windows
reading_timestamps = (
    spark.table("energy.bronze.bronze_meter_readings")
    .filter(col("DateTime").isNotNull())
    .select(col("DateTime").alias("reading_datetime"))
    .distinct()
)

# Define regional London feeder network substations
feeders = spark.createDataFrame([
    ("FDR_LDN_NORTH_01", "SUB_NORTH_HIGHBURY"),
    ("FDR_LDN_SOUTH_01", "SUB_SOUTH_BRIXTON"),
    ("FDR_LDN_EAST_01", "SUB_EAST_STRATFORD"),
    ("FDR_LDN_WEST_01", "SUB_WEST_ACTON")
], ["feeder_id", "substation_id"])

# ==========================================
# 2. GENERATE FEEDER TELEMETRY DATA
# ==========================================

feeder_readings = (
    reading_timestamps
    .crossJoin(feeders)
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
        "source_system"
    )
)

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

feeder_readings = add_schema_drift_metadata(
    spark,
    feeder_readings,
    TARGET_TABLE
)

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
    (
        feeder_readings.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"âœ… Initial load complete for {TARGET_TABLE}")
else:
    (
        feeder_readings.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"âœ… Appended records into {TARGET_TABLE}")

print(f"Rows processed: {feeder_readings.count()}")
```

## src/bronze/ingest_meter_events.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, rand, when, lit, current_timestamp
)
from delta.tables import DeltaTable

from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.bronze.bronze_meter_events"

# ==========================================
# 1. READ DISTINCT METER KEYS FROM BRONZE
# ==========================================

# Extract LCLid values to map synthetic event telemetry directly to existing meters
raw_meters = (
    spark.table("energy.bronze.bronze_meter_readings")
    .filter(col("LCLid").isNotNull())
    .select(col("LCLid"), col("DateTime"))
    .distinct()
)

# ==========================================
# 2. GENERATE SYNTHETIC TAMPER & STATUS EVENTS
# ==========================================

raw_events = (
    raw_meters
    .withColumn("event_multiplier", expr("explode(array(1, 2, 3))"))
    .withColumn("event_probability", rand(seed=42))
    .filter(col("event_probability") < 0.05)  # Generate low-frequency events (~5% chance per meter)
    .withColumn("event_type", expr("""
        element_at(
            array('MAGNETIC_INTERFERENCE', 'COVER_REMOVAL', 'REVERSE_ENERGY_FLOW', 'SEAL_BROKEN', 'POWER_OUTAGE'),
            cast(floor(rand() * 5) + 1 as int)
        )
    """))
    .withColumn("severity", when(col("event_type").isin("MAGNETIC_INTERFERENCE", "COVER_REMOVAL"), "CRITICAL")
                            .when(col("event_type") == "REVERSE_ENERGY_FLOW", "HIGH")
                            .otherwise("MEDIUM"))
    .withColumn("event_timestamp", expr("current_timestamp() - make_interval(0, 0, 0, cast(floor(rand() * 30) as int))"))
    .withColumn("event_id", expr("concat('EVT_', LCLid, '_', cast(floor(rand() * 1000000) as int))"))
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("AMI_HEADEND_SIMULATOR"))
    .select(
        "event_id",
        "LCLid",
        "event_type",
        "severity",
        "event_timestamp",
        "load_ts",
        "source_system"
    )
)

# ==========================================
# 3. SCHEMA DRIFT METADATA
# ==========================================

raw_events = add_schema_drift_metadata(
    spark,
    raw_events,
    TARGET_TABLE
)

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
    (
        raw_events.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"âœ… Initial {TARGET_TABLE} load complete.")
else:
    (
        raw_events.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"âœ… Successfully appended raw events into {TARGET_TABLE}.")

print(f"Rows processed: {raw_events.count()}")
```

## src/bronze/schema_drift.py

```python
from pyspark.sql.functions import lit


def add_schema_drift_metadata(spark, dataframe, target_table):
    """Annotate a bronze DataFrame with newly added or changed source columns."""
    if spark.catalog.tableExists(target_table):
        target_types = {
            field.name: field.dataType.simpleString()
            for field in spark.table(target_table).schema
        }
    else:
        target_types = {}

    drift_columns = []
    for field in dataframe.schema:
        source_type = field.dataType.simpleString()
        target_type = target_types.get(field.name)
        if target_type is None:
            if target_types:
                drift_columns.append(f"{field.name} (new column)")
        elif target_type != source_type:
            drift_columns.append(
                f"{field.name} (type changed: {target_type} -> {source_type})"
            )

    drift_summary = ", ".join(drift_columns)
    return (
        dataframe
        .withColumn("schema_drift_detected", lit(bool(drift_columns)))
        .withColumn("schema_drift_columns", lit(drift_summary))
    )
```

## src/config/config.py

```python
"""Application configuration.

This project must not store live secrets in source control.
Use Databricks Secrets or environment variables instead.
"""

import os


def get_eia_api_key() -> str:
    candidates = (
        "EIA_API_KEY",
        "DATABRICKS_SECRET_EIA_API_KEY",
    )

    for key in candidates:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()

    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            return dbutils.secrets.get(
                scope="energy-secrets",
                key="eia-api-key"
            )
        except Exception:
            pass

    raise RuntimeError(
        "EIA API key not configured."
    )

#EIA_API_KEY = os.getenv("EIA_API_KEY", "")
```

## src/gold/gold_peak_load.py

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, current_timestamp, date_format
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.gold.gold_peak_load"
fact_consumption = spark.table("energy.silver.fact_consumption")

gold_df = (
	fact_consumption
	.withColumn("reading_date", date_format("reading_timestamp", "yyyy-MM-dd"))
	.withColumn("hour_of_day", date_format("reading_timestamp", "HH").cast("int"))
	.groupBy("date_key", "reading_date", "hour_of_day")
	.agg(
		count("consumption_business_key").alias("reading_count"),
		spark_sum("consumption_kwh").alias("total_consumption_kwh"),
		avg("consumption_kwh").alias("avg_consumption_kwh"),
		spark_max("consumption_kwh").alias("max_meter_consumption_kwh"),
	)
	.withColumn("load_ts", current_timestamp())
)

if not spark.catalog.tableExists(TARGET_TABLE):
	gold_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
else:
	(
		DeltaTable.forName(spark, TARGET_TABLE)
		.alias("t")
		.merge(
			gold_df.alias("s"),
			"t.date_key = s.date_key AND t.hour_of_day = s.hour_of_day",
		)
		.whenMatchedUpdateAll()
		.whenNotMatchedInsertAll()
		.execute()
	)

print(f"gold_peak_load rows: {gold_df.count()}")
```

## src/gold/gold_revenue_summary.py

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, current_timestamp
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.gold.gold_revenue_summary"
fact_billing = spark.table("energy.silver.fact_billing")

gold_df = (
    fact_billing
    .groupBy("billing_period")
    .agg(
        countDistinct("customer_key").alias("customer_count"),
        countDistinct("meter_key").alias("meter_count"),
        spark_sum("consumption_kwh").alias("total_consumption_kwh"),
        spark_sum("bill_amount").alias("total_revenue"),
        avg("consumption_kwh").alias("avg_consumption_kwh"),
        avg("bill_amount").alias("avg_bill_amount"),
    )
    .withColumn("load_ts", current_timestamp())
)

if not spark.catalog.tableExists(TARGET_TABLE):
    gold_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
else:
    (
        DeltaTable.forName(spark, TARGET_TABLE)
        .alias("t")
        .merge(gold_df.alias("s"), "t.billing_period = s.billing_period")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"gold_revenue_summary rows: {gold_df.count()}")
```

## src/gold/gold_theft_detection.py

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, date_format, lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import stddev_pop
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.gold.gold_theft_detection"
consumption = spark.table("energy.silver.fact_consumption")
customers = spark.table("energy.silver.dim_customer")

meter_monthly = (
	consumption
	.join(customers.select("customer_key", "LCLid", "tariff_id"), "customer_key", "left")
	.withColumn("billing_period", date_format("reading_timestamp", "yyyy-MM"))
	.groupBy("billing_period", "customer_key", "LCLid", "tariff_id")
	.agg(spark_sum("consumption_kwh").alias("consumption_kwh"))
)

peer_stats = (
	meter_monthly
	.groupBy("billing_period", "tariff_id")
	.agg(
		avg("consumption_kwh").alias("peer_avg_consumption_kwh"),
		stddev_pop("consumption_kwh").alias("peer_stddev_consumption_kwh"),
	)
)

gold_df = (
	meter_monthly
	.join(peer_stats, ["billing_period", "tariff_id"], "left")
	.withColumn(
		"variance_from_peer_kwh",
		spark_round(col("consumption_kwh") - col("peer_avg_consumption_kwh"), 6),
	)
	.withColumn(
		"theft_flag",
		when(
			(col("consumption_kwh") < col("peer_avg_consumption_kwh") - 2 * col("peer_stddev_consumption_kwh"))
			& (col("consumption_kwh") < col("peer_avg_consumption_kwh") * lit(0.5)),
			lit(True),
		).otherwise(lit(False)),
	)
	.withColumn(
		"detection_reason",
		when(col("theft_flag"), lit("Consumption is materially below tariff peer usage"))
		.otherwise(lit("Within expected tariff peer range")),
	)
	.withColumn("load_ts", current_timestamp())
)

if not spark.catalog.tableExists(TARGET_TABLE):
	gold_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
else:
	(
		DeltaTable.forName(spark, TARGET_TABLE)
		.alias("t")
		.merge(
			gold_df.alias("s"),
			"t.billing_period = s.billing_period AND t.LCLid = s.LCLid",
		)
		.whenMatchedUpdateAll()
		.whenNotMatchedInsertAll()
		.execute()
	)

print(f"gold_theft_detection rows: {gold_df.count()}")
```

## src/gold/gold_weather_impact.py

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, hour
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.gold.gold_weather_impact"
consumption = spark.table("energy.silver.fact_consumption")
weather = spark.table("energy.silver.fact_weather")

consumption_hourly = (
	consumption
	.withColumn("hour_of_day", hour("reading_timestamp"))
	.groupBy("date_key", "hour_of_day")
	.agg(
		spark_sum("consumption_kwh").alias("total_consumption_kwh"),
		avg("consumption_kwh").alias("avg_meter_consumption_kwh"),
	)
)

weather_hourly = (
	weather
	.select(
		"date_key",
		"hour_of_day",
		"temperature_c",
		"humidity_pct",
		"wind_speed_kmh",
		"precipitation_mm",
	)
	.dropDuplicates(["date_key", "hour_of_day"])
)

gold_df = (
	weather_hourly
	.join(consumption_hourly, ["date_key", "hour_of_day"], "left")
	.withColumn("load_ts", current_timestamp())
)

if not spark.catalog.tableExists(TARGET_TABLE):
	gold_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
else:
	(
		DeltaTable.forName(spark, TARGET_TABLE)
		.alias("t")
		.merge(
			gold_df.alias("s"),
			"t.date_key = s.date_key AND t.hour_of_day = s.hour_of_day",
		)
		.whenMatchedUpdateAll()
		.whenNotMatchedInsertAll()
		.execute()
	)

print(f"gold_weather_impact rows: {gold_df.count()}")
```

## src/ingestion/eia_ingestion.py

```python
from pyspark.shell import spark

import os
from datetime import datetime, timezone

import requests

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# =====================================================
# CONFIG
# =====================================================

def get_eia_api_key():
    """Resolve the EIA API key from environment variables or Databricks secrets."""
    for env_var in ("EIA_API_KEY", "DATABRICKS_SECRET_EIA_API_KEY"):
        value = os.getenv(env_var)
        if value and value.strip():
            return value.strip()

    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope="energy-secrets", key="eia-api-key")
        except Exception:
            pass

    raise RuntimeError(
        "EIA API key is not configured. Set EIA_API_KEY or store it in "
        "Databricks secret scope energy-secrets / eia-api-key."
    )


def get_pipeline_start_ts():
    """Return the Databricks job start time when available."""
    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            start_time = context.tags().apply("startTime")
            if start_time.isDefined():
                numeric_value = float(start_time.get())
                if numeric_value > 10_000_000_000:
                    numeric_value /= 1000
                return datetime.fromtimestamp(
                    numeric_value,
                    timezone.utc,
                ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            pass
        except Exception:
            pass

    for config_key in (
        "spark.databricks.job.startTime",
        "spark.databricks.job.startTimeMs",
        "spark.databricks.job.runStartTime",
    ):
        try:
            value = spark.conf.get(config_key)
        except Exception:
            continue

        try:
            numeric_value = float(value)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            continue

    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_last_pipeline_start_ts():
    """Use the last completed pipeline start as the EIA API watermark."""
    audit_table = "energy.audit.pipeline_lineage"
    if not spark.catalog.tableExists(audit_table):
        return None

    audit_columns = {
        field.name
        for field in spark.table(audit_table).schema.fields
    }
    timestamp_column = (
        "pipeline_start_ts"
        if "pipeline_start_ts" in audit_columns
        else "run_ts"
        if "run_ts" in audit_columns
        else None
    )
    if timestamp_column is None:
        return None

    watermark = (
        spark.table(audit_table)
        .selectExpr(f"max(`{timestamp_column}`) AS pipeline_start_ts")
        .first()["pipeline_start_ts"]
    )
    return watermark

# =====================================================
# CONSTANTS
# =====================================================

RAW_EIA_PATH = "/Volumes/energy/bronze/raw/eia"

API_URL = (
    "https://api.eia.gov/v2/electricity/"
    "rto/region-data/data/"
)

# =====================================================
# INGESTION
# =====================================================

def fetch_eia_data():

    api_key = get_eia_api_key()
    pipeline_start_ts = get_pipeline_start_ts()
    last_pipeline_start_ts = get_last_pipeline_start_ts()

    start_query = ""
    if last_pipeline_start_ts is not None:
        start_query = (
            f"&start={last_pipeline_start_ts.strftime('%Y-%m-%dT%H')}"
        )

    url = (
        API_URL
        + "?frequency=hourly"
        + "&data[0]=value"
        + "&sort[0][column]=period"
        + "&sort[0][direction]=asc"
        + "&offset=0"
        + "&length=5000"
        + start_query
        + f"&api_key={api_key}"
    )

    response = requests.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    records = payload["response"]["data"]

    if not records:
        print(
            f"No new EIA records after watermark="
            f"{last_pipeline_start_ts or 'bootstrap'}"
        )
        return

    load_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    eia_df = (
        spark.createDataFrame(records)
        .withColumn(
            "_source_system",
            lit("EIA")
        )
        .withColumn(
            "_load_id",
            lit(load_id)
        )
        .withColumn(
            "_ingestion_ts",
            current_timestamp()
        )
    )

    eia_df.show(5, truncate=False)

    (
        eia_df.write
        .mode("append")
        .parquet(
            f"{RAW_EIA_PATH}/eia_{load_id}"
        )
    )

    print(
        f"EIA records ingested: {eia_df.count()}, "
        f"watermark={last_pipeline_start_ts or 'bootstrap'}, "
        f"pipeline_start_ts={pipeline_start_ts}"
    )

    print(
        f"Load ID: {load_id}"
    )

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    fetch_eia_data()
```

## src/ingestion/weather_ingestion.py

```python
from pyspark.shell import spark
import requests
from datetime import datetime, timezone
from pyspark.sql.functions import col, current_timestamp, lit

LATITUDE = 52.52
LONGITUDE = 13.41

# =====================================================
# CONFIG
# =====================================================


def get_pipeline_start_ts():
    """Return the Databricks job start time when available."""
    for config_key in (
        "spark.databricks.job.startTime",
        "spark.databricks.job.startTimeMs",
        "spark.databricks.job.runStartTime",
    ):
        try:
            value = spark.conf.get(config_key)
        except Exception:
            continue

        try:
            numeric_value = float(value)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            continue

    return datetime.now(timezone.utc).replace(tzinfo=None)

def get_last_pipeline_start_ts():
    """Use the last completed pipeline start as the EIA API watermark."""
    audit_table = "energy.audit.pipeline_lineage"
    if not spark.catalog.tableExists(audit_table):
        return None

    audit_columns = {
        field.name
        for field in spark.table(audit_table).schema.fields
    }
    timestamp_column = (
        "pipeline_start_ts"
        if "pipeline_start_ts" in audit_columns
        else "run_ts"
        if "run_ts" in audit_columns
        else None
    )
    if timestamp_column is None:
        return None

    watermark = (
        spark.table(audit_table)
        .selectExpr(f"max(`{timestamp_column}`) AS pipeline_start_ts")
        .first()["pipeline_start_ts"]
    )
    return watermark

pipeline_start_ts = get_pipeline_start_ts()
last_pipeline_start_ts = get_last_pipeline_start_ts()

date_query = ""
if last_pipeline_start_ts is not None:
    start_query = last_pipeline_start_ts.strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    date_query = f"&start_date={start_query}&end_date={end_date}"


url = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LATITUDE}"
    f"&longitude={LONGITUDE}"
    "&hourly=temperature_2m,"
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "precipitation"
    f"{date_query}"
)

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

records = []

for i in range(len(data["hourly"]["time"])):
    records.append({
        "timestamp": data["hourly"]["time"][i],
        "temperature": data["hourly"]["temperature_2m"][i],
        "humidity": data["hourly"]["relative_humidity_2m"][i],
        "wind_speed": data["hourly"]["wind_speed_10m"][i],
        "precipitation": data["hourly"]["precipitation"][i]
    })

load_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
weather_df = (
    spark.createDataFrame(records)
    .withColumn("_source_system", lit("OPEN_METEO"))
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_load_id", lit(load_ts))
)

(
    weather_df.write
    .mode("append")
    .parquet(
        f"/Volumes/energy/bronze/raw/weather/weather_{load_ts}"
    )
)

print("Weather raw snapshot written successfully")
```

## src/silver/data_quality.py

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

DQ_FAILURE_TABLE = "energy.silver.dq_failures"


def _log_dq_failures(spark, table_name, failure_details):
    """Write DQ failure records to the DQ failure tracking table."""
    schema = StructType([
        StructField("dq_table", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("failure_reason", StringType(), False),
        StructField("failed_value", LongType(), False),
    ])

    records = [
        (
            table_name,
            d["check_name"],
            d["check_type"],
            d["column_name"],
            d["failure_reason"],
            d["failed_value"],
        )
        for d in failure_details
    ]

    failure_df = spark.createDataFrame(records, schema)
    failure_df = failure_df.withColumn("check_ts", current_timestamp())

    if not spark.catalog.tableExists(DQ_FAILURE_TABLE):
        (
            failure_df.write
            .format("delta")
            .option("mergeSchema", "true")
            .saveAsTable(DQ_FAILURE_TABLE)
        )
    else:
        (
            failure_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(DQ_FAILURE_TABLE)
        )


def validate_data_quality(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    checks: list,
    fail_on_error: bool = False,
) -> DataFrame:
    """Run data quality checks on a DataFrame and add metadata columns.

    When any check fails, the failure details (table name, check name, check
    type, column name, failure reason, failed value count, and timestamp)
    are written to the energy.silver.dq_failures table.

    Args:
        spark: SparkSession
        dataframe: DataFrame to validate
        table_name: Name of the target table (for logging)
        checks: List of dicts with keys:
            - name: str (check name)
            - check_type: str ("not_null", "unique", "range", "not_empty")
            - column: str (column to check; for "unique" can be a single column)
            - columns: list (for multi-column "unique" check)
            - min_val: numeric (optional, for "range" check)
            - max_val: numeric (optional, for "range" check)
        fail_on_error: If True, raise RuntimeError when any check fails.

    Returns:
        DataFrame with dq_passed and dq_failed_checks columns added.
    """
    total_rows = dataframe.count()
    failed_checks = []
    failure_details = []

    for check in checks:
        check_name = check["name"]
        check_type = check["check_type"]

        if check_type == "not_null":
            column = check["column"]
            null_count = dataframe.filter(col(column).isNull()).count()
            if null_count > 0:
                reason = f"{null_count} nulls in {column}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": column,
                    "failure_reason": reason,
                    "failed_value": null_count,
                })

        elif check_type == "unique":
            columns = check.get("columns") or [check["column"]]
            dup_count = total_rows - dataframe.dropDuplicates(columns).count()
            if dup_count > 0:
                reason = f"{dup_count} duplicates on {columns}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": ", ".join(str(c) for c in columns),
                    "failure_reason": reason,
                    "failed_value": dup_count,
                })

        elif check_type == "range":
            column = check["column"]
            min_val = check.get("min_val")
            max_val = check.get("max_val")
            conditions = []
            if min_val is not None:
                conditions.append(col(column) < min_val)
            if max_val is not None:
                conditions.append(col(column) > max_val)
            if conditions:
                range_filter = conditions[0]
                for c in conditions[1:]:
                    range_filter = range_filter | c
                range_filter = range_filter & col(column).isNotNull()
                out_of_range = dataframe.filter(range_filter).count()
                if out_of_range > 0:
                    reason = f"{out_of_range} out-of-range values in {column}"
                    failed_checks.append(f"{check_name}: {reason}")
                    failure_details.append({
                        "check_name": check_name,
                        "check_type": check_type,
                        "column_name": column,
                        "failure_reason": reason,
                        "failed_value": out_of_range,
                    })

        elif check_type == "not_empty":
            if total_rows == 0:
                reason = "0 rows in dataframe"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": "",
                    "failure_reason": reason,
                    "failed_value": 0,
                })

    if failure_details:
        _log_dq_failures(spark, table_name, failure_details)

    dq_summary = "; ".join(failed_checks)
    status = "PASSED" if not failed_checks else "FAILED"
    print(f"[DQ] {table_name}: {status}")
    if failed_checks:
        print(f"[DQ] {table_name}: {dq_summary}")

    if fail_on_error and failed_checks:
        raise RuntimeError(
            f"Data quality checks failed for {table_name}: {dq_summary}"
        )

    return (
        dataframe
        .withColumn("dq_passed", lit(len(failed_checks) == 0))
        .withColumn("dq_failed_checks", lit(dq_summary))
    )
```

## src/silver/dim_customer.py

```python

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    concat,
    lpad,
    current_timestamp,
    when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.dim_customer"

# ==========================================
# SOURCE
# ==========================================

meter_df = spark.table(
    "energy.bronze.bronze_meter_readings"
)

# ==========================================
# DQ
# ==========================================

meter_df = (
    meter_df
    .filter(col("LCLid").isNotNull())
)

# ==========================================
# DIM CUSTOMER
# ==========================================

window_spec = Window.orderBy("LCLid")

base = (
    meter_df
    .select("LCLid", "stdorToU")
    .dropDuplicates(["LCLid"])
)

if spark.catalog.tableExists(TARGET_TABLE):
    existing = spark.table(TARGET_TABLE).select("LCLid", "customer_key")
    max_key = existing.agg({"customer_key": "max"}).first()[0] or 0
    new_customers = base.join(existing.select("LCLid"), "LCLid", "left_anti")
    existing_customers = base.join(existing, "LCLid", "inner")
else:
    max_key = 0
    new_customers = base
    existing_customers = base.limit(0).withColumn("customer_key", lit(None).cast("long"))

new_customers = new_customers.withColumn(
    "customer_key", row_number().over(window_spec) + lit(max_key)
)

dim_customer = existing_customers.unionByName(new_customers).withColumn(
    "customer_id",
    concat(lit("CUST"), lpad(col("customer_key").cast("string"), 6, "0"))
).withColumn(
    "customer_type", col("stdorToU")          # keep raw Std/ToU â€” see Fix 2
).withColumn("tariff_id",
   when(col("stdorToU").isNull(), lit("TAR001"))
   .when(col("stdorToU") == "Std", lit("TAR001"))
   .otherwise(lit("TAR002"))
).withColumn("status", lit("ACTIVE")) \
 .withColumn("region", lit("LONDON")) \
 .withColumn("load_ts", current_timestamp())

dim_customer.show()

dim_customer = validate_data_quality(
    spark,
    dim_customer,
    TARGET_TABLE,
    [
        {"name": "customer_key_not_null", "check_type": "not_null", "column": "customer_key"},
        {"name": "customer_id_not_null", "check_type": "not_null", "column": "customer_id"},
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "customer_key_unique", "check_type": "unique", "column": "customer_key"},
        {"name": "lclid_unique", "check_type": "unique", "column": "LCLid"},
    ],
)

dim_customer = add_schema_drift_metadata(
    spark, dim_customer, TARGET_TABLE
)

dim_customer = dim_customer.select(
    "customer_key",
    "customer_id",
    "LCLid",
    "tariff_id",
    "status",
    "region",
    "load_ts",
    "dq_passed",
    "dq_failed_checks",
    "schema_drift_detected",
    "schema_drift_columns"
)

# ==========================================
# INITIAL LOAD
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        dim_customer.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial dim_customer load complete")

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
            dim_customer.alias("s"),
            "t.LCLid = s.LCLid"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("dim_customer merged successfully")

print(
    f"dim_customer rows: {dim_customer.count()}"
)
'''

import dlt

from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    concat,
    lpad,
    current_timestamp,
    when
)
from pyspark.sql.window import Window


# =====================================================
# SOURCE SNAPSHOT TABLE
# =====================================================

@dlt.table(
    name="dim_customer_source",
    comment="Customer snapshot derived from bronze_meter_readings."
)
@dlt.expect_or_fail(
    "lclid_not_null",
    "LCLid IS NOT NULL"
)
def dim_customer_source():

    window_spec = Window.orderBy("LCLid")

    return (
        spark.read.table(
            "energy.bronze.bronze_meter_readings"
        )
        .filter(
            col("LCLid").isNotNull()
        )
        .select(
            "LCLid",
            "stdorToU"
        )
        .dropDuplicates(
            ["LCLid"]
        )
        .withColumn(
            "customer_key",
            row_number().over(window_spec)
        )
        .withColumn(
            "customer_id",
            concat(
                lit("CUST"),
                lpad(
                    col("customer_key").cast("string"),
                    6,
                    "0"
                )
            )
        )
        .withColumn(
            "tariff_id",
            when(
                col("stdorToU").isNull(),
                lit("TAR001")
            )
            .when(
                col("stdorToU") == "Std",
                lit("TAR001")
            )
            .otherwise(
                lit("TAR002")
            )
        )
        .withColumn(
            "status",
            lit("ACTIVE")
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
# SILVER TARGET TABLE
# =====================================================

dlt.create_streaming_table(
    name="dim_customer",
    comment="Customer Dimension SCD Type 2"
)

# =====================================================
# AUTO CDC
# =====================================================

dlt.create_auto_cdc_from_snapshot_flow(
    target="dim_customer",
    source="dim_customer_source",
    keys=["LCLid"],
    stored_as_scd_type=2
)
'''
```

## src/silver/dim_date.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    quarter,
    month,
    dayofmonth,
    weekofyear,
    dayofweek,
    date_format,
    when,
    monotonically_increasing_id
)
from delta.tables import DeltaTable
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.dim_date"

# =====================================================
# DATE RANGE
# =====================================================

date_df = spark.sql("""
SELECT explode(
    sequence(
        to_date('2010-01-01'),
        to_date('2035-12-31'),
        interval 1 day
    )
) AS full_date
""")

# =====================================================
# DIM DATE
# =====================================================

dim_date = (
    date_df
    .withColumn(
        "date_key",
        date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "year",
        year("full_date")
    )
    .withColumn(
        "quarter",
        quarter("full_date")
    )
    .withColumn(
        "month",
        month("full_date")
    )
    .withColumn(
        "month_name",
        date_format("full_date", "MMMM")
    )
    .withColumn(
        "day",
        dayofmonth("full_date")
    )
    .withColumn(
        "week_of_year",
        weekofyear("full_date")
    )
    .withColumn(
        "day_of_week",
        dayofweek("full_date")
    )
    .withColumn(
        "day_name",
        date_format("full_date", "EEEE")
    )
    .withColumn(
        "is_weekend",
        when(
            dayofweek("full_date").isin(1, 7),
            True
        ).otherwise(False)
    )
)

dim_date = validate_data_quality(
    spark,
    dim_date,
    TARGET_TABLE,
    [
        {"name": "date_key_not_null", "check_type": "not_null", "column": "date_key"},
        {"name": "full_date_not_null", "check_type": "not_null", "column": "full_date"},
        {"name": "date_key_unique", "check_type": "unique", "column": "date_key"},
    ],
)

dim_date = add_schema_drift_metadata(
    spark, dim_date, TARGET_TABLE
)

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        dim_date.write
        .format("delta")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial dim_date load complete")

# =====================================================
# MERGE
# =====================================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            dim_date.alias("s"),
            "t.date_key = s.date_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("dim_date merged successfully")

print(
    f"dim_date rows: {dim_date.count()}"
)
```

## src/silver/dim_meter.py

```python
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
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.dim_meter"

# =====================================================
# SOURCE
# =====================================================

meter_df = spark.table(
    "energy.bronze.bronze_meter_readings"
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
    .select("LCLid")
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
```

## src/silver/dim_tariff.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    date_format,
    current_timestamp,
    lit,
    concat,
    lpad,
    monotonically_increasing_id,
    when,
    expr
)

#from silver import dim_tariff
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

STANDARD_FLAT_RATE = 0.15  # confirm actual Std-tariff policy rate

# =====================================================
# READ BRONZE TABLES
# =====================================================

customer_df = spark.table("energy.silver.dim_customer")
meter_df = spark.table("energy.silver.dim_meter")
tariff_df = spark.table("energy.silver.dim_tariff")
date_df = spark.table("energy.silver.dim_date")
meter_readings = spark.table("energy.bronze.bronze_meter_readings")
consumption_df = spark.table("energy.silver.fact_consumption")

readings = (consumption_df
    .join(
        customer_df.select(
            "customer_key",
            "customer_id",
            "LCLid",
            "tariff_id"
        ),
        "customer_key",
        "left",
    )
    .join(
        meter_df.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "meter_key",
        "left",
    )
)

# =====================================================
# CUSTOMER + METER MAPPING
# =====================================================

customer_meter = (
    customer_df.alias("c")
    .join(
        meter_df.alias("m"),
        "LCLid",
        "inner"
    )
)

# =====================================================
# CLEAN METER READINGS
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "consumption_kwh",
        expr(
            "try_cast(`kwh_hh` as double)"
        )
    )
    .filter(col("consumption_kwh").isNotNull())
    .filter(col("LCLid").isNotNull())
)

# =====================================================
# BILLING PERIOD
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "billing_period",
        date_format(
            col("DateTime"),
            "yyyy-MM"
        )
    )
)

# =====================================================
# MONTHLY AGGREGATION
# =====================================================

billing_base = (
    meter_readings
    .groupBy(
        "LCLid",
        "billing_period"
    )
    .agg(
        spark_sum(
            "consumption_kwh"
        ).alias(
            "consumption_kwh"
        )
    )
)

# =====================================================
# JOIN CUSTOMER/METER
# =====================================================

billing = (
    billing_base
    .join(
        customer_df.select(
            "customer_key",
            "customer_id",
            "LCLid",
            "tariff_id",
            "region"
        ),
        "LCLid",
        "left"
    )
    .join(
        meter_df.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "LCLid",
        "left"
    )
)
'''
# =====================================================
# METER JOIN
# =====================================================

billing = (
    billing
    .join(
        dim_meter.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "LCLid",
        "left"
    )
)
'''

# =====================================================
# TARIFF JOIN
# =====================================================

billing = (
    billing
    .join(
        tariff_df.select(
            "tariff_key",
            "tariff_id",
            "tariff_name",
            "rate_per_kwh"
        ),
        "tariff_id",
        "left"
    )
)

# =====================================================
# TEMPORARY TARIFF LOGIC
# =====================================================
# Replace later using dim_tariff /
# tariff schedule table

billing = (
    billing
    .withColumn(
        "rate_per_kwh",
        when(
            col("tariff_id") == "TAR002",
            lit(0.18)
        ).otherwise(
            lit(0.15)
        )
    )
)

# =====================================================
# BILL AMOUNT
# =====================================================

billing = (
    billing
    .withColumn(
        "bill_amount",
        (
            col("consumption_kwh")
            * col("rate_per_kwh")
        )
    )
)

# =====================================================
# BILL BUSINESS KEY
# =====================================================

billing = (
    billing
    .withColumn(
        "billing_business_key",
        concat(
            col("customer_id"),
            lit("_"),
            col("meter_id"),
            lit("_"),
            col("billing_period")
        )
    )
)

# =====================================================
# BILL ID
# =====================================================

billing = (
    billing
    .withColumn(
        "bill_id",
        concat(
            lit("BILL"),
            col("customer_id"),
            lit("_"),
            col("billing_period")
        )
    )
)

# =====================================================
# AUDIT COLUMNS
# =====================================================

billing = (
    billing
    .withColumn(
        "billing_status",
        lit("CALCULATED")
    )
    .withColumn(
        "load_ts",
        current_timestamp()
    )
    .withColumn(
        "source_system",
        lit("BILLING_ENGINE")
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

billing = billing.select(
    "billing_business_key",
    "bill_id",
    "customer_key",
    "meter_key",
    "tariff_key",
    "tariff_id",
    "customer_id",
    "meter_id",
    "billing_period",
    "consumption_kwh",
    "rate_per_kwh",
    "bill_amount",
    "billing_status",
    "region",
    "load_ts",
    "source_system"
)

billing = validate_data_quality(
    spark,
    billing,
    "energy.silver.fact_billing",
    [
        {"name": "billing_business_key_not_null", "check_type": "not_null", "column": "billing_business_key"},
        {"name": "bill_id_not_null", "check_type": "not_null", "column": "bill_id"},
        {"name": "consumption_non_negative", "check_type": "range", "column": "consumption_kwh", "min_val": 0},
        {"name": "bill_amount_non_negative", "check_type": "range", "column": "bill_amount", "min_val": 0},
        {"name": "billing_business_key_unique", "check_type": "unique", "column": "billing_business_key"},
    ],
)

billing = add_schema_drift_metadata(
    spark, billing, "energy.silver.fact_billing"
)

billing = billing.dropDuplicates(["billing_business_key"])


# =====================================================
# SILVER FACT BILLING
# =====================================================

from delta.tables import DeltaTable

TARGET_TABLE = "energy.silver.fact_billing"

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        billing.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            billing.alias("s"),
            """
            t.billing_business_key =
            s.billing_business_key
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(
    f"Silver fact_billing created: {billing.count()}"
)
```

## src/silver/fact_billing.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    date_format,
    current_timestamp,
    lit,
    concat,
    lpad,
    monotonically_increasing_id,
    when,
    expr
)

#from silver import dim_tariff
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

STANDARD_FLAT_RATE = 0.15  # confirm actual Std-tariff policy rate

# =====================================================
# READ BRONZE TABLES
# =====================================================

customer_df = spark.table("energy.silver.dim_customer")
meter_df = spark.table("energy.silver.dim_meter")
tariff_df = spark.table("energy.silver.dim_tariff")
date_df = spark.table("energy.silver.dim_date")
meter_readings = spark.table("energy.bronze.bronze_meter_readings")
consumption_df = spark.table("energy.silver.fact_consumption")

readings = (consumption_df
    .join(
        customer_df.select(
            "customer_key",
            "customer_id",
            "LCLid",
            "tariff_id"
        ),
        "customer_key",
        "left",
    )
    .join(
        meter_df.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "meter_key",
        "left",
    )
)

# =====================================================
# CUSTOMER + METER MAPPING
# =====================================================

customer_meter = (
    customer_df.alias("c")
    .join(
        meter_df.alias("m"),
        "LCLid",
        "inner"
    )
)

# =====================================================
# CLEAN METER READINGS
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "consumption_kwh",
        expr(
            "try_cast(`kwh_hh` as double)"
        )
    )
    .filter(col("consumption_kwh").isNotNull())
    .filter(col("LCLid").isNotNull())
)

# =====================================================
# BILLING PERIOD
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "billing_period",
        date_format(
            col("DateTime"),
            "yyyy-MM"
        )
    )
)

# =====================================================
# MONTHLY AGGREGATION
# =====================================================

billing_base = (
    meter_readings
    .groupBy(
        "LCLid",
        "billing_period"
    )
    .agg(
        spark_sum(
            "consumption_kwh"
        ).alias(
            "consumption_kwh"
        )
    )
)

# =====================================================
# JOIN CUSTOMER/METER
# =====================================================

billing = (
    billing_base
    .join(
        customer_df.select(
            "customer_key",
            "customer_id",
            "LCLid",
            "tariff_id",
            "region"
        ),
        "LCLid",
        "left"
    )
    .join(
        meter_df.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "LCLid",
        "left"
    )
)
'''
# =====================================================
# METER JOIN
# =====================================================

billing = (
    billing
    .join(
        dim_meter.select(
            "meter_key",
            "meter_id",
            "LCLid"
        ),
        "LCLid",
        "left"
    )
)
'''

# =====================================================
# TARIFF JOIN
# =====================================================

billing = (
    billing
    .join(
        tariff_df.select(
            "tariff_key",
            "tariff_id",
            "tariff_name",
            "rate_per_kwh"
        ),
        "tariff_id",
        "left"
    )
)

# =====================================================
# TEMPORARY TARIFF LOGIC
# =====================================================
# Replace later using dim_tariff /
# tariff schedule table

billing = (
    billing
    .withColumn(
        "rate_per_kwh",
        when(
            col("tariff_id") == "TAR002",
            lit(0.18)
        ).otherwise(
            lit(0.15)
        )
    )
)

# =====================================================
# BILL AMOUNT
# =====================================================

billing = (
    billing
    .withColumn(
        "bill_amount",
        (
            col("consumption_kwh")
            * col("rate_per_kwh")
        )
    )
)

# =====================================================
# BILL BUSINESS KEY
# =====================================================

billing = (
    billing
    .withColumn(
        "billing_business_key",
        concat(
            col("customer_id"),
            lit("_"),
            col("meter_id"),
            lit("_"),
            col("billing_period")
        )
    )
)

# =====================================================
# BILL ID
# =====================================================

billing = (
    billing
    .withColumn(
        "bill_id",
        concat(
            lit("BILL"),
            col("customer_id"),
            lit("_"),
            col("billing_period")
        )
    )
)

# =====================================================
# AUDIT COLUMNS
# =====================================================

billing = (
    billing
    .withColumn(
        "billing_status",
        lit("CALCULATED")
    )
    .withColumn(
        "load_ts",
        current_timestamp()
    )
    .withColumn(
        "source_system",
        lit("BILLING_ENGINE")
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

billing = billing.select(
    "billing_business_key",
    "bill_id",
    "customer_key",
    "meter_key",
    "tariff_key",
    "tariff_id",
    "customer_id",
    "meter_id",
    "billing_period",
    "consumption_kwh",
    "rate_per_kwh",
    "bill_amount",
    "billing_status",
    "region",
    "load_ts",
    "source_system"
)

billing = validate_data_quality(
    spark,
    billing,
    "energy.silver.fact_billing",
    [
        {"name": "billing_business_key_not_null", "check_type": "not_null", "column": "billing_business_key"},
        {"name": "bill_id_not_null", "check_type": "not_null", "column": "bill_id"},
        {"name": "consumption_non_negative", "check_type": "range", "column": "consumption_kwh", "min_val": 0},
        {"name": "bill_amount_non_negative", "check_type": "range", "column": "bill_amount", "min_val": 0},
        {"name": "billing_business_key_unique", "check_type": "unique", "column": "billing_business_key"},
    ],
)

billing = add_schema_drift_metadata(
    spark, billing, "energy.silver.fact_billing"
)

billing = billing.dropDuplicates(["billing_business_key"])


# =====================================================
# SILVER FACT BILLING
# =====================================================

from delta.tables import DeltaTable

TARGET_TABLE = "energy.silver.fact_billing"

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        billing.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            billing.alias("s"),
            """
            t.billing_business_key =
            s.billing_business_key
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(
    f"Silver fact_billing created: {billing.count()}"
)
```

## src/silver/fact_consumption.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    to_date,
    date_format
)
from delta.tables import DeltaTable
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.fact_consumption"

# =====================================================
# SOURCE TABLES
# =====================================================

meter_readings = spark.table(
    "energy.bronze.bronze_meter_readings"
)

dim_customer = spark.table(
    "energy.silver.dim_customer"
)

dim_meter = spark.table(
    "energy.silver.dim_meter"
)

dim_date = spark.table(
    "energy.silver.dim_date"
)

# =====================================================
# CLEAN DATA
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "consumption_kwh",
        expr(
            "try_cast(`kwh_hh` as double)"
        )
    )
    .filter(col("consumption_kwh").isNotNull())
    .filter(col("LCLid").isNotNull())
)

# =====================================================
# DATE KEY
# =====================================================

meter_readings = (
    meter_readings
    .withColumn(
        "full_date",
        to_date(col("DateTime"))
    )
    .withColumn(
        "date_key",
        date_format(
            col("full_date"),
            "yyyyMMdd"
        ).cast("int")
    )
)

# =====================================================
# CUSTOMER JOIN
# =====================================================

fact_df = (
    meter_readings
    .join(
        dim_customer.select(
            "customer_key",
            "LCLid",
            "region"
        ),
        "LCLid",
        "left"
    )
)

# =====================================================
# METER JOIN
# =====================================================

fact_df = (
    fact_df
    .join(
        dim_meter.select(
            "meter_key",
            "LCLid"
        ),
        "LCLid",
        "left"
    )
)

# =====================================================
# DATE JOIN
# =====================================================

fact_df = (
    fact_df
    .join(
        dim_date.select(
            "date_key"
        ),
        "date_key",
        "left"
    )
)

# =====================================================
# BUSINESS KEY
# =====================================================

fact_df = (
    fact_df
    .withColumn(
        "consumption_business_key",
        expr(
            """
            concat(
                LCLid,
                '_',
                cast(DateTime as string)
            )
            """
        )
    )
)

# =====================================================
# AUDIT COLUMNS
# =====================================================

fact_df = (
    fact_df
    .withColumn(
        "load_ts",
        current_timestamp()
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

fact_df = fact_df.select(
    "consumption_business_key",
    "customer_key",
    "meter_key",
    "date_key",
    col("DateTime").alias("reading_timestamp"),
    "consumption_kwh",
    "region",
    "load_ts"
)

fact_df = validate_data_quality(
    spark,
    fact_df,
    TARGET_TABLE,
    [
        {"name": "consumption_business_key_not_null", "check_type": "not_null", "column": "consumption_business_key"},
        {"name": "consumption_kwh_non_negative", "check_type": "range", "column": "consumption_kwh", "min_val": 0},
        {"name": "consumption_business_key_unique", "check_type": "unique", "column": "consumption_business_key"},
    ],
)

fact_df = add_schema_drift_metadata(
    spark, fact_df, TARGET_TABLE
)

fact_df = fact_df.dropDuplicates(["consumption_business_key"])

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        fact_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial fact_consumption load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            fact_df.alias("s"),
            """
            t.consumption_business_key =
            s.consumption_business_key
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("fact_consumption merged successfully")

print(
    f"fact_consumption rows: {fact_df.count()}"
)
```

## src/silver/fact_eia_demand.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    date_format,
    hour,
    lit,
    to_date,
    to_timestamp,
)
from delta.tables import DeltaTable
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.fact_eia_demand"

# =====================================================
# SOURCE TABLES
# =====================================================

eia_df = spark.table(
    "energy.bronze.bronze_eia"
)

dim_date = spark.table(
    "energy.silver.dim_date"
)

# =====================================================
# CLEAN EIA DATA
# =====================================================

fact_df = (
    eia_df
    .filter(col("period").isNotNull())
    .filter(col("respondent").isNotNull())
    .filter(col("type").isNotNull())
    .withColumn(
        "demand_timestamp",
        to_timestamp(col("period"), "yyyy-MM-dd'T'HH")
    )
    .withColumn(
        "full_date",
        to_date(col("demand_timestamp"))
    )
    .withColumn(
        "date_key",
        date_format(col("full_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "hour_of_day",
        hour(col("demand_timestamp"))
    )
    .withColumn(
        "demand_mwh",
        col("value").cast("double")
    )
    .withColumn(
        "demand_business_key",
        concat(
            col("respondent"),
            lit("_"),
            col("period"),
            lit("_"),
            col("type")
        )
    )
    .withColumn(
        "load_ts",
        current_timestamp()
    )
    .join(
        dim_date.select("date_key"),
        "date_key",
        "left"
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

fact_df = fact_df.select(
    "demand_business_key",
    "date_key",
    "respondent",
    col("respondent-name").alias("respondent_name"),
    "type",
    col("type-name").alias("type_name"),
    col("value-units").alias("value_units"),
    "demand_timestamp",
    "hour_of_day",
    "demand_mwh",
    "load_ts",
)

fact_df = validate_data_quality(
    spark,
    fact_df,
    TARGET_TABLE,
    [
        {"name": "demand_business_key_not_null", "check_type": "not_null", "column": "demand_business_key"},
        {"name": "respondent_not_null", "check_type": "not_null", "column": "respondent"},
        {"name": "demand_mwh_non_negative", "check_type": "range", "column": "demand_mwh", "min_val": 0},
        {"name": "demand_business_key_unique", "check_type": "unique", "column": "demand_business_key"},
    ],
)

fact_df = add_schema_drift_metadata(
    spark, fact_df, TARGET_TABLE
)

fact_df = fact_df.dropDuplicates(["demand_business_key"])

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        fact_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial fact_eia_demand load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            fact_df.alias("s"),
            "t.demand_business_key = s.demand_business_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("fact_eia_demand merged successfully")

print(f"fact_eia_demand rows: {fact_df.count()}")
```

## src/silver/fact_feeder_readings.py

```python
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
    print(f"âœ… Initial load complete for {TARGET_TABLE}")
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
    print(f"âœ… Merged records successfully into {TARGET_TABLE}")

print(f"Rows processed into Silver: {silver_feeder.count()}")
```

## src/silver/fact_meter_events.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    trim,
    upper,
    when,
    current_timestamp,
    date_format
)
from delta.tables import DeltaTable

from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "energy.bronze.bronze_meter_events"
TARGET_TABLE = "energy.silver.fact_meter_events"

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
    bronze_df
    .select(
        trim(col("event_id")).alias("event_id"),
        trim(col("LCLid")).alias("LCLid"),
        upper(trim(col("event_type"))).alias("event_type"),
        upper(trim(col("severity"))).alias("severity"),
        to_timestamp(col("event_timestamp")).alias("event_timestamp"),
        trim(col("source_system")).alias("source_system")
    )
    .filter(
        col("event_id").isNotNull() & 
        col("LCLid").isNotNull() & 
        col("event_timestamp").isNotNull()
    )
    # Standardize/validate severity categories
    .withColumn(
        "severity",
        when(col("severity").isin("CRITICAL", "HIGH", "MEDIUM", "LOW"), col("severity"))
        .otherwise("UNKNOWN")
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

silver_events = add_schema_drift_metadata(
    spark,
    silver_events,
    TARGET_TABLE
)

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
    "schema_drift_columns"
)

# ==========================================
# 5. INITIAL LOAD & INCREMENTAL MERGE
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
    (
        silver_events.write
        .format("delta")
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"âœ… Initial load complete for {TARGET_TABLE}")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
  
    (
        target.alias("t")
        .merge(
            silver_events.alias("s"),
            "t.event_id = s.event_id AND t.event_date = s.event_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"âœ… Merged records successfully into {TARGET_TABLE}")

print(f"Rows processed into Silver: {silver_events.count()}")
```

## src/silver/fact_weather.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    date_format,
    hour,
    lit,
    to_date,
    to_timestamp,
)
from delta.tables import DeltaTable
from data_quality import validate_data_quality
from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "energy.silver.fact_weather"

# =====================================================
# SOURCE TABLES
# =====================================================

weather_df = spark.table(
    "energy.bronze.bronze_weather"
)

dim_date = spark.table(
    "energy.silver.dim_date"
)

# =====================================================
# CLEAN WEATHER DATA
# =====================================================

fact_df = (
    weather_df
    .filter(col("timestamp").isNotNull())
    .withColumn(
        "weather_timestamp",
        to_timestamp(col("timestamp"))
    )
    .withColumn(
        "full_date",
        to_date(col("weather_timestamp"))
    )
    .withColumn(
        "date_key",
        date_format(col("full_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "hour_of_day",
        hour(col("weather_timestamp"))
    )
    .withColumn(
        "weather_business_key",
        concat(
            date_format(col("weather_timestamp"), "yyyyMMdd"),
            lit("_"),
            date_format(col("weather_timestamp"), "HH")
        )
    )
    .withColumn(
        "temperature_c",
        col("temperature").cast("double")
    )
    .withColumn(
        "humidity_pct",
        col("humidity").cast("double")
    )
    .withColumn(
        "wind_speed_kmh",
        col("wind_speed").cast("double")
    )
    .withColumn(
        "precipitation_mm",
        col("precipitation").cast("double")
    )
    .withColumn(
        "load_ts",
        current_timestamp()
    )
    .join(
        dim_date.select("date_key"),
        "date_key",
        "left"
    )
)

# =====================================================
# FINAL COLUMNS
# =====================================================

fact_df = fact_df.select(
    "weather_business_key",
    "date_key",
    "weather_timestamp",
    "hour_of_day",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "precipitation_mm",
    "load_ts",
)

fact_df = validate_data_quality(
    spark,
    fact_df,
    TARGET_TABLE,
    [
        {"name": "weather_business_key_not_null", "check_type": "not_null", "column": "weather_business_key"},
        {"name": "temperature_range", "check_type": "range", "column": "temperature_c", "min_val": -50, "max_val": 60},
        {"name": "humidity_range", "check_type": "range", "column": "humidity_pct", "min_val": 0, "max_val": 100},
        {"name": "wind_speed_non_negative", "check_type": "range", "column": "wind_speed_kmh", "min_val": 0},
        {"name": "weather_business_key_unique", "check_type": "unique", "column": "weather_business_key"},
    ],
)

fact_df = add_schema_drift_metadata(
    spark, fact_df, TARGET_TABLE
)

fact_df = fact_df.dropDuplicates(["weather_business_key"])

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        fact_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial fact_weather load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            fact_df.alias("s"),
            "t.weather_business_key = s.weather_business_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("fact_weather merged successfully")

print(f"fact_weather rows: {fact_df.count()}")
```

## src/silver/schema_drift.py

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


def add_schema_drift_metadata(
    spark: SparkSession,
    dataframe: DataFrame,
    target_table: str,
) -> DataFrame:
    """Annotate a silver DataFrame with newly added or changed source columns."""
    if spark.catalog.tableExists(target_table):
        target_types = {
            field.name: field.dataType.simpleString()
            for field in spark.table(target_table).schema
        }
    else:
        target_types = {}

    drift_columns = []
    for field in dataframe.schema:
        source_type = field.dataType.simpleString()
        target_type = target_types.get(field.name)
        if target_type is None:
            if target_types:
                drift_columns.append(f"{field.name} (new column)")
        elif target_type != source_type:
            drift_columns.append(
                f"{field.name} (type changed: {target_type} -> {source_type})"
            )

    drift_summary = ", ".join(drift_columns)
    return (
        dataframe
        .withColumn("schema_drift_detected", lit(bool(drift_columns)))
        .withColumn("schema_drift_columns", lit(drift_summary))
    )
```
