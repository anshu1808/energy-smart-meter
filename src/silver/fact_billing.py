import sys
from pathlib import Path

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
    expr,date_trunc,
)

#from silver import dim_tariff
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

STANDARD_FLAT_RATE = 0.15  # confirm actual Std-tariff policy rate

# =====================================================
# READ BRONZE TABLES
# =====================================================

customer_df = spark.table(f"{catalog}.silver.dim_customer")
meter_df = spark.table(f"{catalog}.silver.dim_meter")
tariff_df = spark.table(f"{catalog}.silver.dim_tariff")
date_df = spark.table(f"{catalog}.silver.dim_date")
meter_readings = spark.table(f"{catalog}.bronze.bronze_meter_readings")
consumption_df = spark.table(f"{catalog}.silver.fact_consumption")

readings = (consumption_df
    .join(
        customer_df.select(
            "customer_key",
            "customer_id",
            "LCLid",
            "tariff_id",
            "customer_type",
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
# ATTACH TARIFF RATE FOR THE HALF-HOUR (ToU only)
# =====================================================

readings = readings.withColumn(
    "reading_half_hour", date_trunc("minute", col("reading_timestamp"))
)

readings = readings.join(
    tariff_df.select(
        col("tariff_half_hour"),
        col("tariff_key"),
        col("rate_per_kwh").alias("schedule_rate_per_kwh"),
    ),
    readings.reading_half_hour == tariff_df.tariff_half_hour,
    "left",
)

# =====================================================
# APPLY RATE: FLAT FOR Std, SCHEDULE FOR ToU
# =====================================================

readings = (
    readings
    .withColumn(
        "applied_rate_per_kwh",
        when(col("customer_type") == "Std", lit(STANDARD_FLAT_RATE))
        .otherwise(col("schedule_rate_per_kwh")),
    )
    .withColumn(
        "applied_tariff_key",
        when(col("customer_type") == "Std", lit(None).cast("int"))
        .otherwise(col("tariff_key")),
    )
    .withColumn(
        "applied_tariff_id",
        when(col("customer_type") == "Std", lit("FLAT"))
        .otherwise(col("tariff_id")),
    )
    .withColumn("half_hour_cost", col("consumption_kwh") * col("applied_rate_per_kwh"))
    .withColumn("billing_period", date_format(col("reading_timestamp"), "yyyy-MM"))
)

# =====================================================
# MONTHLY AGGREGATION
# =====================================================

billing = (
    readings
    .groupBy("customer_key", "meter_key", "customer_id", "meter_id", "billing_period", "tariff_id", "region")
    .agg(
        spark_sum("consumption_kwh").alias("consumption_kwh"),
        spark_sum("half_hour_cost").alias("bill_amount"),
    )
)

# =====================================================
# BUSINESS KEYS + AUDIT
# =====================================================

billing = (
    billing
    .withColumn(
        "billing_business_key",
        concat(col("customer_id"), lit("_"), col("meter_id"), lit("_"), col("billing_period")),
    )
    .withColumn(
        "bill_id",
        concat(lit("BILL"), col("customer_id"), lit("_"), col("billing_period")),
    )
    .withColumn("billing_status", lit("CALCULATED"))
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("BILLING_ENGINE"))
)

# =====================================================
# FINAL COLUMNS
# =====================================================

billing = billing.select(
    "billing_business_key",
    "bill_id",
    "customer_key",
    "meter_key",
    "tariff_id",
    "customer_id",
    "meter_id",
    "billing_period",
    "consumption_kwh",
    "bill_amount",
    "billing_status",
    "region",
    "load_ts",
    "source_system"
)

billing = validate_data_quality(
    spark,
    billing,
    f"{catalog}.silver.fact_billing",
    [
        {"name": "billing_business_key_not_null", "check_type": "not_null", "column": "billing_business_key"},
        {"name": "bill_id_not_null", "check_type": "not_null", "column": "bill_id"},
        {"name": "consumption_non_negative", "check_type": "range", "column": "consumption_kwh", "min_val": 0},
        {"name": "bill_amount_non_negative", "check_type": "range", "column": "bill_amount", "min_val": 0},
        {"name": "billing_business_key_unique", "check_type": "unique", "column": "billing_business_key"},
    ],
)

billing = add_schema_drift_metadata(
    spark, billing, f"{catalog}.silver.fact_billing"
)

billing = billing.dropDuplicates(["billing_business_key"])


# =====================================================
# SILVER FACT BILLING
# =====================================================

from delta.tables import DeltaTable

TARGET_TABLE = f"{catalog}.silver.fact_billing"

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

    # Align schemas: add new source columns to target, add stale target columns to source as nulls
    _target_schema = {f.name: f.dataType for f in spark.table(TARGET_TABLE).schema.fields}
    for _f in billing.schema.fields:
        if _f.name not in _target_schema:
            spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMNS ({_f.name} {_f.dataType.simpleString()})")
    _target_schema = {f.name: f.dataType for f in spark.table(TARGET_TABLE).schema.fields}
    for _name, _dtype in _target_schema.items():
        if _name not in billing.columns:
            billing = billing.withColumn(_name, lit(None).cast(_dtype))

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