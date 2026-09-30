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