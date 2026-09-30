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