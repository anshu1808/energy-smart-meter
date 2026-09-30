import sys
from pathlib import Path

from delta.tables import DeltaTable
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.silver.fact_eia_demand"

# =====================================================
# SOURCE TABLES
# =====================================================

eia_df = spark.table(f"{catalog}.bronze.bronze_eia")

dim_date = spark.table(f"{catalog}.silver.dim_date")

# =====================================================
# CLEAN EIA DATA
# =====================================================

fact_df = (
    eia_df.filter(col("period").isNotNull())
    .filter(col("respondent").isNotNull())
    .filter(col("type").isNotNull())
    .withColumn("demand_timestamp", to_timestamp(col("period"), "yyyy-MM-dd'T'HH"))
    .withColumn("full_date", to_date(col("demand_timestamp")))
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("hour_of_day", hour(col("demand_timestamp")))
    .withColumn("demand_mwh", col("value").cast("double"))
    .withColumn("demand_business_key", concat(col("respondent"), lit("_"), col("period"), lit("_"), col("type")))
    .withColumn("load_ts", current_timestamp())
    .join(dim_date.select("date_key"), "date_key", "left")
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

fact_df = add_schema_drift_metadata(spark, fact_df, TARGET_TABLE)

fact_df = fact_df.dropDuplicates(["demand_business_key"])

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (fact_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE))

    print("Initial fact_eia_demand load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("t")
        .merge(fact_df.alias("s"), "t.demand_business_key = s.demand_business_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("fact_eia_demand merged successfully")

print(f"fact_eia_demand rows: {fact_df.count()}")
