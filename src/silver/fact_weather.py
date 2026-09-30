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

TARGET_TABLE = f"{catalog}.silver.fact_weather"

# =====================================================
# SOURCE TABLES
# =====================================================

weather_df = spark.table(f"{catalog}.bronze.bronze_weather")

dim_date = spark.table(f"{catalog}.silver.dim_date")

# =====================================================
# CLEAN WEATHER DATA
# =====================================================

fact_df = (
    weather_df.filter(col("timestamp").isNotNull())
    .withColumn("weather_timestamp", to_timestamp(col("timestamp")))
    .withColumn("full_date", to_date(col("weather_timestamp")))
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("hour_of_day", hour(col("weather_timestamp")))
    .withColumn(
        "weather_business_key",
        concat(
            date_format(col("weather_timestamp"), "yyyyMMdd"), lit("_"), date_format(col("weather_timestamp"), "HH")
        ),
    )
    .withColumn("temperature_c", col("temperature").cast("double"))
    .withColumn("humidity_pct", col("humidity").cast("double"))
    .withColumn("wind_speed_kmh", col("wind_speed").cast("double"))
    .withColumn("precipitation_mm", col("precipitation").cast("double"))
    .withColumn("load_ts", current_timestamp())
    .join(dim_date.select("date_key"), "date_key", "left")
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

fact_df = add_schema_drift_metadata(spark, fact_df, TARGET_TABLE)

fact_df = fact_df.dropDuplicates(["weather_business_key"])

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (fact_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE))

    print("Initial fact_weather load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("t")
        .merge(fact_df.alias("s"), "t.weather_business_key = s.weather_business_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("fact_weather merged successfully")

print(f"fact_weather rows: {fact_df.count()}")
