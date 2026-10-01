import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, hour
from pyspark.sql.functions import sum as spark_sum

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET_TABLE = f"{catalog}.gold.gold_weather_impact"
consumption = spark.table(f"{catalog}.silver.fact_consumption")
weather = spark.table(f"{catalog}.silver.fact_weather")

consumption_hourly = (
    consumption.withColumn("hour_of_day", hour("reading_timestamp"))
    .groupBy("date_key", "hour_of_day")
    .agg(
        spark_sum("consumption_kwh").alias("total_consumption_kwh"),
        avg("consumption_kwh").alias("avg_meter_consumption_kwh"),
    )
)

weather_hourly = weather.select(
    "date_key",
    "hour_of_day",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "precipitation_mm",
).dropDuplicates(["date_key", "hour_of_day"])

gold_df = weather_hourly.join(consumption_hourly, ["date_key", "hour_of_day"], "left").withColumn(
    "load_ts", current_timestamp()
)

# ==========================================
# DATA QUALITY
# ==========================================

gold_df = validate_data_quality(
    spark,
    gold_df,
    TARGET_TABLE,
    [
        {"name": "weather_impact_not_empty", "check_type": "not_empty"},
        {"name": "date_key_not_null", "check_type": "not_null", "column": "date_key"},
        {"name": "hour_of_day_not_null", "check_type": "not_null", "column": "hour_of_day"},
        {"name": "weather_impact_key_unique", "check_type": "unique", "columns": ["date_key", "hour_of_day"]},
        {"name": "temperature_range", "check_type": "range", "column": "temperature_c", "min_val": -50, "max_val": 60},
        {"name": "humidity_range", "check_type": "range", "column": "humidity_pct", "min_val": 0, "max_val": 100},
        {"name": "precipitation_non_negative", "check_type": "range", "column": "precipitation_mm", "min_val": 0},
    ],
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
