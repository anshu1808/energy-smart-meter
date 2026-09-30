import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402


spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

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
    spark, bronze_df, f"{catalog}.bronze.bronze_weather"
)

bronze_df = bronze_df.dropDuplicates(["timestamp"])

bronze_df = validate_data_quality(
    spark,
    bronze_df,
    f"{catalog}.bronze.bronze_weather",
    [
        {"name": "timestamp_not_null", "check_type": "not_null", "column": "timestamp"},
        {"name": "humidity_range", "check_type": "range", "column": "humidity", "min_val": 0, "max_val": 100},
        {"name": "temperature_range", "check_type": "range", "column": "temperature", "min_val": -50, "max_val": 60},
        {"name": "wind_speed_non_negative", "check_type": "range", "column": "wind_speed", "min_val": 0},
        {"name": "precipitation_non_negative", "check_type": "range", "column": "precipitation", "min_val": 0},
    ],
)

bronze_df.show()

target_table = f"{catalog}.bronze.bronze_weather"

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