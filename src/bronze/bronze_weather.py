from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("multiline", "true").json(
    "datasets/weather/*.json"
)

bronze_df = (
    df.withColumn("load_timestamp", current_timestamp())
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("energy.bronze.bronze_weather")
)