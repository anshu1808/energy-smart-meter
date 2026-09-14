from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp

builder = (
    SparkSession.builder
    .appName("bronze_customer")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

customer_df = (
    spark.read
    .option("header", "true")
    .csv("datasets/master/customer_master.csv")
)

bronze_customer = (
    customer_df
    .withColumn("load_timestamp", current_timestamp())
)

(
    bronze_customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "energy.bronze.bronze_customer"
    )
)