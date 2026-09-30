import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, current_timestamp
from pyspark.sql.functions import sum as spark_sum
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()
#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET_TABLE = f"{catalog}.gold.gold_revenue_summary"
fact_billing = spark.table(f"{catalog}.silver.fact_billing")

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

# ==========================================
# DATA QUALITY
# ==========================================

gold_df = validate_data_quality(
    spark,
    gold_df,
    TARGET_TABLE,
    [
        {"name": "revenue_summary_not_empty", "check_type": "not_empty"},
        {"name": "billing_period_not_null", "check_type": "not_null", "column": "billing_period"},
        {"name": "billing_period_unique", "check_type": "unique", "column": "billing_period"},
        {"name": "total_consumption_non_negative", "check_type": "range", "column": "total_consumption_kwh", "min_val": 0},
        {"name": "total_revenue_non_negative", "check_type": "range", "column": "total_revenue", "min_val": 0},
    ],
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