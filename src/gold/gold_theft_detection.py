import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, date_format, lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import stddev_pop
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()
#
TARGET_TABLE = f"{catalog}.gold.gold_theft_detection"
consumption = spark.table(f"{catalog}.silver.fact_consumption")
customers = spark.table(f"{catalog}.silver.dim_customer")

meter_monthly = (
	consumption
	.join(customers.select("customer_key", "LCLid", "tariff_id"), "customer_key", "left")
	.withColumn("billing_period", date_format("reading_timestamp", "yyyy-MM"))
	.groupBy("billing_period", "customer_key", "LCLid", "tariff_id")
	.agg(spark_sum("consumption_kwh").alias("consumption_kwh"))
)

peer_stats = (
	meter_monthly
	.groupBy("billing_period", "tariff_id")
	.agg(
		avg("consumption_kwh").alias("peer_avg_consumption_kwh"),
		stddev_pop("consumption_kwh").alias("peer_stddev_consumption_kwh"),
	)
)

gold_df = (
	meter_monthly
	.join(peer_stats, ["billing_period", "tariff_id"], "left")
	.withColumn(
		"variance_from_peer_kwh",
		spark_round(col("consumption_kwh") - col("peer_avg_consumption_kwh"), 6),
	)
	.withColumn(
		"theft_flag",
		when(
			(col("consumption_kwh") < col("peer_avg_consumption_kwh") - 2 * col("peer_stddev_consumption_kwh"))
			& (col("consumption_kwh") < col("peer_avg_consumption_kwh") * lit(0.5)),
			lit(True),
		).otherwise(lit(False)),
	)
	.withColumn(
		"detection_reason",
		when(col("theft_flag"), lit("Consumption is materially below tariff peer usage"))
		.otherwise(lit("Within expected tariff peer range")),
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
		{"name": "theft_detection_not_empty", "check_type": "not_empty"},
		{"name": "billing_period_not_null", "check_type": "not_null", "column": "billing_period"},
		{"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
		{"name": "theft_detection_key_unique", "check_type": "unique", "columns": ["billing_period", "LCLid"]},
		{"name": "consumption_non_negative", "check_type": "range", "column": "consumption_kwh", "min_val": 0},
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
			"t.billing_period = s.billing_period AND t.LCLid = s.LCLid",
		)
		.withSchemaEvolution()
		.whenMatchedUpdateAll()
		.whenNotMatchedInsertAll()
		.execute()
	)

print(f"gold_theft_detection rows: {gold_df.count()}")
