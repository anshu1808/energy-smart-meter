from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, current_timestamp, date_format
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from delta.tables import DeltaTable
from data_quality import validate_data_quality
#from schema_drift import add_schema_drift_metadata

spark = SparkSession.builder.getOrCreate()
#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET_TABLE = "energy.gold.gold_peak_load"

# =====================================================
# SOURCE TABLES
# =====================================================

fact_consumption = spark.table("energy.silver.fact_consumption")


gold_df = (
	fact_consumption
	.withColumn("reading_date", date_format("reading_timestamp", "yyyy-MM-dd"))
	.withColumn("hour_of_day", date_format("reading_timestamp", "HH").cast("int"))
	.groupBy("date_key", "reading_date", "hour_of_day")
	.agg(
		count("consumption_business_key").alias("reading_count"),
		spark_sum("consumption_kwh").alias("total_consumption_kwh"),
		avg("consumption_kwh").alias("avg_consumption_kwh"),
		spark_max("consumption_kwh").alias("max_meter_consumption_kwh"),
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
		{"name": "peak_load_not_empty", "check_type": "not_empty"},
		{"name": "date_key_not_null", "check_type": "not_null", "column": "date_key"},
		{"name": "hour_of_day_not_null", "check_type": "not_null", "column": "hour_of_day"},
		{"name": "peak_load_key_unique", "check_type": "unique", "columns": ["date_key", "hour_of_day"]},
		{"name": "total_consumption_non_negative", "check_type": "range", "column": "total_consumption_kwh", "min_val": 0},
	],
)

# ==========================================
# INITIAL LOAD
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):
	gold_df.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# ==========================================
# INCREMENTAL MERGE
# ==========================================

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

print(f"gold_peak_load rows: {gold_df.count()}")
