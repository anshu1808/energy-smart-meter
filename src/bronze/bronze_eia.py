import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

catalog = get_catalog()

spark = SparkSession.builder.getOrCreate()

eia_df = (
    spark.read.option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet("/Volumes/energy/bronze/raw/eia")
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("EIA_API"))
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_file_name", col("_metadata.file_name"))
    .withColumn("source_file_size", col("_metadata.file_size"))
    .withColumn("source_file_modification_time", col("_metadata.file_modification_time"))
)
eia_df = add_schema_drift_metadata(spark, eia_df, f"{catalog}.bronze.bronze_eia")

eia_df = eia_df.dropDuplicates(["period", "respondent", "type"])

eia_df = validate_data_quality(
    spark,
    eia_df,
    f"{catalog}.bronze.bronze_eia",
    [
        {"name": "period_not_null", "check_type": "not_null", "column": "period"},
        {"name": "respondent_not_null", "check_type": "not_null", "column": "respondent"},
        {"name": "type_not_null", "check_type": "not_null", "column": "type"},
        {"name": "value_not_null", "check_type": "not_null", "column": "value"},
    ],
)

eia_df.show()

target_table = f"{catalog}.bronze.bronze_eia"

if spark.catalog.tableExists(target_table):

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("t")
        .merge(
            eia_df.alias("s"),
            """
            t.period = s.period
            AND t.respondent = s.respondent
            AND t.type = s.type
            AND t.source_system = s.source_system
            AND t.`type-name` = s.`type-name`
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (eia_df.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table))
