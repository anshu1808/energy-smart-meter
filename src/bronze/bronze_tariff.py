import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from schema_drift import add_schema_drift_metadata
from data_quality import validate_data_quality
spark = SparkSession.builder.getOrCreate()

tariff_pd = pd.read_excel("/Volumes/energy/bronze/raw/tariff/Tariffs.xlsx")
tariff_df = (
    spark.createDataFrame(tariff_pd)
    .withColumn("load_ts", current_timestamp())
    .withColumn("source_system", lit("TARIFF_FILE"))
    .withColumn(
        "source_file",
        lit("/Volumes/energy/bronze/raw/tariff/Tariffs.xlsx")
    )
    .withColumn("source_file_name", lit("Tariffs.xlsx"))
    .withColumn("source_file_size", lit(None).cast("long"))
    .withColumn(
        "source_file_modification_time",
        lit(None).cast("timestamp"),
    )
)
tariff_df = add_schema_drift_metadata(
    spark, tariff_df, "energy.bronze.bronze_tariff"
)

tariff_df = validate_data_quality(
    spark,
    tariff_df,
    "energy.bronze.bronze_tariff",
    [
        {"name": "tariff_datetime_not_null", "check_type": "not_null", "column": "TariffDateTime"},
        {"name": "tariff_not_null", "check_type": "not_null", "column": "Tariff"},
        {"name": "tariff_unique", "check_type": "unique", "column": "TariffDateTime", "columns": ["TariffDateTime", "Tariff"]},
    ],
)

tariff_df.show()

target_table = "energy.bronze.bronze_tariff"

if spark.catalog.tableExists(target_table):

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target.alias("t")
        .merge(
            tariff_df.alias("s"),
            """
            t.TariffDateTime = s.TariffDateTime
            and t.Tariff = s.Tariff
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        tariff_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

print("bronze_tariff loaded")