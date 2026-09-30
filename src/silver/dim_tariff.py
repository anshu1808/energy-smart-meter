import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_timestamp,
    when,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.silver.dim_tariff"

# =====================================================
# SOURCE - derive tariff dimension from bronze_tariff
# =====================================================

bronze_tariff = spark.table(f"{catalog}.bronze.bronze_tariff")

# =====================================================
# DIM TARIFF
# =====================================================

dim_tariff = (
    bronze_tariff.select("Tariff", "TariffDateTime")
    .filter(col("Tariff").isNotNull())
    .dropDuplicates(["Tariff"])
    .withColumn(
        "tariff_id",
        when(col("Tariff") == "Normal", lit("TAR001"))
        .when(col("Tariff") == "Low", lit("TAR002"))
        .when(col("Tariff") == "High", lit("TAR003"))
        .otherwise(lit("TAR000")),
    )
    .withColumn("tariff_name", col("Tariff"))
    .withColumn(
        "tariff_description",
        when(col("Tariff") == "Normal", lit("Standard rate tariff"))
        .when(col("Tariff") == "Low", lit("Off-peak low rate tariff"))
        .when(col("Tariff") == "High", lit("Peak high rate tariff"))
        .otherwise(lit("Unknown tariff type")),
    )
    .withColumn(
        "rate_per_kwh",
        when(col("Tariff") == "Normal", lit(0.15))
        .when(col("Tariff") == "Low", lit(0.08))
        .when(col("Tariff") == "High", lit(0.25))
        .otherwise(lit(0.0)),
    )
    .withColumn("status", lit("ACTIVE"))
    .withColumn("region", lit("LONDON"))
    .withColumn("load_ts", current_timestamp())
    .withColumn("tariff_half_hour", to_timestamp(col("TariffDateTime")))
    .dropDuplicates(["tariff_half_hour"])
)

# =====================================================
# FINAL COLUMNS
# =====================================================

dim_tariff = dim_tariff.withColumn(
    "tariff_key",
    when(col("tariff_id") == "TAR001", lit(1))
    .when(col("tariff_id") == "TAR002", lit(2))
    .when(col("tariff_id") == "TAR003", lit(3))
    .otherwise(lit(0)),
).select(
    "tariff_key",
    "tariff_id",
    "tariff_name",
    "tariff_description",
    "rate_per_kwh",
    "status",
    "region",
    "load_ts",
    "tariff_half_hour",
)

# =====================================================
# DATA QUALITY
# =====================================================

dim_tariff = validate_data_quality(
    spark,
    dim_tariff,
    TARGET_TABLE,
    [
        {"name": "tariff_key_not_null", "check_type": "not_null", "column": "tariff_key"},
        {"name": "tariff_id_not_null", "check_type": "not_null", "column": "tariff_id"},
        {"name": "tariff_name_not_null", "check_type": "not_null", "column": "tariff_name"},
        {"name": "tariff_key_unique", "check_type": "unique", "column": "tariff_key"},
        {"name": "tariff_id_unique", "check_type": "unique", "column": "tariff_id"},
    ],
)

dim_tariff = add_schema_drift_metadata(spark, dim_tariff, TARGET_TABLE)

# =====================================================
# INITIAL LOAD
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (dim_tariff.write.format("delta").option("mergeSchema", "true").saveAsTable(TARGET_TABLE))

    print("Initial dim_tariff load complete")

# =====================================================
# INCREMENTAL MERGE
# =====================================================

else:

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("t")
        .merge(dim_tariff.alias("s"), "t.tariff_id = s.tariff_id")
        .withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("dim_tariff merged successfully")

print(f"dim_tariff rows: {dim_tariff.count()}")
