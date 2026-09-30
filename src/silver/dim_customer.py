
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    concat,
    lpad,
    current_timestamp,
    when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402
from common.data_quality import validate_data_quality  # noqa: E402
from common.schema_drift import add_schema_drift_metadata  # noqa: E402

spark = SparkSession.builder.getOrCreate()
catalog = get_catalog()

TARGET_TABLE = f"{catalog}.silver.dim_customer"

# ==========================================
# SOURCE
# ==========================================

meter_df = spark.table(
    f"{catalog}.bronze.bronze_meter_readings"
)

# ==========================================
# DQ
# ==========================================

meter_df = (
    meter_df
    .filter(col("LCLid").isNotNull())
)

# ==========================================
# DIM CUSTOMER
# ==========================================

window_spec = Window.orderBy("LCLid")

base = (
    meter_df
    .select("LCLid", "stdorToU")
    .dropDuplicates(["LCLid"])
)

if spark.catalog.tableExists(TARGET_TABLE):
    existing = spark.table(TARGET_TABLE).select("LCLid", "customer_key")
    max_key = existing.agg({"customer_key": "max"}).first()[0] or 0
    new_customers = base.join(existing.select("LCLid"), "LCLid", "left_anti")
    existing_customers = base.join(existing, "LCLid", "inner")
else:
    max_key = 0
    new_customers = base
    existing_customers = base.limit(0).withColumn("customer_key", lit(None).cast("long"))

new_customers = new_customers.withColumn(
    "customer_key", row_number().over(window_spec) + lit(max_key)
)

dim_customer = existing_customers.unionByName(new_customers).withColumn(
    "customer_id",
    concat(lit("CUST"), lpad(col("customer_key").cast("string"), 6, "0"))
).withColumn(
    "customer_type", col("stdorToU")          # keep raw Std/ToU — see Fix 2
).withColumn(
    "tariff_id",
    when(col("customer_type") == "Std", lit("TAR001"))
    .otherwise(lit("TAR002"))  # ToU customers get Low tariff
).withColumn("status", lit("ACTIVE")) \
 .withColumn("region", lit("LONDON")) \
 .withColumn("load_ts", current_timestamp())

dim_customer.show()

dim_customer = validate_data_quality(
    spark,
    dim_customer,
    TARGET_TABLE,
    [
        {"name": "customer_key_not_null", "check_type": "not_null", "column": "customer_key"},
        {"name": "customer_id_not_null", "check_type": "not_null", "column": "customer_id"},
        {"name": "lclid_not_null", "check_type": "not_null", "column": "LCLid"},
        {"name": "customer_key_unique", "check_type": "unique", "column": "customer_key"},
        {"name": "lclid_unique", "check_type": "unique", "column": "LCLid"},
    ],
)

dim_customer = add_schema_drift_metadata(
    spark, dim_customer, TARGET_TABLE
)

dim_customer = dim_customer.select(
    "customer_key",
    "customer_id",
    "LCLid",
    "customer_type",
    "tariff_id",
    "status",
    "region",
    "load_ts",
    "dq_passed",
    "dq_failed_checks",
    "schema_drift_detected",
    "schema_drift_columns"
)

# ==========================================
# INITIAL LOAD
# ==========================================

if not spark.catalog.tableExists(TARGET_TABLE):

    (
        dim_customer.write
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print("Initial dim_customer load complete")

# ==========================================
# INCREMENTAL MERGE
# ==========================================

else:

    target = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        target.alias("t")
        .merge(
            dim_customer.alias("s"),
            "t.LCLid = s.LCLid"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("dim_customer merged successfully")

print(
    f"dim_customer rows: {dim_customer.count()}"
)
'''

import dlt

from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    concat,
    lpad,
    current_timestamp,
    when
)
from pyspark.sql.window import Window


# =====================================================
# SOURCE SNAPSHOT TABLE
# =====================================================

@dlt.table(
    name="dim_customer_source",
    comment="Customer snapshot derived from bronze_meter_readings."
)
@dlt.expect_or_fail(
    "lclid_not_null",
    "LCLid IS NOT NULL"
)
def dim_customer_source():

    window_spec = Window.orderBy("LCLid")

    return (
        spark.read.table(
            f"{catalog}.bronze.bronze_meter_readings"
        )
        .filter(
            col("LCLid").isNotNull()
        )
        .select(
            "LCLid",
            "stdorToU"
        )
        .dropDuplicates(
            ["LCLid"]
        )
        .withColumn(
            "customer_key",
            row_number().over(window_spec)
        )
        .withColumn(
            "customer_id",
            concat(
                lit("CUST"),
                lpad(
                    col("customer_key").cast("string"),
                    6,
                    "0"
                )
            )
        )
        .withColumn(
            "tariff_id",
            when(
                col("stdorToU").isNull(),
                lit("TAR001")
            )
            .when(
                col("stdorToU") == "Std",
                lit("TAR001")
            )
            .otherwise(
                lit("TAR002")
            )
        )
        .withColumn(
            "status",
            lit("ACTIVE")
        )
        .withColumn(
            "region",
            lit("LONDON")
        )
        .withColumn(
            "load_ts",
            current_timestamp()
        )
    )


# =====================================================
# SILVER TARGET TABLE
# =====================================================

dlt.create_streaming_table(
    name="dim_customer",
    comment="Customer Dimension SCD Type 2"
)

# =====================================================
# AUTO CDC
# =====================================================

dlt.create_auto_cdc_from_snapshot_flow(
    target="dim_customer",
    source="dim_customer_source",
    keys=["LCLid"],
    stored_as_scd_type=2
)
'''