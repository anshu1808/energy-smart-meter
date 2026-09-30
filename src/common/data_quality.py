from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

from common.config import get_catalog


def _log_dq_failures(spark, table_name, failure_details):
    """Write DQ failure records to the DQ failure tracking table."""
    dq_failure_table = f"{get_catalog()}.silver.dq_failures"
    schema = StructType([
        StructField("dq_table", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("failure_reason", StringType(), False),
        StructField("failed_value", LongType(), False),
    ])

    records = [
        (
            table_name,
            d["check_name"],
            d["check_type"],
            d["column_name"],
            d["failure_reason"],
            d["failed_value"],
        )
        for d in failure_details
    ]

    failure_df = spark.createDataFrame(records, schema)
    failure_df = failure_df.withColumn("check_ts", current_timestamp())

    if not spark.catalog.tableExists(dq_failure_table):
        (
            failure_df.write
            .format("delta")
            .option("mergeSchema", "true")
            .saveAsTable(dq_failure_table)
        )
    else:
        (
            failure_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(dq_failure_table)
        )


def validate_data_quality(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    checks: list,
    fail_on_error: bool = False,
) -> DataFrame:
    """Run data quality checks on a DataFrame and add metadata columns.

    When any check fails, the failure details (table name, check name, check
    type, column name, failure reason, failed value count, and timestamp)
    are written to the catalog's silver.dq_failures table.

    Args:
        spark: SparkSession
        dataframe: DataFrame to validate
        table_name: Name of the target table (for logging)
        checks: List of dicts with keys:
            - name: str (check name)
            - check_type: str ("not_null", "unique", "range", "not_empty")
            - column: str (column to check; for "unique" can be a single column)
            - columns: list (for multi-column "unique" check)
            - min_val: numeric (optional, for "range" check)
            - max_val: numeric (optional, for "range" check)
        fail_on_error: If True, raise RuntimeError when any check fails.

    Returns:
        DataFrame with dq_passed and dq_failed_checks columns added.
    """
    total_rows = dataframe.count()
    failed_checks = []
    failure_details = []

    for check in checks:
        check_name = check["name"]
        check_type = check["check_type"]

        if check_type == "not_null":
            column = check["column"]
            null_count = dataframe.filter(col(column).isNull()).count()
            if null_count > 0:
                reason = f"{null_count} nulls in {column}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": column,
                    "failure_reason": reason,
                    "failed_value": null_count,
                })

        elif check_type == "unique":
            columns = check.get("columns") or [check["column"]]
            dup_count = total_rows - dataframe.dropDuplicates(columns).count()
            if dup_count > 0:
                reason = f"{dup_count} duplicates on {columns}"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": ", ".join(str(c) for c in columns),
                    "failure_reason": reason,
                    "failed_value": dup_count,
                })

        elif check_type == "range":
            column = check["column"]
            min_val = check.get("min_val")
            max_val = check.get("max_val")
            conditions = []
            if min_val is not None:
                conditions.append(col(column) < min_val)
            if max_val is not None:
                conditions.append(col(column) > max_val)
            if conditions:
                range_filter = conditions[0]
                for condition in conditions[1:]:
                    range_filter = range_filter | condition
                range_filter = range_filter & col(column).isNotNull()
                out_of_range = dataframe.filter(range_filter).count()
                if out_of_range > 0:
                    reason = f"{out_of_range} out-of-range values in {column}"
                    failed_checks.append(f"{check_name}: {reason}")
                    failure_details.append({
                        "check_name": check_name,
                        "check_type": check_type,
                        "column_name": column,
                        "failure_reason": reason,
                        "failed_value": out_of_range,
                    })

        elif check_type == "not_empty":
            if total_rows == 0:
                reason = "0 rows in dataframe"
                failed_checks.append(f"{check_name}: {reason}")
                failure_details.append({
                    "check_name": check_name,
                    "check_type": check_type,
                    "column_name": "",
                    "failure_reason": reason,
                    "failed_value": 0,
                })

    if failure_details:
        _log_dq_failures(spark, table_name, failure_details)

    dq_summary = "; ".join(failed_checks)
    status = "PASSED" if not failed_checks else "FAILED"
    print(f"[DQ] {table_name}: {status}")
    if failed_checks:
        print(f"[DQ] {table_name}: {dq_summary}")

    if fail_on_error and failed_checks:
        raise RuntimeError(
            f"Data quality checks failed for {table_name}: {dq_summary}"
        )

    return (
        dataframe
        .withColumn("dq_passed", lit(len(failed_checks) == 0))
        .withColumn("dq_failed_checks", lit(dq_summary))
    )