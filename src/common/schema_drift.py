from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


def add_schema_drift_metadata(
    spark: SparkSession,
    dataframe: DataFrame,
    target_table: str,
) -> DataFrame:
    """Annotate a DataFrame with newly added or changed source columns."""
    if spark.catalog.tableExists(target_table):
        target_types = {
            field.name: field.dataType.simpleString()
            for field in spark.table(target_table).schema
        }
    else:
        target_types = {}

    drift_columns = []
    for field in dataframe.schema:
        source_type = field.dataType.simpleString()
        target_type = target_types.get(field.name)
        if target_type is None:
            if target_types:
                drift_columns.append(f"{field.name} (new column)")
        elif target_type != source_type:
            drift_columns.append(
                f"{field.name} (type changed: {target_type} -> {source_type})"
            )

    drift_summary = ", ".join(drift_columns)
    return (
        dataframe
        .withColumn("schema_drift_detected", lit(bool(drift_columns)))
        .withColumn("schema_drift_columns", lit(drift_summary))
    )