from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

AUDIT_TABLE = "energy.audit.pipeline_lineage"
PIPELINE_SCHEMAS = ("bronze", "silver", "gold", "audit")

def get_databricks_context():
    dbutils = globals().get("dbutils")
    if dbutils is None:
        return None

    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return None


def get_context_tag(context, tag_name):
    if context is None:
        return None

    try:
        value = context.tags().apply(tag_name)
        return value.get() if value.isDefined() else None
    except Exception:
        return None


def get_run_id(context):
    try:
        return str(context.currentRunId().get().id())
    except Exception:
        return get_context_tag(context, "jobRunId") or "manual"

def get_pipeline_start_ts(context):
    context_start = get_context_tag(context, "startTime")
    if context_start:
        try:
            numeric_value = float(context_start)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            pass

    for config_key in (
        "spark.databricks.job.startTime",
        "spark.databricks.job.startTimeMs",
        "spark.databricks.job.runStartTime",
    ):
        try:
            value = spark.conf.get(config_key)
            numeric_value = float(value)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (Exception, TypeError, ValueError, OverflowError):
            continue

    return datetime.now(timezone.utc).replace(tzinfo=None)


context = get_databricks_context()
run_id = get_run_id(context)
audit_task_start_ts = datetime.now(timezone.utc).replace(tzinfo=None)
pipeline_start_ts = get_pipeline_start_ts(context)

spark.sql("CREATE SCHEMA IF NOT EXISTS energy.audit")

schema_filter = ", ".join(f"'{schema_name}'" for schema_name in PIPELINE_SCHEMAS)
tables_df = spark.sql(
    f"""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type
    FROM energy.information_schema.tables
    WHERE table_schema IN ({schema_filter})
    """
)

lineage_df = (
    tables_df
    .withColumn("run_id", lit(run_id))
    .withColumn("run_ts", lit(pipeline_start_ts).cast("timestamp"))
    .withColumn("pipeline_start_ts", lit(pipeline_start_ts).cast("timestamp"))
    .withColumn("pipeline_end_ts", current_timestamp())
    .withColumn("audit_task_start_ts", lit(audit_task_start_ts).cast("timestamp"))
    .withColumn("audit_ts", current_timestamp())
    .select(
        "run_id",
        "run_ts",
        "pipeline_start_ts",
        "pipeline_end_ts",
        "audit_task_start_ts",
        "audit_ts",
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
    )
)

(
    lineage_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(AUDIT_TABLE)
)

print(
    f"Pipeline lineage recorded: run_id={run_id}, "
    f"tables={lineage_df.count()}, "
    f"pipeline_start_ts={pipeline_start_ts.isoformat()}Z"
)
