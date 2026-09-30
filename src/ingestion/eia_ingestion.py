from pyspark.shell import spark

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from pyspark.sql.functions import (
    current_timestamp,
    lit
)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402

catalog = get_catalog()

# =====================================================
# CONFIG
# =====================================================

def get_eia_api_key():
    """Resolve the EIA API key from environment variables or Databricks secrets."""
    for env_var in ("EIA_API_KEY", "DATABRICKS_SECRET_EIA_API_KEY"):
        value = os.getenv(env_var)
        if value and value.strip():
            return value.strip()

    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope="energy-secrets", key="eia-api-key")
        except Exception:
            pass

    raise RuntimeError(
        "EIA API key is not configured. Set EIA_API_KEY or store it in "
        "Databricks secret scope energy-secrets / eia-api-key."
    )


def get_pipeline_start_ts():
    """Return the Databricks job start time when available."""
    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            start_time = context.tags().apply("startTime")
            if start_time.isDefined():
                numeric_value = float(start_time.get())
                if numeric_value > 10_000_000_000:
                    numeric_value /= 1000
                return datetime.fromtimestamp(
                    numeric_value,
                    timezone.utc,
                ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            pass
        except Exception:
            pass

    for config_key in (
        "spark.databricks.job.startTime",
        "spark.databricks.job.startTimeMs",
        "spark.databricks.job.runStartTime",
    ):
        try:
            value = spark.conf.get(config_key)
        except Exception:
            continue

        try:
            numeric_value = float(value)
            if numeric_value > 10_000_000_000:
                numeric_value /= 1000
            return datetime.fromtimestamp(
                numeric_value,
                timezone.utc,
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            continue

    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_last_pipeline_start_ts():
    """Use the last completed pipeline start as the EIA API watermark."""
    audit_table = f"{catalog}.audit.pipeline_lineage"
    if not spark.catalog.tableExists(audit_table):
        return None

    audit_columns = {
        field.name
        for field in spark.table(audit_table).schema.fields
    }
    timestamp_column = (
        "pipeline_start_ts"
        if "pipeline_start_ts" in audit_columns
        else "run_ts"
        if "run_ts" in audit_columns
        else None
    )
    if timestamp_column is None:
        return None

    watermark = (
        spark.table(audit_table)
        .selectExpr(f"max(`{timestamp_column}`) AS pipeline_start_ts")
        .first()["pipeline_start_ts"]
    )
    return watermark

# =====================================================
# CONSTANTS
# =====================================================

RAW_EIA_PATH = "/Volumes/energy/bronze/raw/eia"

API_URL = (
    "https://api.eia.gov/v2/electricity/"
    "rto/region-data/data/"
)

# =====================================================
# INGESTION
# =====================================================

def fetch_eia_data():

    api_key = get_eia_api_key()
    pipeline_start_ts = get_pipeline_start_ts()
    last_pipeline_start_ts = get_last_pipeline_start_ts()

    start_query = ""
    if last_pipeline_start_ts is not None:
        start_query = (
            f"&start={last_pipeline_start_ts.strftime('%Y-%m-%dT%H')}"
        )

    url = (
        API_URL
        + "?frequency=hourly"
        + "&data[0]=value"
        + "&sort[0][column]=period"
        + "&sort[0][direction]=asc"
        + "&offset=0"
        + "&length=5000"
        + start_query
        + f"&api_key={api_key}"
    )

    response = requests.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    records = payload["response"]["data"]

    if not records:
        print(
            f"No new EIA records after watermark="
            f"{last_pipeline_start_ts or 'bootstrap'}"
        )
        return

    load_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    eia_df = (
        spark.createDataFrame(records)
        .withColumn(
            "_source_system",
            lit("EIA")
        )
        .withColumn(
            "_load_id",
            lit(load_id)
        )
        .withColumn(
            "_ingestion_ts",
            current_timestamp()
        )
    )

    eia_df.show(5, truncate=False)

    (
        eia_df.write
        .mode("append")
        .parquet(
            f"{RAW_EIA_PATH}/eia_{load_id}"
        )
    )

    print(
        f"EIA records ingested: {eia_df.count()}, "
        f"watermark={last_pipeline_start_ts or 'bootstrap'}, "
        f"pipeline_start_ts={pipeline_start_ts}"
    )

    print(
        f"Load ID: {load_id}"
    )

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    fetch_eia_data()