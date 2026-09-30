from pyspark.shell import spark
import sys
import requests
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql.functions import col, current_timestamp, lit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_catalog  # noqa: E402

catalog = get_catalog()

LATITUDE = 52.52
LONGITUDE = 13.41

# =====================================================
# CONFIG
# =====================================================


def get_pipeline_start_ts():
    """Return the Databricks job start time when available."""
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

pipeline_start_ts = get_pipeline_start_ts()
last_pipeline_start_ts = get_last_pipeline_start_ts()

date_query = ""
if last_pipeline_start_ts is not None:
    start_query = last_pipeline_start_ts.strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    date_query = f"&start_date={start_query}&end_date={end_date}"


url = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LATITUDE}"
    f"&longitude={LONGITUDE}"
    "&hourly=temperature_2m,"
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "precipitation"
    f"{date_query}"
)

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

records = []

for i in range(len(data["hourly"]["time"])):
    records.append({
        "timestamp": data["hourly"]["time"][i],
        "temperature": data["hourly"]["temperature_2m"][i],
        "humidity": data["hourly"]["relative_humidity_2m"][i],
        "wind_speed": data["hourly"]["wind_speed_10m"][i],
        "precipitation": data["hourly"]["precipitation"][i]
    })

load_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
weather_df = (
    spark.createDataFrame(records)
    .withColumn("_source_system", lit("OPEN_METEO"))
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_load_id", lit(load_ts))
)

(
    weather_df.write
    .mode("append")
    .parquet(
        f"/Volumes/energy/bronze/raw/weather/weather_{load_ts}"
    )
)

print("Weather raw snapshot written successfully")