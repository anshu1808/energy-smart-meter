"""Application configuration.

This project must not store live secrets in source control.
Use Databricks Secrets or environment variables instead.
"""

import os


def get_eia_api_key() -> str:
    candidates = (
        "EIA_API_KEY",
        "DATABRICKS_SECRET_EIA_API_KEY",
    )

    for key in candidates:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()

    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            return dbutils.secrets.get(
                scope="energy-secrets",
                key="eia-api-key"
            )
        except Exception:
            pass

    raise RuntimeError(
        "EIA API key not configured."
    )

#EIA_API_KEY = os.getenv("EIA_API_KEY", "")
