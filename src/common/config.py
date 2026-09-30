"""Application configuration.

This project must not store live secrets in source control.
Use Databricks Secrets or environment variables instead.
"""

import os
import sys


def get_catalog() -> str:
    catalog = os.getenv("DATABRICKS_CATALOG")
    if not catalog and len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        catalog = sys.argv[1]

    if not catalog or not catalog.strip():
        raise RuntimeError(
            "Databricks catalog not configured. Pass it as the first task argument " "or set DATABRICKS_CATALOG."
        )

    return catalog.strip()


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
            return dbutils.secrets.get(scope="energy-secrets", key="eia-api-key")
        except Exception:
            pass

    raise RuntimeError("EIA API key not configured.")
