
# Energy Smart Meter Analytics Platform

For the complete architecture, data flow, table catalog, operational commands,
audit behavior, and known limitations, see [the high-level project documentation](docs/project_high_level.md).

For a single document containing the complete Python source under `src/`, see
[the Python source compendium](docs/all_python_source.md).

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Python
- GitHub

## Architecture

Sources
→ Bronze
→ Silver
→ Gold

## Sources

- Open Meteo API
- EIA API
- Smart Meter Data
- Customer Master
- Meter Master
- Tariff Master

## Bronze Tables

- bronze_customer
- bronze_meter
- bronze_tariff
- bronze_billing
- bronze_weather
- bronze_eia_demand
- bronze_meter_readings
