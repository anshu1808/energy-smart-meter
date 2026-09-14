import pandas as pd
import numpy as np
import glob
import os

# ==========================
# Read Customer Master
# ==========================

customer_df = pd.read_csv(
    "datasets/master/customer_master.csv"
)

meter_df = pd.read_csv(
    "datasets/master/meter_master.csv"
)

customer_meter = (
    customer_df
    .merge(
        meter_df[["meter_id", "LCLid"]],
        on="LCLid",
        how="inner"
    )
)

# ==========================
# Read Meter Files
# ==========================

files = glob.glob("datasets/meter/*.csv")[:2]

meter_data = pd.concat(
    [
        pd.read_csv(
            f,
            usecols=[
                "LCLid",
                "DateTime",
                "KWH/hh (per half hour) "
            ]
        )
        for f in files
    ],
    ignore_index=True
)

# Rename consumption column
meter_data.rename(
    columns={
        "KWH/hh (per half hour) ": "consumption_kwh"
    },
    inplace=True
)

# ==========================
# Data Cleaning
# ==========================

meter_data = meter_data.dropna(
    subset=["LCLid", "consumption_kwh"]
)

meter_data["consumption_kwh"] = pd.to_numeric(
    meter_data["consumption_kwh"],
    errors="coerce"
)

meter_data = meter_data.dropna(
    subset=["consumption_kwh"]
)

# ==========================
# Billing Period
# ==========================

meter_data["DateTime"] = pd.to_datetime(
    meter_data["DateTime"]
)

meter_data["billing_period"] = (
    meter_data["DateTime"]
    .dt.strftime("%Y-%m")
)

# ==========================
# Monthly Consumption
# ==========================

billing_base = (
    meter_data
    .groupby(
        [
            "LCLid",
            "billing_period"
        ],
        as_index=False
    )
    ["consumption_kwh"]
    .sum()
)

# ==========================
# Join Customer/Meter
# ==========================

billing = (
    billing_base
    .merge(
        customer_meter,
        on="LCLid",
        how="left"
    )
)

# ==========================
# Tariff Logic
# ==========================

billing["rate_per_kwh"] = billing[
    "tariff_id"
].map(
    {
        "STD": 0.15,
        "TOU": 0.18
    }
)

billing["rate_per_kwh"] = billing[
    "rate_per_kwh"
].fillna(0.15)

# ==========================
# Consumption
# ==========================

billing["consumption_kwh"] = (
    billing["consumption_kwh"]
    .round(2)
)

# ==========================
# Bill Amount
# ==========================

billing["bill_amount"] = (
    billing["consumption_kwh"]
    * billing["rate_per_kwh"]
).round(2)

# ==========================
# Bill ID
# ==========================

billing["bill_id"] = (
    "BILL"
    + (billing.index + 1)
      .astype(str)
      .str.zfill(8)
)

# ==========================
# Billing Status
# ==========================

billing["billing_status"] = np.random.choice(
    ["PAID", "UNPAID"],
    size=len(billing),
    p=[0.95, 0.05]
)

# ==========================
# Generated Date
# ==========================

billing["generated_date"] = pd.Timestamp.now()

# ==========================
# Final Columns
# ==========================

billing = billing[
    [
        "bill_id",
        "customer_id",
        "meter_id",
        "billing_period",
        "consumption_kwh",
        "rate_per_kwh",
        "bill_amount",
        "billing_status",
        "generated_date"
    ]
]

# ==========================
# Output Folder
# ==========================

os.makedirs(
    "datasets/billing",
    exist_ok=True
)

# ==========================
# Save CSV
# ==========================

billing.to_csv(
    "datasets/billing/billing_data.csv",
    index=False
)

# ==========================
# DQ Checks
# ==========================

print("=" * 50)
print("Billing file created successfully")
print("=" * 50)

print("Rows :", len(billing))
print("Customers :", billing["customer_id"].nunique())
print("Meters :", billing["meter_id"].nunique())

print("\nSample Data:")
print(billing.head())

print("\nNull Check:")
print(billing.isnull().sum())