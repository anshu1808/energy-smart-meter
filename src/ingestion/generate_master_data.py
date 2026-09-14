import pandas as pd
import numpy as np
import os
import glob

# Update path to your smart meter file
INPUT_FILE = glob.glob("datasets/meter/*.csv")

if not INPUT_FILE:
    raise Exception("No meter files found")

# Read sample meter file
try:
    df = pd.read_csv(INPUT_FILE[0], usecols=['LCLid', 'stdorToU'])
except Exception as e:
    raise Exception(f"Error reading meter file: {e}")

df = pd.concat([pd.read_csv(f, usecols=['LCLid', 'stdorToU']) for f in INPUT_FILE])

# Get unique households
customer_master = df.drop_duplicates()

# ------------------------
# CUSTOMER MASTER
# ------------------------

customer_master = customer_master.reset_index(drop=True)

customer_master["customer_id"] = (
    customer_master.index + 1
).map(lambda x: f"CUST{x:06d}")

customer_master["tariff_id"] = (
    customer_master["stdorToU"]
    .fillna("STD")
    .str.upper()
)

customer_master["status"] = "ACTIVE"

customer_master["region"] = "LONDON"

customer_master = customer_master[
    [
        "customer_id",
        "LCLid",
        "tariff_id",
        "status",
        "region"
    ]
]

os.makedirs("datasets/master", exist_ok=True)

customer_master.to_csv(
    "datasets/master/customer_master.csv",
    index=False
)

print(customer_master.head())
print(customer_master.shape)

# ------------------------
# METER MASTER
# ------------------------

customer_df = pd.read_csv(
    "datasets/master/customer_master.csv"
)

meter_df = customer_df[["LCLid"]].copy()

meter_df["meter_id"] = (
    meter_df.index + 1
).map(lambda x: f"MTR{x:06d}")

meter_df["meter_type"] = "SMART"

meter_df["manufacturer"] = "LANDIS_GYR"

meter_df["status"] = "ACTIVE"

meter_df["install_date"] = "2012-01-01"

meter_df["region"] = "LONDON"

meter_df = meter_df[
    [
        "meter_id",
        "LCLid",
        "meter_type",
        "manufacturer",
        "status",
        "install_date",
        "region"
    ]
]

meter_df.to_csv(
    "datasets/master/meter_master.csv",
    index=False
)

print(meter_df.head())
print(meter_df.shape)

# ------------------------
# print FILES
# ------------------------

print("========== DQ REPORT ==========")

print(
    "Customer Count:",
    len(customer_master)
)

print(
    "Unique LCLid:",
    customer_master["LCLid"].nunique()
)

print(
    "Null LCLid:",
    customer_master["LCLid"].isnull().sum()
)