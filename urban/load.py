"""
Load step for AtmosTrack Air Quality ETL.

- Reads transformed CSV: data/staged/air_quality_transformed.csv
- Inserts records into Supabase table: air_quality_data
- Batch size = 200
- Converts NaN -> NULL
- Datetime converted to ISO format
- Retry failed batches (2 retries)
- Renames columns to match Supabase table
"""

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import time

# --------------------------
# LOAD ENV
# --------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STAGED_CSV = os.getenv("STAGED_CSV", "data/staged/air_quality_transformed.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 200))
MAX_RETRIES = int(os.getenv("LOAD_MAX_RETRIES", 2))

# --------------------------
# INIT SUPABASE CLIENT
# --------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------
# LOAD DATA
# --------------------------
df = pd.read_csv(STAGED_CSV)

# Convert datetime to ISO string
df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

# Rename columns to match Supabase table
rename_map = {
    "AQI": "aqi_category",
    "severity": "severity_score",
    "risk": "risk_flag"
}
df = df.rename(columns=rename_map)

# Replace NaN with None for Supabase
df = df.where(pd.notnull(df), None)

records = df.to_dict(orient="records")
total_inserted = 0

# --------------------------
# BATCH INSERT
# --------------------------
for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i+BATCH_SIZE]
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            supabase.table("air_quality_data").insert(batch).execute()
            total_inserted += len(batch)
            print(f"Inserted batch {i//BATCH_SIZE + 1} ({len(batch)} rows)")
            break
        except Exception as e:
            attempt += 1
            print(f"⚠️ Batch insert failed (attempt {attempt}): {e}")
            time.sleep(2 ** attempt)
    else:
        print(f"❌ Failed to insert batch {i//BATCH_SIZE + 1} after {MAX_RETRIES} retries")

print(f"✅ Total inserted rows: {total_inserted}")