"""
Transform step for AtmosTrack Air Quality ETL.

- Reads raw JSON files from data/raw/
- Flattens hourly pollutant data into tabular format (one row per hour per city)
- Adds derived features:
    - AQI based on PM2.5
    - Pollution severity score
    - Risk classification
    - Hour of day
- Saves transformed data into data/staged/air_quality_transformed.csv
"""

import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# --------------------------
# LOAD ENV
# --------------------------
load_dotenv()

RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
STAGED_DIR = Path(os.getenv("STAGED_DIR", "data/staged"))
STAGED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"

POLLUTANTS = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]

# --------------------------
# UTILITY FUNCTIONS
# --------------------------
def compute_aqi(pm2_5: float) -> str:
    if pd.isna(pm2_5):
        return None
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def compute_severity(row: pd.Series) -> float:
    return (
        (row.get("pm2_5", 0) * 5) +
        (row.get("pm10", 0) * 3) +
        (row.get("nitrogen_dioxide", 0) * 4) +
        (row.get("sulphur_dioxide", 0) * 4) +
        (row.get("carbon_monoxide", 0) * 2) +
        (row.get("ozone", 0) * 3)
    )

def classify_risk(severity: float) -> str:
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

# --------------------------
# TRANSFORM
# --------------------------
all_records = []

for json_file in RAW_DIR.glob("*_raw_*.json"):
    with open(json_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {json_file}")
            continue

    # Extract city name from filename
    city = json_file.stem.split("_raw_")[0].replace("_", " ").title()

    # Open-Meteo raw structure: data["hourly"]["<pollutant>"] + data["hourly"]["time"]
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        continue

    # Build rows
    for i, timestamp in enumerate(times):
        row = {"city": city, "time": pd.to_datetime(timestamp)}
        for pol in POLLUTANTS:
            values = hourly.get(pol, [])
            row[pol] = pd.to_numeric(values[i], errors="coerce") if i < len(values) else None
        all_records.append(row)

# Create DataFrame
df = pd.DataFrame(all_records)

# Remove rows where all pollutant readings are missing
df = df.dropna(subset=POLLUTANTS, how="all")

# Derived features
df["AQI"] = df["pm2_5"].apply(compute_aqi)
df["severity"] = df.apply(compute_severity, axis=1)
df["risk"] = df["severity"].apply(classify_risk)
df["hour"] = df["time"].dt.hour

# Save transformed CSV
df.to_csv(OUTPUT_FILE, index=False)
print(f"Transformed data saved to: {OUTPUT_FILE}")