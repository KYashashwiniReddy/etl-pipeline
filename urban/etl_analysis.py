"""
etl_analysis.py

- Reads loaded data from Supabase table `air_quality_data`
- Computes KPIs:
    * City with highest average PM2.5
    * City with highest average severity_score
    * Percentage distribution of risk_flag (High/Moderate/Low)
    * Hour of day with worst AQI (highest avg pm2_5)
- City pollution trend report (time -> pm2_5, pm10, ozone)
- Saves CSVs to urban/data/processed/
- Saves PNG visualizations:
    - pm2_5_histogram.png
    - risk_flags_bar.png
    - pm2_5_trends.png
    - severity_vs_pm2_5.png
"""

import os
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "urban/data/processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch all rows from Supabase
resp = supabase.table("air_quality_data").select("*").execute()
data = resp.data

if not data:
    print("No data fetched from Supabase table 'air_quality_data'. Exiting.")
    exit(0)

df = pd.DataFrame(data)

# Ensure correct dtypes
df["time"] = pd.to_datetime(df["time"], errors="coerce")
df = df.dropna(subset=["time"])  # drop rows with invalid times

numeric_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
                "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# A. KPI Metrics
city_pm25 = df.groupby("city")["pm2_5"].mean().dropna()
city_highest_pm2_5 = city_pm25.idxmax() if not city_pm25.empty else None

city_severity = df.groupby("city")["severity_score"].mean().dropna()
city_highest_severity = city_severity.idxmax() if not city_severity.empty else None

risk_counts = df["risk_flag"].value_counts(dropna=True)
risk_pct = (risk_counts / risk_counts.sum() * 100).round(2)

df["hour_of_day"] = df["time"].dt.hour
hourly_pm25 = df.groupby("hour_of_day")["pm2_5"].mean().dropna()
worst_hour_aqi = int(hourly_pm25.idxmax()) if not hourly_pm25.empty else None

# Save summary metrics CSV
summary = {
    "city_highest_pm2_5": [city_highest_pm2_5],
    "city_highest_severity": [city_highest_severity],
    "worst_hour_aqi": [worst_hour_aqi]
}
summary_df = pd.DataFrame(summary)
for k, v in risk_pct.items():
    summary_df[f"risk_pct_{k}"] = v
summary_df.to_csv(os.path.join(PROCESSED_DIR, "summary_metrics.csv"), index=False)

# B. City Pollution Trend Report
trend_cols = ["time", "pm2_5", "pm10", "ozone"]
trend_df = df[["city"] + trend_cols].sort_values(["city", "time"])
trend_df.to_csv(os.path.join(PROCESSED_DIR, "pollution_trends.csv"), index=False)

# C. City risk distribution
risk_dist = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
risk_dist.to_csv(os.path.join(PROCESSED_DIR, "city_risk_distribution.csv"))

# D. Visualizations
plt.rcParams.update({'figure.max_open_warning': 0})

# 1) Histogram of PM2.5
plt.figure(figsize=(8,5))
pm25_vals = df["pm2_5"].dropna()
plt.hist(pm25_vals, bins=30)
plt.title("Histogram of PM2.5")
plt.xlabel("PM2.5")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "pm2_5_histogram.png"))
plt.close()

# 2) Bar chart of risk flags per city (stacked)
risk_dist_plot = risk_dist.copy()
risk_dist_plot.plot(kind="bar", stacked=True, figsize=(10,6))
plt.title("Risk Flags per City")
plt.xlabel("City")
plt.ylabel("Number of Hours")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "risk_flags_bar.png"))
plt.close()

# 3) Line chart of hourly PM2.5 trends (per city)
plt.figure(figsize=(12,6))
for city in df["city"].unique():
    city_df = df[df["city"] == city].set_index("time").resample("h")["pm2_5"].mean()  # fixed warning
    if city_df.dropna().empty:
        continue
    plt.plot(city_df.index, city_df.values, label=city)
plt.title("Hourly PM2.5 Trends by City")
plt.xlabel("Time")
plt.ylabel("PM2.5")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "pm2_5_trends.png"))
plt.close()

# 4) Scatter: severity_score vs pm2_5
plt.figure(figsize=(8,5))
scatter_df = df[["pm2_5", "severity_score", "city"]].dropna(subset=["pm2_5", "severity_score"])
plt.scatter(scatter_df["pm2_5"], scatter_df["severity_score"], s=10)
plt.title("Severity Score vs PM2.5")
plt.xlabel("PM2.5")
plt.ylabel("Severity Score")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "severity_vs_pm2_5.png"))
plt.close()

print("Analysis complete. Outputs saved to:", PROCESSED_DIR)
print("Summary metrics:")
print(summary_df.to_dict(orient="records")[0])
print("Risk distribution (%):")
print(risk_pct.to_dict())
