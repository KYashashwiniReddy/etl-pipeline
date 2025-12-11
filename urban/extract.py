"""
Extract step for AtmosTrack Air Quality ETL.

- Fetches hourly pollutant data for major Indian cities from Open-Meteo Air Quality API.
- Implements retry with exponential backoff (default 3 attempts).
- Saves each city response as JSON in data/raw/<city>_raw_<timestamp>.json
- Reads configuration from .env
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

# --------------------------
# LOAD ENV
# --------------------------
load_dotenv()

# --------------------------
# CONFIG
# --------------------------
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = Path(os.getenv("RAW_DIR", BASE_DIR / "data" / "raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(os.getenv("LOG_DIR", BASE_DIR / "logs"))
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "extract.log"),
        logging.StreamHandler()
    ]
)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "10"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "1"))
POLLUTANTS = os.getenv("POLLUTANTS", "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index")

# Supabase (to be used later in load step)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

# --------------------------
# Cities (name, lat, lon)
# Format in .env: AQ_CITIES=Delhi:28.7041:77.1025|Mumbai:19.0760:72.8777
# --------------------------
cities_env = os.getenv("AQ_CITIES", "")
CITIES: List[Dict[str, float]] = []

if cities_env:
    for item in cities_env.split("|"):
        try:
            name, lat, lon = item.split(":")
            CITIES.append({"name": name.strip(), "lat": float(lat), "lon": float(lon)})
        except Exception as e:
            logging.warning(f"Skipping invalid city entry: {item} - {e}")
else:
    # default cities
    CITIES = [
        {"name": "Delhi", "lat": 28.7041, "lon": 77.1025},
        {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
        {"name": "Bengaluru", "lat": 12.9716, "lon": 77.5946},
        {"name": "Hyderabad", "lat": 17.3850, "lon": 78.4867},
        {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639},
    ]


# --------------------------
# UTILITY FUNCTIONS
# --------------------------
def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _save_raw(payload: object, city: str) -> str:
    ts = _now_ts()
    filename = f"{city.replace(' ', '_').lower()}_raw_{ts}.json"
    path = RAW_DIR / filename
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        logging.info(f"✅ Saved data for {city} -> {path}")
    except Exception as e:
        logging.error(f"Failed to save {city} JSON: {e}")
        path = RAW_DIR / f"{city.replace(' ', '_').lower()}_raw_{ts}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(repr(payload))
    return str(path.resolve())


def _fetch_city(city: Dict[str, float]) -> Optional[str]:
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": POLLUTANTS
    }

    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            response = requests.get(API_BASE, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            saved_path = _save_raw(payload, city["name"])
            return saved_path
        except Exception as e:
            wait = 2 ** (attempt - 1)
            logging.warning(f"⚠️ {city['name']} attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {wait}s")
            time.sleep(wait)
    logging.error(f"❌ Failed to fetch data for {city['name']} after {MAX_RETRIES} attempts")
    return None


def fetch_all_cities(cities: List[Dict[str, float]] = CITIES) -> List[str]:
    saved_files = []
    for city in cities:
        logging.info(f"Starting extraction for {city['name']}")
        path = _fetch_city(city)
        if path:
            saved_files.append(path)
        time.sleep(SLEEP_BETWEEN_CALLS)
    return saved_files


# --------------------------
# CLI RUN
# --------------------------
if __name__ == "__main__":
    logging.info("Starting Open-Meteo Air Quality extraction for Indian cities")
    saved_paths = fetch_all_cities()
    logging.info("Extraction complete. Summary:")
    for path in saved_paths:
        print(f" - {path}")