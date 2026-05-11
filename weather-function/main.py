"""
    This script automates intake of weather forecast data from Open-Meteo API to GCS
    Author: Sean Cowan
    Project: Wildfire Risk Predictor, CPSC-482
"""

import csv
import io
import json
import time
import requests
import functions_framework
from datetime import datetime, timedelta
from google.cloud import storage

BUCKET = "wildfire-raw-weather"
BATCH_SIZE = 100
PAUSE_BETWEEN = 20.0

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "rain_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "sunshine_duration",
]

client = storage.Client()

def load_counties() -> list[dict]:
    bucket = client.bucket(BUCKET)
    blob = bucket.blob("us_counties.json")
    return json.loads(blob.download_as_text())

COUNTIES = load_counties()


def fetch_batch(counties_batch: list[dict]) -> list[dict]:
    """Fetch one day of weather for up to 1,000 counties from Open-Meteo."""
    payload = {
        "latitude":           [round(c["lat"], 4) for c in counties_batch],
        "longitude":          [round(c["lon"], 4) for c in counties_batch],
        "daily":              DAILY_VARS,
        "forecast_days":      1,
        "temperature_unit":   "fahrenheit",
        "wind_speed_unit":    "mph",
        "precipitation_unit": "inch",
        "timezone":           ["America/Los_Angeles"] * len(counties_batch),
    }
    response = requests.post(
        "https://api.open-meteo.com/v1/forecast",
        json=payload,
        timeout=60,
    )
    response.raise_for_status()

    raw = response.json()
    if isinstance(raw, dict):
        raw = [raw]

    records = []
    for county, location_data in zip(counties_batch, raw):
        daily = location_data.get("daily", {})
        records.append({
            "geoid":               county["geoid"],
            "county":              county["name"],
            "state":               county["state"],
            "lat":                 county["lat"],
            "lon":                 county["lon"],
            "date":                (daily.get("time") or [None])[0],
            "temp_max_f":          (daily.get("temperature_2m_max") or [None])[0],
            "temp_min_f":          (daily.get("temperature_2m_min") or [None])[0],
            "rain_sum_in":         (daily.get("rain_sum") or [None])[0],
            "wind_speed_max_mph":  (daily.get("wind_speed_10m_max") or [None])[0],
            "wind_gusts_max_mph":  (daily.get("wind_gusts_10m_max") or [None])[0],
            "sunshine_duration_s": (daily.get("sunshine_duration") or [None])[0],
        })
    return records


def records_to_csv(records: list[dict]) -> str:
    """Serialize a list of record dicts to a CSV string."""
    if not records:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
    return output.getvalue()


@functions_framework.http
def fetch_and_store(request):
    now = datetime.utcnow()
    date_str = request.args.get("date") or (now - timedelta(days=1)).strftime("%Y-%m-%d")

    if not COUNTIES:
        return "COUNTIES list is empty — populate it from the Census Gazetteer before deploying.", 500

    all_records = []
    errors = []

    for batch_start in range(0, len(COUNTIES), BATCH_SIZE):
        batch = COUNTIES[batch_start : batch_start + BATCH_SIZE]
        batch_label = f"counties {batch_start + 1}–{batch_start + len(batch)}"

        try:
            records = fetch_batch(batch)
            all_records.extend(records)
            print(f"Fetched {batch_label}")
        except requests.exceptions.RequestException as e:
            print(f"ERROR fetching {batch_label}: {e}")
            errors.append(f"{batch_label}: {e}")
            continue

        time.sleep(PAUSE_BETWEEN)

    if not all_records:
        return f"All batches failed: {errors}", 500

    # Upload one CSV per day to GCS
    blob_path = f"{date_str}/open-meteo-counties-raw.csv"
    try:
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(records_to_csv(all_records), content_type="text/csv")
        print(f"Uploaded {blob_path} ({len(all_records)} counties)")
    except Exception as e:
        print(f"ERROR uploading to GCS at {blob_path}: {e}")
        return f"Fetch succeeded but GCS upload failed: {e}", 500

    if errors:
        return (
            f"Partial success. Uploaded {len(all_records)} counties to {blob_path}. "
            f"Failed batches: {errors}"
        ), 207

    return f"Success: {blob_path} ({len(all_records)} counties)", 200