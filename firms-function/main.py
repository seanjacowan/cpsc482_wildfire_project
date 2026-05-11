"""
    This script automates intake of data from NASA's FIRMS API to GCS
    Author: Sean Cowan
    Project: Wildfire Risk Predictor, CPSC-482
"""

import requests
import functions_framework
from datetime import datetime, timedelta
from google.cloud import storage

MAP_KEY = "4acaeb83a03772d5927808be9a2cf3ef"
BUCKET = "wildfire-raw-firms"
BOUNDS = "-124.8,24.5,-66.9,49.4"

client = storage.Client()

@functions_framework.http
def fetch_and_store(request):
    sources = ["VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT", "VIIRS_SNPP_NRT"]
    now = datetime.utcnow()
    date_str = request.args.get("date") or (now - timedelta(days=1)).strftime("%Y-%m-%d")

    results = []
    errors = []

    for source in sources:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{source}/{BOUNDS}/1/{date_str}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"ERROR fetching {source} for {date_str}: {e}")
            errors.append(f"{source}: {e}")
            continue

        dt = datetime.strptime(date_str, "%Y-%m-%d")
        blob_path = f"{date_str}/{source}-raw.csv"

        try:
            bucket = client.bucket(BUCKET)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(response.text, content_type="text/csv")
            print(f"Uploaded {blob_path}")
            results.append(blob_path)
        except Exception as e:
            print(f"ERROR uploading {source} to GCS at {blob_path}: {e}")
            errors.append(f"{source}: {e}")
            continue

    if errors and not results:
        return f"All sources failed: {errors}", 500
    elif errors:
        return f"Partial success. Uploaded: {results}. Failed: {errors}", 207
    return f"Success: {results}", 200




