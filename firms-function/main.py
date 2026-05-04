"""
    This script automates intake of data from NASA's FIRMS API to GCS
    Author: Sean Cowan
    Project: Wildfire Risk Predictor, CPSC-482
"""

import os
import requests
import functions_framework
from datetime import datetime, timedelta
from google.cloud import storage

MAP_KEY = os.FIRMS_MAP_KEY
BUCKET = "wildfire-raw-firms"
BOUNDS = "-124.8,24.5,-66.9,49.4"

client = storage.Client()


@functions_framework.http
def fetch_and_store(request):
    """Fetch the previous day of FIRMS data and store it in GCS."""
    now = datetime.utcnow()
    date_str = request.args.get("date") or (now - timedelta(days=1)).strftime("%Y-%m-%d")
    source = request.args.get("source", "VIIRS_SNPP_NRT")

    url = f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/{MAP_KEY}/{source}/{BOUNDS}/{date_str}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.execeptions.RequestException as e:
        print(f"Error fetching FRIMS data for {date_str}: {e}")
        return f"Failed to fetch data: {e}", 500

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    blob_path = f"{source}/{dt.year}/{dt.month:02d}/{dt.day:02d}/detections.csv"

    try:
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(response.text, content_type="text/csv")
    except Exception as e:
        print(f"Error uploading to GCS at {blob_path}: {e}")
        return f"Failed to upload data: {e}", 500

    print(f"Uploaded {blob_path}")
    return f"Success: {blob_path}", 200


