"""
firms_add_county_geoid.py
─────────────────────────────────────────────────────────────────────────────
Reads NASA FIRMS fire-detection CSV data from GCS, performs a spatial join
against the US Census TIGER/Line county boundaries, and writes the enriched
data (with a 'county_geoid' column) back to GCS as Parquet.

Designed to run on Google Cloud Dataproc (PySpark).
─────────────────────────────────────────────────────────────────────────────
"""
import glob
import os
from typing import Optional
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import SparkFiles

# Config
now = datetime.utcnow()
date_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

INPUT_PATH = f"gs://wildfire-raw-firms/{date_str}/*"
OUTPUT_PATH = f"gs://wildfire-processed/firms/{date_str}/"
SHAPEFILE_PATH = "gs://wildfire-processed/shapefiles/tl_2023_us_county/"

def download_shapefile(gcs_folder: str, local_dir: str = "/tmp/county_shp") -> str:
    """Copy all files from a GCS folder to a local directory on the driver."""
    from google.cloud import storage

    os.makedirs(local_dir, exist_ok=True)
    bucket_name, prefix = gcs_folder.replace("gs://", "").split("/", 1)
    prefix = prefix.rstrip("/") + "/"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=prefix))

    for blob in blobs:
        filename = blob.name.split("/")[-1]
        if filename:
            blob.download_to_filename(os.path.join(local_dir, filename))
            print(f"[shapefile] downloaded {filename}")

    return local_dir

# Spatial lookup UDF
_cache = {}
def lookup_geoid(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    if lat is None or lon is None:
        return None

    if "index" not in _cache:
        import fiona
        from shapely.geometry import shape
        from shapely.strtree import STRtree

        shp_files = glob.glob(f"{SparkFiles.getRootDirectory()}/*.shp")
        polygons, geoids = [], []
        with fiona.open(shp_files[0]) as src:
            for feat in src:
                geom = shape(feat["geometry"])
                geoid = feat["properties"].get("GEOID") or feat["properties"].get("geoid")
                if geom and geoid:
                    polygons.append(geom)
                    geoids.append(geoid)

        _cache["polys"] = polygons
        _cache["geoids"] = geoids
        _cache["index"] = STRtree(polygons)

    from shapely.geometry import Point
    pt = Point(lon, lat)  # shapely is (x=lon, y=lat)

    for idx in _cache["index"].query(pt):
        if _cache["polys"][idx].contains(pt):
            return _cache["geoids"][idx]

    # Fallback: nearest county for points on borders / coastlines
    return _cache["geoids"][_cache["index"].nearest(pt)]


geoid_udf = F.udf(lookup_geoid, StringType())

def main():
    spark = (
        SparkSession.builder
        .appName("FIRMS_AddCountyGEOID")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Download shapefile to driver, then ship every file to all executors
    print(f"[main] downloading shapefile from {SHAPEFILE_PATH}")
    local_dir = download_shapefile(SHAPEFILE_PATH)
    for fpath in glob.glob(f"{local_dir}/*"):
        sc.addFile(fpath)

    # Read firms data
    print(f"[main] reading FIRMS data from {INPUT_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )
    df.printSchema()

    # Append country_geoid via spatial join
    print("[main] running spatial join")
    df_enriched = df.withColumn(
        "county_geoid",
        geoid_udf(F.col("latitude").cast("double"),
                  F.col("longitude").cast("double"))
    )

    # Write output
    print(f"[main] writing to {OUTPUT_PATH}")
    writer = df_enriched.write.mode("overwrite")
    writer.parquet(OUTPUT_PATH)


    print("[main] done.")
    spark.stop()

if __name__ == "__main__":
    main()

