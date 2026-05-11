import functions_framework
from google.cloud import bigquery

client = bigquery.Client()


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def load_gcs_to_bigquery(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    if not name.endswith(".csv") or name.startswith("."):
        print(f"Ignoring non-data file: {name}")
        return "Ignored", 200

    uri = f"gs://{bucket}/{name}"
    table_id = "wildfire-project-cpsc482.wildfire_data.weather"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        skip_leading_rows=1,
    )

    # This part triggers the BigQuery movement
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    print(f"Automated load complete for {name}")