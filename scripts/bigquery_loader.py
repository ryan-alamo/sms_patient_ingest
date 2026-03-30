import json
from pathlib import Path
from google.cloud import bigquery

def upload_to_bigquery(csv_path: Path, table_name: str, schema_dict: list[dict]) -> None:
    client = bigquery.Client(project="candid-v0")
    
    schema: list[bigquery.SchemaField] = []
    for field in schema_dict:
        schema.append(bigquery.SchemaField(
            name=field['name'],
            field_type=field['type'],
            mode=field['mode']
        ))

    dataset_id = 'patient_ingests'
    table_id = f"candid-v0.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,  # Skip header row
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,  # Write if empty
        max_bad_records=0  # 0 allowed errors
    )

    print(f"Uploading {csv_path} to BigQuery table {table_id}")

    with open(csv_path, 'rb') as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config
        )

    try:
        load_job.result()
        print(f"Successfully uploaded {load_job.output_rows} rows to {table_id}")
    except Exception as e:
        print(f"BigQuery upload failed: {e}")
        raise
