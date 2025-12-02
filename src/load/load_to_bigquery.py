import os
import json
import pandas as pd
from google.cloud import storage, bigquery
from src.config_loader import load_config

config = load_config()

PROJECT_ID = config["gcp"]["project_id"]
DATASET_ID = config["gcp"]["dataset_id"]

PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)


def ensure_dataset(client):
    ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(ref)
    except:
        client.create_dataset(ref)
        print(f"[CREATE] Dataset creado: {DATASET_ID}")


def load_csv(client, table, path):
    cfg = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table, job_config=cfg)
    job.result()
    print(f"[OK] CSV cargado → {table}")


def load_json(client, table, path):
    cfg = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND"
    )
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table, job_config=cfg)
    job.result()
    print(f"[OK] JSON cargado → {table}")


def download_processed():
    bucket = storage.Client.from_service_account_json(
        config["gcp"]["credentials"]
    ).bucket(config["gcp"]["bucket_processed"])

    for blob in bucket.list_blobs(prefix="processed/"):
        name = blob.name.replace("processed/", "")
        if not name:
            continue
        dest = os.path.join(PROCESSED_DIR, name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        blob.download_to_filename(dest)
        print(f"[OK] Descargado → {dest}")


def main():
    print("INICIANDO LOAD → BIGQUERY")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    download_processed()

    for f in os.listdir(PROCESSED_DIR):
        path = os.path.join(PROCESSED_DIR, f)

        table_name = f.split("_clean")[0].lower()
        table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        if f.endswith(".csv"):
            load_csv(client, table, path)

        elif f.endswith(".json"):
            load_json(client, table, path)

        else:
            print(f"[SKIP] No soportado: {f}")


if __name__ == "__main__":
    main()
