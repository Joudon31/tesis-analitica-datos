import os
import json
import pandas as pd
from google.cloud import bigquery, storage
from src.config_loader import load_config
from datetime import datetime

config = load_config()
PROJECT_ID = config["gcp"].get("project_id")
# dataset_id key fallback: try several possible names in config
DATASET_ID = config["gcp"].get("dataset_id") or config["gcp"].get("dataset_analytics") or config["gcp"].get("dataset") or "warehouse_tesis"

PROCESSED_DIR = "data/processed"

def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[✓] Dataset existente: {DATASET_ID}")
    except Exception:
        print(f"[CREATE] Creando dataset: {DATASET_ID}")
        client.create_dataset(dataset_ref)
        print(f"[OK] Dataset creado")


def load_csv_to_bq(client, table_id, file_path):
    print(f"[LOAD] CSV -> {table_id} ({file_path})")
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="UTF-8"
    )
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)
    job.result()
    print(f"[OK] CSV cargado -> {table_id}")


def load_ndjson_to_bq(client, table_id, file_path):
    print(f"[LOAD] NDJSON -> {table_id} ({file_path})")
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        encoding="UTF-8"
    )
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)
    job.result()
    print(f"[OK] NDJSON cargado -> {table_id}")


def download_processed_from_bucket_if_empty():
    """Si data/processed está vacío, intenta descargar desde bucket_processed."""
    if os.path.exists(PROCESSED_DIR) and os.listdir(PROCESSED_DIR):
        return
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(config["gcp"]["bucket_processed"])
    print(f"[INFO] Descargando archivos desde gs://{bucket.name}/processed/")
    for blob in bucket.list_blobs(prefix="processed/"):
        dest = os.path.join(PROCESSED_DIR, os.path.basename(blob.name))
        blob.download_to_filename(dest)
        print(f"[OK] Descargado → {dest}")


def main():
    print("\n=========== INICIANDO LOAD → BIGQUERY ===========\n")
    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    # ensure processed files present
    download_processed_from_bucket_if_empty()

    files = sorted([f for f in os.listdir(PROCESSED_DIR) if not f.startswith(".")])
    print("[INFO] Archivos detectados:", files)

    for filename in files:
        path = os.path.join(PROCESSED_DIR, filename)
        # Create table name by stripping timestamp parts: keep first two parts of filename by underscores
        # But per option A, we'll map table = filename base before first timestamp-ish part.
        # Simpler: remove trailing timestamps (YYYYMMDD...) patterns
        base = filename
        # remove known suffixes
        for s in ["_cleancsv.csv", "_clean.csv", "_cleanjson.json", "_cleanbin.json", "_expanded.ndjson", "_clean_expanded.ndjson", "_releases_expanded.ndjson", "_items_expanded.ndjson", "_cleanjson.json", "_cleancsv.csv"]:
            if base.endswith(s):
                base = base[: -len(s)]
        # fallback: strip extension
        if "." in base:
            base = base.rsplit(".", 1)[0]
        # Further sanitize: remove trailing timestamps like _20251202034822
        import re
        base = re.sub(r"_[0-9]{8,}", "", base)  # remove _YYYY... parts
        table_name = base.lower().replace("-", "_").replace(".", "_")
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        print(f"\n------------------------------")
        print(f"Archivo: {filename}")
        print(f"Tabla destino: {table_id}")
        print(f"------------------------------")

        try:
            if filename.endswith(".csv") or filename.endswith("_cleancsv.csv"):
                load_csv_to_bq(client, table_id, path)
            elif filename.endswith(".ndjson") or filename.endswith(".json"):
                # For JSON files that are NDJSON use NDJSON loader
                # We'll try to detect if file is NDJSON (one json per line) or a JSON array
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    first = f.readline().strip()
                    second = f.readline().strip()
                # If the file has multiple lines and first line starts with { or [" -> treat as NDJSON
                if first.startswith("{") or first.startswith("[") or second:
                    # But if it's a single-line array "[{...},{...}]" convert to NDJSON temp file
                    content = None
                    with open(path, "r", encoding="utf-8", errors="ignore") as fr:
                        whole = fr.read().strip()
                    if whole.startswith("[") and whole.endswith("]"):
                        # convert into NDJSON temp file
                        try:
                            arr = json.loads(whole)
                            temp = path + ".ndtmp"
                            with open(temp, "w", encoding="utf-8") as fout:
                                for obj in arr:
                                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                            load_ndjson_to_bq(client, table_id, temp)
                            os.remove(temp)
                        except Exception as e:
                            print(f"[ERROR] No se pudo convertir array JSON a NDJSON: {e}")
                    else:
                        # assume already NDJSON
                        load_ndjson_to_bq(client, table_id, path)
                else:
                    # fallback treat as NDJSON
                    load_ndjson_to_bq(client, table_id, path)
            else:
                print(f"[SKIP] Formato no soportado por loader: {filename}")
        except Exception as e:
            print(f"[ERROR] Falló la carga de {filename} -> {e}")

    print("\n=========== LOAD COMPLETO ===========\n")


if __name__ == "__main__":
    main()
