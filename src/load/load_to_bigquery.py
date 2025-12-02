import os
import json
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from src.config_loader import load_config

config = load_config()
PROJECT_ID = config["gcp"]["project_id"]
DATASET_ID = config["gcp"]["dataset_id"]

PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

# =============================
# Helpers
# =============================

def normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = name.replace(" ", "_").replace("-", "_").replace("/", "_")
    name = name.replace(":", "_")
    name = name.replace("__", "_")
    return name


# =============================
# LOAD CSV
# =============================
def load_csv_to_bq(client, table_id, file_path):
    print(f"[LOAD] Cargando CSV → {table_id}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="UTF-8"
    )

    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()
    print(f"[OK] CSV cargado correctamente → {table_id}")


# =============================
# LOAD JSON (con NDJSON seguro)
# =============================
def load_json_to_bq(client, table_id, file_path):
    print(f"[LOAD] Cargando JSON → {table_id}")

    temp_ndjson = file_path.replace(".json", "_nd.json")

    try:
        df = pd.read_json(file_path)
        df.to_json(temp_ndjson, orient="records", lines=True, force_ascii=False)
        file_to_upload = temp_ndjson

    except Exception:
        print("[WARN] JSON no estructurado. Validando línea por línea.")
        file_to_upload = temp_ndjson

        valid_lines = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                    valid_lines.append(line)
                except:
                    print(f"[SKIP] Línea inválida: {line[:80]}")
                    continue

        with open(temp_ndjson, "w", encoding="utf-8") as f:
            f.write("\n".join(valid_lines))

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        encoding="UTF-8"
    )

    with open(file_to_upload, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()
    print(f"[OK] JSON cargado correctamente → {table_id}")

    if os.path.exists(temp_ndjson):
        os.remove(temp_ndjson)


# =============================
# Crear dataset si no existe
# =============================
def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[OK] Dataset existente: {DATASET_ID}")
    except:
        print("[CREATE] Creando dataset...")
        client.create_dataset(dataset_ref)
        print("[OK] Dataset creado.")


# =============================
# Descargar archivos procesados desde GCS
# =============================
def download_processed_from_bucket():
    bucket_name = config["gcp"]["bucket_processed"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)

    print(f"[INFO] Descargando archivos desde gs://{bucket_name}/processed/")

    blobs = bucket.list_blobs(prefix="processed/")

    for blob in blobs:
        clean_name = blob.name.replace("processed/", "")
        if not clean_name:
            continue

        dest_path = os.path.join(PROCESSED_DIR, clean_name)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        blob.download_to_filename(dest_path)
        print(f"[OK] {blob.name} → {dest_path}")


# =============================
# MAIN
# =============================
def main():
    print("\n=========== INICIANDO LOAD → BIGQUERY ===========\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])

    ensure_dataset(client)
    download_processed_from_bucket()

    files = os.listdir(PROCESSED_DIR)
    print(f"[INFO] Archivos detectados: {files}")

    for filename in files:
        file_path = os.path.join(PROCESSED_DIR, filename)

        base = filename.replace("_clean", "")
        table_name = normalize_name(base.split(".")[0])

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print("\n--------------------------------------")
        print(f"Procesando archivo: {filename}")
        print(f"Tabla destino: {table_id}")
        print("--------------------------------------")

        if filename.endswith(".csv"):
            load_csv_to_bq(client, table_id, file_path)

        elif filename.endswith(".json"):
            load_json_to_bq(client, table_id, file_path)

        else:
            print(f"[SKIP] Formato ignorado: {filename}")

    print("\n=========== LOAD COMPLETO ===========\n")


if __name__ == "__main__":
    main()
