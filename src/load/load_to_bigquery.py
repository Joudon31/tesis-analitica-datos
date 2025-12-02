import os
import pandas as pd
from google.cloud import bigquery, storage

PROJECT_ID = "proyecto-datos-gub-2025"
DATASET_ID = "warehouse_tesis"
BUCKET_NAME = "tesis-processed-datos-joseph"

client_bq = bigquery.Client()
client_gcs = storage.Client()


# ===========================================
# 📥 DESCARGAR ARCHIVOS PROCESADOS DEL BUCKET
# ===========================================
def download_processed_files():
    bucket = client_gcs.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="processed/")

    local_files = []

    os.makedirs("data/processed/", exist_ok=True)

    for blob in blobs:
        filename = blob.name.replace("processed/", "")
        local_path = f"data/processed/{filename}"

        blob.download_to_filename(local_path)
        print(f"[OK] Descargado → {local_path}")
        local_files.append(local_path)

    return local_files


# =======================================
# 📤 CARGAR ARCHIVO A BIGQUERY
# =======================================
def load_file_to_bq(table_id, path):
    job_config = bigquery.LoadJobConfig()

    if path.endswith(".csv"):
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.skip_leading_rows = 1

    elif path.endswith(".parquet"):
        job_config.source_format = bigquery.SourceFormat.PARQUET

    else:
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True

    with open(path, "rb") as f:
        job = client_bq.load_table_from_file(
            f,
            table_id,
            job_config=job_config
        )
    job.result()
    print(f"[LOAD OK] {path} → {table_id}")


# =======================================
# 🚀 PROCESO PRINCIPAL
# =======================================
def main():
    print("\n=========== INICIANDO LOAD → BIGQUERY ===========\n")

    # 1) DESCARGAR ARCHIVOS
    files = download_processed_files()

    # 2) CREAR DATASET SI NO EXISTE
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client_bq.get_dataset(dataset_ref)
        print("[✓] Dataset existente:", DATASET_ID)
    except:
        client_bq.create_dataset(dataset_ref)
        print("[+] Dataset creado:", DATASET_ID)

    # 3) CARGAR UNO POR UNO
    for path in files:
        filename = os.path.basename(path)
        table_name = filename.replace(".", "_").lower()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print("\n-------------------------------")
        print(f"Procesando archivo: {filename}")
        print(f"Tabla destino: {table_id}")
        print("-------------------------------")

        load_file_to_bq(table_id, path)


if __name__ == "__main__":
    main()
