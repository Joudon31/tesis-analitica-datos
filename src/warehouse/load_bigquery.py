import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from src.config_loader import load_config

# ======================================================
# CARGA DE CONFIG
# ======================================================
config = load_config()
MODE = config["mode"]

PROCESSED_DIR = "data/processed"

PROJECT_ID = config["gcp"]["project_id"]
DATASET_ID = config["gcp"]["dataset_analytics"]
CREDENTIALS_PATH = config["gcp"]["credentials"]


# ======================================================
# CREAR CLIENTE BIGQUERY
# ======================================================
def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


# ======================================================
# NORMALIZAR NOMBRE DE TABLA
# ======================================================
def normalize_table_name(filename):
    name = filename.lower()
    name = name.replace(".csv", "").replace(".json", "")
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    return name


# ======================================================
# SUBIR ARCHIVO A BIGQUERY
# ======================================================
def load_to_bigquery(filepath, table_name):
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    print(f"[INFO] Cargando a BigQuery: {table_id}")

    # autodetección de esquema
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=(
            bigquery.SourceFormat.CSV if filepath.endswith(".csv")
            else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ),
        write_disposition="WRITE_TRUNCATE",  # siempre reemplaza tabla (ideal para pipeline)
    )

    with open(filepath, "rb") as f:
        load_job = client.load_table_from_file(
            f, table_id, job_config=job_config
        )

    load_job.result()  # Esperar a que termine

    print(f"[OK] Tabla creada/cargada: {table_id}")
    table = client.get_table(table_id)
    print(f"[OK] Filas cargadas: {table.num_rows}")


# ======================================================
# MAIN
# ======================================================
def main():
    print("\n========== INICIANDO CARGA A BIGQUERY (ANALYTICS) ==========\n")

    if not os.path.exists(PROCESSED_DIR):
        print("[ERROR] No existe data/processed. Ejecuta TRANSFORM primero.")
        exit(1)

    files = os.listdir(PROCESSED_DIR)

    if not files:
        print("[WARN] No hay archivos procesados para cargar.")
        return

    print(f"[INFO] Archivos detectados en {PROCESSED_DIR}:")
    print(files)

    for filename in files:
        filepath = os.path.join(PROCESSED_DIR, filename)

        if not os.path.isfile(filepath):
            continue

        if not (filename.endswith(".csv") or filename.endswith(".json")):
            print(f"[SKIP] Formato no soportado para BigQuery: {filename}")
            continue

        table_name = normalize_table_name(filename)
        load_to_bigquery(filepath, table_name)

    print("\n========== CARGA A BIGQUERY COMPLETA ==========\n")


if __name__ == "__main__":
    main()
