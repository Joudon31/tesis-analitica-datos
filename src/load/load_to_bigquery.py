import os
import pandas as pd
from google.cloud import bigquery
from src.config_loader import load_config

# ============================
#  Cargar configuración
# ============================
config = load_config()
PROJECT_ID = config["gcp"]["project_id"]
DATASET_ID = config["gcp"]["dataset_id"]

PROCESSED_DIR = "data/processed"

# ============================
#  Función principal
# ============================
def load_csv_to_bq(client, table_id, file_path):
    print(f"[LOAD] Cargando CSV → {table_id}")

    df = pd.read_csv(file_path)

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


def load_json_to_bq(client, table_id, file_path):
    print(f"[LOAD] Cargando JSON → {table_id}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        encoding="UTF-8"
    )

    # Convertir JSON normal a NDJSON si fuera necesario
    temp_ndjson = file_path.replace(".json", "_nd.json")

    try:
        df = pd.read_json(file_path)
        df.to_json(temp_ndjson, orient="records", lines=True, force_ascii=False)
        file_to_upload = temp_ndjson
    except:
        file_to_upload = file_path  # Ya es NDJSON

    with open(file_to_upload, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()
    print(f"[OK] JSON cargado correctamente → {table_id}")

    if os.path.exists(temp_ndjson):
        os.remove(temp_ndjson)


# ============================
#  Crear dataset si no existe
# ============================
def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[✓] Dataset existente: {DATASET_ID}")
    except Exception:
        print(f"[CREATE] Creando dataset: {DATASET_ID}")
        client.create_dataset(dataset_ref)
        print(f"[OK] Dataset creado")


# ============================
#  Pipeline LOAD
# ============================
def main():
    print("\n=========== INICIANDO LOAD → BIGQUERY ===========\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])

    ensure_dataset(client)

    files = os.listdir(PROCESSED_DIR)
    print(f"[INFO] Archivos detectados en processed/: {files}")

    for filename in files:
        file_path = os.path.join(PROCESSED_DIR, filename)

        # Definir table name sin el "_clean"
        base = filename.replace("_clean", "")
        table_name = base.split(".")[0].lower()

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print(f"\n------------------------------")
        print(f"Procesando archivo: {filename}")
        print(f"Tabla destino: {table_id}")
        print(f"------------------------------")

        if filename.endswith(".csv"):
            load_csv_to_bq(client, table_id, file_path)

        elif filename.endswith(".json"):
            load_json_to_bq(client, table_id, file_path)

        else:
            print(f"[SKIP] Formato no soportado: {filename}")

    print("\n=========== LOAD COMPLETO ===========\n")


if __name__ == "__main__":
    main()
