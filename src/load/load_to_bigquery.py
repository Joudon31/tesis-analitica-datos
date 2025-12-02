import os
import json
import csv
import pandas as pd
from google.cloud import bigquery, storage
from src.config_loader import load_config
from datetime import datetime
import re

config = load_config()
PROJECT_ID = config["gcp"].get("project_id")
DATASET_ID = (
    config["gcp"].get("dataset_id")
    or config["gcp"].get("dataset_analytics")
    or config["gcp"].get("dataset")
    or "warehouse_tesis"
)

PROCESSED_DIR = "data/processed"


# ---------------------------------------------------------
# NORMALIZAR NOMBRES DE COLUMNAS PARA BIGQUERY
# ---------------------------------------------------------
def clean_bq_column(name: str) -> str:
    name = name.replace("\ufeff", "")        # eliminar BOM
    name = name.strip()
    name = name.lower()
    name = name.replace(";", "_")            # Fix CSV mal separados
    name = name.replace(" ", "_")
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    name = re.sub(r"_+", "_", name)
    return name


# ---------------------------------------------------------
# LIMPIAR Y REPARAR CSV ANTES DE CARGAR
# ---------------------------------------------------------
def fix_csv_file(path: str) -> str:
    """
    - Detecta si el CSV usa ';' como separador
    - Limpia BOM
    - Normaliza nombres de columnas
    - Reescribe el archivo en formato CSV limpio
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(2048)

    separator = ";" if sample.count(";") > sample.count(",") else ","

    df = pd.read_csv(path, sep=separator, encoding="utf-8", engine="python")

    df.columns = [clean_bq_column(c) for c in df.columns]

    cleaned_path = path.replace(".csv", "_cleanload.csv")
    df.to_csv(cleaned_path, index=False, encoding="utf-8")

    return cleaned_path


# ---------------------------------------------------------
# LIMPIAR JSON / NDJSON ANTES DE CARGAR
# ---------------------------------------------------------
def fix_ndjson_file(path: str) -> str:
    """
    Limpia claves JSON inválidas como:
    "geometry.coordinates" → "geometry_coordinates"
    """
    cleaned_path = path.replace(".ndjson", "_cleanload.ndjson")

    with open(path, "r", encoding="utf-8", errors="ignore") as fr, \
         open(cleaned_path, "w", encoding="utf-8") as fw:
        for line in fr:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except:
                continue

            cleaned = {}

            for k, v in obj.items():
                new_k = clean_bq_column(k)
                cleaned[new_k] = v

            fw.write(json.dumps(cleaned, ensure_ascii=False) + "\n")

    return cleaned_path


# ---------------------------------------------------------
# CREAR DATASET SI NO EXISTE
# ---------------------------------------------------------
def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[✓] Dataset existente: {DATASET_ID}")
    except Exception:
        print(f"[CREATE] Creando dataset: {DATASET_ID}")
        client.create_dataset(dataset_ref)
        print(f"[OK] Dataset creado")


# ---------------------------------------------------------
# CARGA A BIGQUERY
# ---------------------------------------------------------
def load_csv_to_bq(client, table_id, file_path):
    print(f"[LOAD] CSV -> {table_id} ({file_path})")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="UTF-8",
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
        encoding="UTF-8",
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)

    job.result()
    print(f"[OK] NDJSON cargado -> {table_id}")


# ---------------------------------------------------------
# DESCARGAR ARCHIVOS DESDE BUCKET SI NO EXISTEN
# ---------------------------------------------------------
def download_processed_from_bucket_if_empty():
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


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    print("\n=========== INICIANDO LOAD → BIGQUERY ===========\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    download_processed_from_bucket_if_empty()
    files = sorted([f for f in os.listdir(PROCESSED_DIR) if not f.startswith(".")])

    print("[INFO] Archivos detectados:", files)

    for filename in files:
        path = os.path.join(PROCESSED_DIR, filename)

        # Normalizar nombre base → tabla
        base = filename
        base = re.sub(r"_[0-9]{8,}", "", base)
        base = re.sub(r"\..+$", "", base)
        table_name = clean_bq_column(base)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print("\n------------------------------")
        print(f"Archivo: {filename}")
        print(f"Tabla destino: {table_id}")
        print("------------------------------")

        try:
            # ----------------------------------
            # CSV
            # ----------------------------------
            if filename.endswith(".csv"):
                fixed_path = fix_csv_file(path)
                load_csv_to_bq(client, table_id, fixed_path)

            # ----------------------------------
            # NDJSON
            # ----------------------------------
            elif filename.endswith(".ndjson"):
                fixed_path = fix_ndjson_file(path)
                load_ndjson_to_bq(client, table_id, fixed_path)

            # ----------------------------------
            # JSON → convertir si es array
            # ----------------------------------
            elif filename.endswith(".json"):
                with open(path, "r", encoding="utf-8", errors="ignore") as fr:
                    txt = fr.read().strip()

                # Es un array?
                if txt.startswith("["):
                    arr = json.loads(txt)
                    temp = path + ".ndtmp"
                    with open(temp, "w", encoding="utf-8") as fw:
                        for obj in arr:
                            clean_obj = {clean_bq_column(k): v for k, v in obj.items()}
                            fw.write(json.dumps(clean_obj, ensure_ascii=False) + "\n")
                    load_ndjson_to_bq(client, table_id, temp)
                    os.remove(temp)

                else:
                    # JSON normal → NDJSON
                    obj = json.loads(txt)
                    temp = path + ".ndtmp"
                    with open(temp, "w", encoding="utf-8") as fw:
                        fw.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    fixed_path = fix_ndjson_file(temp)
                    load_ndjson_to_bq(client, table_id, fixed_path)
                    os.remove(temp)

            else:
                print(f"[SKIP] Formato no soportado: {filename}")

        except Exception as e:
            print(f"[ERROR] Falló la carga de {filename} -> {e}")

    print("\n=========== LOAD COMPLETO ===========\n")


if __name__ == "__main__":
    main()
