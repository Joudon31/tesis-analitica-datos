import os
import pandas as pd
import json
import hashlib
from datetime import datetime
from google.cloud import storage
from src.config_loader import load_config

config = load_config()
MODE = config["mode"]

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)


# =============================
# HELPERS
# =============================
def normalize_columns(df):
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
                  .str.replace("-", "_")
                  .str.replace("/", "_")
    )
    return df


def generate_unique_id(row):
    """Crear hash único por fila."""
    raw = "|".join(map(str, row.values))
    return hashlib.md5(raw.encode()).hexdigest()


def upload_to_bucket(local_path, dest_name):
    bucket_name = config["gcp"]["bucket_processed"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_name)
    blob.upload_from_filename(local_path)
    print(f"[GCP] Subido a gs://{bucket_name}/{dest_name}")


def load_file(path):
    """Carga archivos dinámicamente según su extensión."""
    ext = path.split(".")[-1].lower()

    if ext == "csv":
        return pd.read_csv(path, encoding="latin-1", low_memory=False)

    if ext in ["xlsx", "xls"]:
        return pd.read_excel(path)

    if ext == "json":
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        return pd.json_normalize(data)

    print(f"[WARN] Formato no soportado: {path}")
    return None


# =============================
# MAIN
# =============================
def main():
    print("\n========== INICIANDO TRANSFORMACIÓN ==========\n")

    for filename in os.listdir(RAW_DIR):
        raw_path = os.path.join(RAW_DIR, filename)
        print(f"[INFO] Procesando: {filename}")

        df = load_file(raw_path)

        if df is None:
            print(f"[SKIP] No se pudo procesar: {filename}")
            continue

        # LIMPIEZA
        df = normalize_columns(df)
        df.drop_duplicates(inplace=True)
        df.dropna(how="all", axis=1, inplace=True)

        # AGREGAR CAMPOS INTERNOS
        df["fuente_archivo"] = filename
        df["fecha_proceso_utc"] = datetime.utcnow().isoformat()

        df["id_registro"] = df.apply(generate_unique_id, axis=1)

        # GUARDAR
        output_file = filename.replace(".", "_clean.")
        output_path = os.path.join(PROCESSED_DIR, output_file)

        df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"[OK] Procesado y guardado: {output_path}")

        # SUBIR A GCP SI MODE = CLOUD
        if MODE == "cloud":
            upload_to_bucket(output_path, f"processed/{output_file}")

    print("\n========== TRANSFORMACIÓN COMPLETA ==========\n")


if __name__ == "__main__":
    main()
