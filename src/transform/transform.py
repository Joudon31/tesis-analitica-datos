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
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# =============================
# HELPERS
# =============================

def normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = name.replace(" ", "_").replace("-", "_").replace("/", "_")
    name = name.replace(":", "_")
    name = name.replace("__", "_")
    return name


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
    raw = "|".join(map(str, row.values))
    return hashlib.md5(raw.encode()).hexdigest()


def upload_to_bucket(local_path, dest_name):
    bucket_name = config["gcp"]["bucket_processed"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)

    dest_blob = bucket.blob(dest_name)
    dest_blob.upload_from_filename(local_path)
    print(f"[UPLOAD] {local_path} → gs://{bucket_name}/{dest_name}")


def load_file(path):
    ext = path.split(".")[-1].lower()

    if ext == "csv":
        try:
            return pd.read_csv(path, encoding="latin-1", low_memory=False)
        except:
            try:
                import csv
                with open(path, "r", encoding="latin-1", errors="ignore") as f:
                    dialect = csv.Sniffer().sniff(f.readline())
                return pd.read_csv(
                    path,
                    encoding="latin-1",
                    sep=dialect.delimiter,
                    engine="python",
                    on_bad_lines="skip"
                )
            except:
                return pd.read_csv(
                    path,
                    encoding="latin-1",
                    sep=";",
                    engine="python",
                    on_bad_lines="skip"
                )

    if ext in ["xlsx", "xls"]:
        return pd.read_excel(path)

    if ext == "json":
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)

        if isinstance(data, dict) and "features" in data:
            return pd.json_normalize(data["features"])
        if isinstance(data, list):
            return pd.json_normalize(data)
        return pd.json_normalize(data)

    print(f"[WARN] Formato no soportado: {path}")
    return None


def download_all_raw_from_bucket():
    bucket_name = config["gcp"]["bucket_raw"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)

    print(f"[INFO] Descargando RAW desde bucket: {bucket_name}")

    for blob in bucket.list_blobs():
        dest_path = os.path.join(RAW_DIR, blob.name)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        blob.download_to_filename(dest_path)
        print(f"[OK] Descargado: {blob.name} → {dest_path}")


# =============================
# MAIN
# =============================
def main():
    print("\n========== INICIANDO TRANSFORMACIÓN ==========\n")

    download_all_raw_from_bucket()

    print("\n[INFO] Archivos en data/raw/:")
    print(os.listdir(RAW_DIR))

    for filename in os.listdir(RAW_DIR):
        raw_path = os.path.join(RAW_DIR, filename)
        print(f"[INFO] Procesando: {filename}")

        df = load_file(raw_path)
        if df is None:
            print(f"[SKIP] No se pudo procesar: {filename}")
            continue

        df = normalize_columns(df)

        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False)
                if isinstance(x, (list, dict))
                else x
            )

        df.drop_duplicates(inplace=True)
        df.dropna(how="all", axis=1, inplace=True)

        df["fuente_archivo"] = filename
        df["fecha_proceso_utc"] = datetime.utcnow().isoformat()
        df["id_registro"] = df.apply(generate_unique_id, axis=1)

        clean_name = normalize_name(filename.replace(".", "_clean."))
        output_path = os.path.join(PROCESSED_DIR, clean_name)

        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"[OK] Procesado y guardado: {output_path}")

        if MODE == "cloud":
            upload_to_bucket(output_path, f"processed/{clean_name}")

    print("\n========== TRANSFORMACIÓN COMPLETA ==========\n")


if __name__ == "__main__":
    main()
