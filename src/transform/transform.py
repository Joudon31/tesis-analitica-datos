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


def normalize_name(name: str) -> str:
    return (
        name.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace(":", "_")
            .replace("__", "_")
    )


def generate_unique_id(row):
    raw = "|".join(map(str, row.values))
    return hashlib.md5(raw.encode()).hexdigest()


def load_json_safe(path):
    """
    Intenta cargar JSON real.
    Si no es JSON válido (lista o dict), lo devuelve como texto.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)

        if isinstance(data, dict) and "features" in data:
            return pd.json_normalize(data["features"]), "json"

        if isinstance(data, list):
            return pd.json_normalize(data), "json"

        return pd.json_normalize(data), "json"

    except Exception:
        # No es JSON → probablemente CSV disfrazado
        return None, "text"


def load_file(path):
    ext = path.split(".")[-1].lower()

    if ext == "csv":
        try:
            return pd.read_csv(path, encoding="latin-1"), "csv"
        except:
            return pd.read_csv(path, sep=";", encoding="latin-1", engine="python"), "csv"

    if ext in ["xlsx", "xls"]:
        return pd.read_excel(path), "excel"

    if ext == "json":
        df, status = load_json_safe(path)
        if status == "json":
            return df, "json"
        else:
            # Tiene extensión JSON pero contenido CSV
            return pd.read_csv(path, engine="python", on_bad_lines="skip"), "csv"

    return None, "unknown"


def upload_to_bucket(local_path, dest_name):
    bucket_name = config["gcp"]["bucket_processed"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)
    bucket.blob(dest_name).upload_from_filename(local_path)
    print(f"[UPLOAD] {local_path} → gs://{bucket_name}/{dest_name}")


def main():
    print("\n========== INICIANDO TRANSFORMACIÓN ==========\n")

    # Descargar RAW
    bucket_name = config["gcp"]["bucket_raw"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs():
        dest = os.path.join(RAW_DIR, blob.name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        blob.download_to_filename(dest)

    for filename in os.listdir(RAW_DIR):

        raw_path = os.path.join(RAW_DIR, filename)
        df, ftype = load_file(raw_path)

        if df is None:
            print(f"[SKIP] No procesable: {filename}")
            continue

        df.columns = [
            normalize_name(c) for c in df.columns
        ]

        df.drop_duplicates(inplace=True)
        df.dropna(how="all", axis=1, inplace=True)

        df["fuente_archivo"] = filename
        df["fecha_proceso_utc"] = datetime.utcnow().isoformat()
        df["id_registro"] = df.apply(generate_unique_id, axis=1)

        # Guardado consistente
        if ftype == "csv":
            clean_name = normalize_name(filename.replace(".", "_clean.")) + ".csv"
        else:
            clean_name = normalize_name(filename.replace(".", "_clean.")) + ".json"

        output_path = os.path.join(PROCESSED_DIR, clean_name)

        if ftype == "csv":
            df.to_csv(output_path, index=False, encoding="utf-8")
        else:
            df.to_json(output_path, orient="records", lines=True, force_ascii=False)

        if MODE == "cloud":
            upload_to_bucket(output_path, f"processed/{clean_name}")

    print("\n========== TRANSFORMACIÓN COMPLETA ==========\n")


if __name__ == "__main__":
    main()
