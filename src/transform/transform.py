import os
import json
import pandas as pd
from google.cloud import storage
from datetime import datetime

# =============================
# CONFIG
# =============================
BUCKET_RAW = "tesis-raw-datos-joseph"
BUCKET_PROCESSED = "tesis-processed-datos-joseph"
CREDENTIALS = "gcp-key.json"

# =============================
# GCP CLIENTS
# =============================
def gcs_client():
    return storage.Client.from_service_account_json(CREDENTIALS)

client = gcs_client()


# =============================
# DOWNLOAD RAW FROM BUCKET
# =============================
def download_raw_files():
    bucket = client.bucket(BUCKET_RAW)
    blobs = bucket.list_blobs()

    os.makedirs("data/raw", exist_ok=True)

    downloaded = []

    for blob in blobs:
        local_path = f"data/raw/{blob.name}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        blob.download_to_filename(local_path)
        print(f"[RAW] Descargado: {local_path}")
        downloaded.append(local_path)

    return downloaded


# =============================
# UPLOAD PROCESSED TO BUCKET
# =============================
def upload_processed(local_path, dest_name):
    bucket = client.bucket(BUCKET_PROCESSED)
    blob = bucket.blob(f"processed/{dest_name}")
    blob.upload_from_filename(local_path)
    print(f"[UPLOAD] gs://{BUCKET_PROCESSED}/processed/{dest_name}")


# =============================
# DETECTAR DATASET
# =============================
def detect_type(filename):
    if "clima" in filename:
        return "clima"

    if "sismos_usgs" in filename:
        return "usgs"

    if "sismos_ec" in filename:
        return "sismos_ec"

    if "catalogo_electronico" in filename:
        return "sercop"

    if filename.endswith(".csv"):
        return "csv"

    return "json"


# =============================
# TRANSFORMACIONES ESPECIALES
# =============================
def transform_clima(path):
    df = pd.read_json(path)
    row = df.iloc[0]

    times = json.loads(row["hourly.time"])
    temps = json.loads(row["hourly.temperature_2m"])

    expanded = []
    for i in range(len(times)):
        expanded.append({
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "timezone": row["timezone"],
            "time": times[i],
            "temperature_2m": temps[i],
            "fuente_archivo": row["fuente_archivo"],
            "fecha_proceso_utc": row["fecha_proceso_utc"],
            "id_registro": row["id_registro"],
        })

    return pd.DataFrame(expanded)


def transform_usgs(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for row in data:
        coords = json.loads(row["geometry.coordinates"])

        row["longitude"] = coords[0]
        row["latitude"] = coords[1]
        row["depth"] = coords[2]
        del row["geometry.coordinates"]

        rows.append(row)

    return pd.DataFrame(rows)


def transform_sismos_ec(path):
    return pd.read_json(path)


def transform_sercop(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    releases = json.loads(data[0]["releases"])
    return pd.json_normalize(releases)


# =============================
# CSV / JSON SIMPLE
# =============================
def transform_csv(path):
    return pd.read_csv(path)


def transform_json(path):
    return pd.read_json(path)


# =============================
# MAIN
# =============================
def main():
    print("\n===== INICIANDO TRANSFORM =====")

    raw_files = download_raw_files()

    os.makedirs("data/processed", exist_ok=True)

    for path in raw_files:
        filename = os.path.basename(path)
        tipo = detect_type(filename)

        print(f"\nProcesando {filename} ({tipo})")

        if tipo == "clima":
            df = transform_clima(path)
        elif tipo == "usgs":
            df = transform_usgs(path)
        elif tipo == "sismos_ec":
            df = transform_sismos_ec(path)
        elif tipo == "sercop":
            df = transform_sercop(path)
        elif tipo == "csv":
            df = transform_csv(path)
        else:
            df = transform_json(path)

        out_name = filename.replace(".json", "_clean.json").replace(".csv", "_clean.csv")
        out_path = f"data/processed/{out_name}"

        if out_name.endswith(".csv"):
            df.to_csv(out_path, index=False)
        else:
            df.to_json(out_path, orient="records", lines=True, force_ascii=False)

        upload_processed(out_path, out_name)

    print("\n===== TRANSFORM COMPLETO =====")


if __name__ == "__main__":
    main()
