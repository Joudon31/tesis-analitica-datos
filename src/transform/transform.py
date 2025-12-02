import os
import json
import pandas as pd
import numpy as np
from google.cloud import storage
from datetime import datetime

BUCKET_NAME = "tesis-processed-datos-joseph"

# =============================
# 🔧 GCP CLIENTE
# =============================
def get_gcs_client():
    return storage.Client()

client = get_gcs_client()

# =============================
# 🔧 SUBIR ARCHIVO A GCP
# =============================
def upload_to_gcs(local_path, dest_blob):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"processed/{dest_blob}")
    blob.upload_from_filename(local_path)
    print(f"[UPLOAD] {dest_blob}")


# =============================
# 🔎 DETECTAR TIPO DE DATASET
# =============================
def detect_dataset_type(filename):
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

    if filename.endswith(".parquet"):
        return "parquet"

    return "json"


# =====================================================
# 🌦 1) PROCESAR API CLIMA (EXPANDIR hourly)
# =====================================================
def transform_clima(path):
    df = pd.read_json(path)

    row = df.iloc[0]

    n = len(json.loads(row["hourly.time"]))

    new_rows = []
    for i in range(n):
        new_rows.append({
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "timezone": row["timezone"],
            "time": json.loads(row["hourly.time"])[i],
            "temperature_2m": json.loads(row["hourly.temperature_2m"])[i],
            "fuente_archivo": row["fuente_archivo"],
            "fecha_proceso_utc": row["fecha_proceso_utc"],
            "id_registro": row["id_registro"],
        })

    df2 = pd.DataFrame(new_rows)
    return df2


# =====================================================
# 🌋 2) PROCESAR USGS — EXPANDIR "geometry" + LIMPIAR
# =====================================================
def transform_usgs(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fixed = []
    for row in data:
        try:
            coords = json.loads(row["geometry.coordinates"])
        except:
            coords = [None, None, None]

        new_row = row.copy()
        new_row["longitude"] = coords[0]
        new_row["latitude"] = coords[1]
        new_row["depth"] = coords[2]
        del new_row["geometry.coordinates"]

        fixed.append(new_row)

    return pd.DataFrame(fixed)


# =====================================================
# 🌎 3) PROCESAR SISMOS ECUADOR — LISTA PLANA
# =====================================================
def transform_sismos_ec(path):
    df = pd.read_json(path)
    # DF ya está en formato de filas → no requiere expandir
    return df


# =====================================================
# 🟪 4) PROCESAR SERCOP — EXPANDIR SOLO RELEASES
# =====================================================
def transform_sercop(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    releases = json.loads(data[0]["releases"])
    df = pd.json_normalize(releases)
    return df


# =====================================================
# 📄 5) CSV NORMAL (solo limpiar y devolver)
# =====================================================
def transform_csv(path):
    return pd.read_csv(path)


# =====================================================
# 🟫 6) JSON NORMAL (sin diccionarios complejos)
# =====================================================
def transform_json_simple(path):
    df = pd.read_json(path)
    return df


# =====================================================
# 🚀 PIPELINE PRINCIPAL
# =====================================================
def transform_all():
    input_dir = "data/raw/"
    output_dir = "data/processed/"
    os.makedirs(output_dir, exist_ok=True)

    files = os.listdir(input_dir)

    for filename in files:
        path = os.path.join(input_dir, filename)
        tipo = detect_dataset_type(filename)

        print(f"\n=== Procesando {filename} ({tipo}) ===")

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

        elif tipo == "parquet":
            df = pd.read_parquet(path)

        else:
            df = transform_json_simple(path)

        # GUARDAR NDJSON (compatibilidad BigQuery)
        out_name = filename.replace(".json", "_clean.json").replace(".csv", "_clean.csv")
        output_path = os.path.join(output_dir, out_name)

        if out_name.endswith(".csv"):
            df.to_csv(output_path, index=False)
        else:
            df.to_json(output_path, orient="records", lines=True, force_ascii=False)

        # SUBIR A GCP
        upload_to_gcs(output_path, out_name)


if __name__ == "__main__":
    transform_all()
