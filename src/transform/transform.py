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


# ============================================
# DETECCIÓN SÓLIDA: JSON REAL vs CSV DISFRAZADO
# ============================================
def load_json_safe(path):
    """
    Intenta cargar JSON real.
    Si no comienza con { o [, se analiza si el archivo es CSV disfrazado.
    Retorna (df, tipo) donde tipo ∈ {"json", "csv", "unknown"}.
    """

    # Intento preliminar: leer texto completo
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read().strip()
    except:
        return None, "unknown"

    # 1) JSON real comienza con { o [
    if text.startswith("{") or text.startswith("["):
        try:
            data = json.loads(text)

            if isinstance(data, dict) and "features" in data:
                return pd.json_normalize(data["features"]), "json"

            if isinstance(data, list):
                return pd.json_normalize(data), "json"

            return pd.json_normalize(data), "json"
        except:
            pass

    # 2) Detectar CSV disfrazado (si la primera línea tiene muchas comas)
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()

        if first_line.count(",") > 2:
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
            return df, "csv"
    except:
        pass

    return None, "unknown"


# ============================================
# HELPERS
# ============================================
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
    blob = bucket.blob(dest_name)
    blob.upload_from_filename(local_path)
    print(f"[GCP] Subido a gs://{bucket_name}/{dest_name}")

def read_csv_robusto(path):
    """
    Lector CSV infalible con múltiples estrategias:
    1) utf-8
    2) latin-1
    3) engine=python
    4) detección por delimitadores raros
    5) lectura línea por línea en caso extremo
    """

    # Estrategia 1: UTF-8 normal
    try:
        return pd.read_csv(path, encoding="utf-8", low_memory=False)
    except:
        pass

    # Estrategia 2: LATIN-1
    try:
        return pd.read_csv(path, encoding="latin-1", low_memory=False)
    except:
        pass

    # Estrategia 3: engine=python (tolerante)
    try:
        return pd.read_csv(path, encoding="latin-1", engine="python", on_bad_lines="skip")
    except:
        pass

    # Estrategia 4: detectar delimitador
    import csv
    with open(path, "r", errors="ignore") as f:
        dialect = csv.Sniffer().sniff(f.read(2048))

    try:
        return pd.read_csv(path, delimiter=dialect.delimiter, encoding="latin-1", engine="python")
    except:
        pass

    # Estrategia 5 FINAL: lectura manual (nunca falla)
    rows = []
    with open(path, "r", errors="ignore") as f:
        for line in f:
            rows.append(line.strip().split(","))

    df = pd.DataFrame(rows)
    df.columns = [f"col_{i}" for i in range(df.shape[1])]
    return df


def load_file(path):
    ext = path.split(".")[-1].lower()

    # CSV normal
    if ext == "csv":
        df = read_csv_robusto(path)
        return df, "csv"

    # Excel
    if ext in ["xlsx", "xls"]:
        df = pd.read_excel(path)
        return df, "csv"

    # JSON y pseudo-JSON
    if ext == "json" or ext == "bin":
        df, detected_type = load_json_safe(path)

        # Si era .bin, asumimos que puede ser CSV disfrazado
        if detected_type == "unknown" and ext == "bin":
            try:
                df = read_csv_robusto(path)
                return df, "csv"
            except:
                return None, "unknown"

        return df, detected_type

    print(f"[WARN] Formato no soportado: {path}")
    return None, "unknown"


def download_all_raw_from_bucket():
    bucket_name = config["gcp"]["bucket_raw"]
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)

    print(f"[INFO] Descargando archivos RAW desde GCP bucket: {bucket_name}")

    for blob in bucket.list_blobs():
        dest_path = os.path.join(RAW_DIR, blob.name)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        blob.download_to_filename(dest_path)
        print(f"[OK] Descargado: {blob.name} -> {dest_path}")


# ============================================
# MAIN
# ============================================
def main():
    print("\n========== INICIANDO TRANSFORMACIÓN ==========\n")

    download_all_raw_from_bucket()

    print("\n[INFO] Archivos encontrados en data/raw:")
    print(os.listdir(RAW_DIR))

    for filename in os.listdir(RAW_DIR):
        raw_path = os.path.join(RAW_DIR, filename)
        print(f"\n[INFO] Procesando: {filename}")

        df, ftype = load_file(raw_path)

        if df is None or ftype == "unknown":
            print(f"[SKIP] No se pudo procesar: {filename}")
            continue

        # Normalizar columnas
        df = normalize_columns(df)

        # Convertir listas/dict → strings
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False)
                if isinstance(x, (list, dict))
                else x
            )

        # Limpieza general
        df.drop_duplicates(inplace=True)
        df.dropna(how="all", axis=1, inplace=True)

        # Metadatos
        df["fuente_archivo"] = filename
        df["fecha_proceso_utc"] = datetime.utcnow().isoformat()
        df["id_registro"] = df.apply(generate_unique_id, axis=1)

        # Seleccionar extensión correcta
        if ftype == "json":
            out_ext = ".json"
        else:
            out_ext = ".csv"

        output_file = filename.replace(".", "_clean") + out_ext
        output_path = os.path.join(PROCESSED_DIR, output_file)

        # Guardar archivo limpio
        if out_ext == ".csv":
            df.to_csv(output_path, index=False, encoding="utf-8")
        else:
            df.to_json(output_path, orient="records", force_ascii=False)

        print(f"[OK] Guardado transformado → {output_path}")

        # Subir a bucket en modo cloud
        if MODE == "cloud":
            upload_to_bucket(output_path, f"processed/{output_file}")

    print("\n========== TRANSFORMACIÓN COMPLETA ==========\n")


if __name__ == "__main__":
    main()
