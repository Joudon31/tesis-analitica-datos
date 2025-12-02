import glob
import os
import pandas as pd
from google.cloud import storage

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

GCP_BUCKET = "tesis-processed-datos-joseph"

def subir_gcp(local_path, bucket_name):
    """Sube un archivo a Google Cloud Storage"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(os.path.basename(local_path))
        blob.upload_from_filename(local_path)
        print(f"[GCP] Subido a gs://{bucket_name}/{os.path.basename(local_path)}")
    except Exception as e:
        print(f"[GCP ERROR] {e}")

def limpiar_df(df):
    df = df.dropna(axis=1, how="all")  # columnas vacías
    df = df.drop_duplicates()           # filas duplicadas
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ".").str.replace("%", ""),
                    errors="ignore"
                )
            except Exception:
                continue
    return df

def process():
    files = glob.glob(os.path.join(RAW_DIR, "*"))

    if not files:
        print("[INFO] No se encontraron archivos en data/raw")
        return

    for f in files:
        fname = os.path.basename(f)
        out_file = os.path.join(OUT_DIR, os.path.splitext(fname)[0] + ".parquet")

        # Procesar solo si no existe localmente
        if not os.path.exists(out_file):
            print(f"[INFO] Procesando: {fname}")
            try:
                if fname.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(f, sep=None, engine="python", encoding="utf-8")
                    except Exception:
                        df = pd.read_csv(f, sep=None, engine="python", encoding="latin1")
                    df = limpiar_df(df)
                    df.to_parquet(out_file, engine="pyarrow", index=False)
                    print(f"[OK] CSV -> Parquet: {out_file}")

                elif fname.lower().endswith(".json") or fname.lower().endswith(".bin"):
                    try:
                        df = pd.read_json(f, lines=True, encoding="utf-8")
                    except Exception:
                        df = pd.read_json(f, lines=True, encoding="latin1")
                    df = limpiar_df(df)
                    df.to_parquet(out_file, engine="pyarrow", index=False)
                    print(f"[OK] JSON/BIN -> Parquet: {out_file}")

                else:
                    print(f"[SKIP] Tipo no soportado: {fname}")
                    continue

            except Exception as e:
                print(f"[ERROR] Falló el procesamiento de {fname}: {e}")
                continue
        else:
            print(f"[SKIP] Ya procesado localmente: {fname}")

        # Subir siempre a GCP aunque el archivo exista localmente
        subir_gcp(out_file, GCP_BUCKET)

    # Resumen final
    print("\n[RESUMEN] Archivos procesados/subidos en data/processed y GCP:")
    for pf in glob.glob(os.path.join(OUT_DIR, "*.parquet")):
        print(f" - {os.path.basename(pf)}")

if __name__ == "__main__":
    process()
