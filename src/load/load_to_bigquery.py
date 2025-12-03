import os
import json
import pandas as pd
from google.cloud import bigquery, storage
from src.config_loader import load_config
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


# ==========================================
# REGLAS DE FILTRADO
# ==========================================
def should_skip_file(filename: str) -> tuple[bool, str]:
    """
    Determina si un archivo debe ser omitido
    Retorna: (skip: bool, reason: str)
    """
    # REGLA 1: Omitir archivos _clean.json (corruptos)
    if filename.endswith("_clean.json"):
        return True, "JSON corrupto/truncado (_clean.json)"
    
    # REGLA 2: Omitir archivos _cleanjson.json (campos con puntos)
    if filename.endswith("_cleanjson.json"):
        return True, "JSON con campos inválidos (_cleanjson.json)"
    
    # REGLA 3: Omitir archivos _cleanbin.json (duplicados de expanded)
    if filename.endswith("_cleanbin.json"):
        return True, "Duplicado de archivo expanded (_cleanbin.json)"
    
    # REGLA 4: Omitir CSV MIES/MREMH con delimitadores rotos
    if "mies_" in filename.lower() or "mremh_" in filename.lower():
        if "_cleancsv.csv" in filename:
            return True, "CSV con delimitadores mal formateados"
    
    # REGLA 5: Omitir archivos duplicados que ya tienen versión expanded
    base_name = re.sub(r"_[0-9]{8,}", "", filename)
    base_name = re.sub(r"_(clean|cleancsv|cleanload).*", "", base_name)
    
    # Si existe un archivo _expanded.ndjson correspondiente, omitir otros formatos
    expanded_pattern = f"{base_name}_*_expanded.ndjson"
    if not filename.endswith("_expanded.ndjson"):
        # Verificar si existe versión expanded
        for f in os.listdir(PROCESSED_DIR):
            if f.endswith("_expanded.ndjson") and base_name in f:
                return True, f"Existe versión expanded válida"
    
    return False, ""


# ==========================================
# VALIDACIÓN DE CSV
# ==========================================
def validate_csv(path: str) -> tuple[bool, str]:
    """
    Valida que el CSV tenga estructura correcta
    Retorna: (valid: bool, reason: str)
    """
    try:
        # Leer primera línea (headers)
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            first_line = f.readline().strip()
        
        # Detectar delimitador
        comma_count = first_line.count(",")
        semicolon_count = first_line.count(";")
        
        # Si tiene muchos semicolons pero ninguna coma, probablemente está mal
        if semicolon_count > 10 and comma_count == 0:
            return False, "CSV usa ';' sin estructura tabular válida"
        
        # Intentar leer con pandas
        separator = ";" if semicolon_count > comma_count else ","
        df = pd.read_csv(path, sep=separator, nrows=1, encoding="utf-8-sig", engine="python")
        
        # Validar que haya más de una columna
        if len(df.columns) <= 1:
            return False, "CSV tiene solo una columna (delimitador incorrecto)"
        
        return True, ""
    
    except Exception as e:
        return False, f"Error al validar CSV: {str(e)}"


# ==========================================
# NORMALIZAR NOMBRES DE COLUMNAS
# ==========================================
def clean_bq_column(name: str) -> str:
    """Limpia nombres de campos para BigQuery"""
    name = name.replace("\ufeff", "")
    name = name.strip()
    name = name.lower()
    name = name.replace(";", "_")
    name = name.replace(" ", "_")
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    
    # Si empieza con número, agregar prefijo
    if name and name[0].isdigit():
        name = f"col_{name}"
    
    return name or "unnamed"


# ==========================================
# PROCESAMIENTO DE ARCHIVOS
# ==========================================
def fix_csv_file(path: str) -> str:
    """Limpia y normaliza CSV"""
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        sample = f.read(4096)

    separator = ";" if sample.count(";") > sample.count(",") else ","
    
    df = pd.read_csv(
        path, 
        sep=separator, 
        encoding="utf-8-sig",
        engine="python",
        on_bad_lines='skip'
    )

    df.columns = [clean_bq_column(str(c)) for c in df.columns]
    
    cleaned_path = path.replace(".csv", "_cleanload.csv")
    df.to_csv(cleaned_path, index=False, encoding="utf-8", sep=",")
    
    print(f"    ✓ Columnas: {', '.join(df.columns[:3])}...")
    return cleaned_path


def fix_ndjson_file(path: str) -> str:
    """Limpia nombres de campos en NDJSON (ya aplanado)"""
    cleaned_path = path.replace(".ndjson", "_cleanload.ndjson")

    with open(path, "r", encoding="utf-8", errors="ignore") as fr, \
         open(cleaned_path, "w", encoding="utf-8") as fw:
        
        for line in fr:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
                cleaned = {clean_bq_column(k): v for k, v in obj.items()}
                fw.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
            except json.JSONDecodeError:
                continue

    return cleaned_path


# ==========================================
# DATASET Y CARGA
# ==========================================
def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[✓] Dataset: {DATASET_ID}\n")
    except Exception:
        print(f"[CREATE] Creando dataset: {DATASET_ID}")
        client.create_dataset(dataset_ref)


def load_csv_to_bq(client, table_id, file_path):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="UTF-8",
        allow_quoted_newlines=True,
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)

    job.result()
    print(f"    ✓ Cargado a BigQuery")


def load_ndjson_to_bq(client, table_id, file_path):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        encoding="UTF-8",
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)

    job.result()
    print(f"    ✓ Cargado a BigQuery")


def download_processed_from_bucket_if_empty():
    if os.path.exists(PROCESSED_DIR) and os.listdir(PROCESSED_DIR):
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(config["gcp"]["bucket_processed"])

    print(f"[INFO] Descargando desde gs://{bucket.name}/processed/\n")

    for blob in bucket.list_blobs(prefix="processed/"):
        dest = os.path.join(PROCESSED_DIR, os.path.basename(blob.name))
        blob.download_to_filename(dest)


# ==========================================
# MAIN
# ==========================================
def main():
    print("\n" + "="*70)
    print("  LOAD TO BIGQUERY - PRODUCCIÓN")
    print("="*70 + "\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    download_processed_from_bucket_if_empty()
    
    all_files = sorted([f for f in os.listdir(PROCESSED_DIR) if not f.startswith(".")])
    
    print(f"📂 Total archivos detectados: {len(all_files)}\n")

    # Estadísticas
    loaded = 0
    skipped = 0
    failed = 0

    for filename in all_files:
        path = os.path.join(PROCESSED_DIR, filename)

        # FILTRADO INTELIGENTE
        should_skip, skip_reason = should_skip_file(filename)
        if should_skip:
            skipped += 1
            print(f"⊘ SKIP: {filename}")
            print(f"  Razón: {skip_reason}\n")
            continue

        # Normalizar nombre de tabla
        base = filename
        base = re.sub(r"_[0-9]{8,}", "", base)
        base = re.sub(r"\..+$", "", base)
        table_name = clean_bq_column(base)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print(f"📄 {filename}")
        print(f"   → Tabla: {table_name}")

        try:
            # CSV
            if filename.endswith(".csv"):
                valid, reason = validate_csv(path)
                if not valid:
                    print(f"   ✗ RECHAZADO: {reason}\n")
                    skipped += 1
                    continue
                
                fixed_path = fix_csv_file(path)
                load_csv_to_bq(client, table_id, fixed_path)
                loaded += 1

            # NDJSON (solo expanded)
            elif filename.endswith(".ndjson"):
                fixed_path = fix_ndjson_file(path)
                load_ndjson_to_bq(client, table_id, fixed_path)
                loaded += 1

            else:
                print(f"   ⊘ Formato no soportado\n")
                skipped += 1

            print()

        except Exception as e:
            failed += 1
            print(f"   ✗ ERROR: {str(e)[:100]}...\n")

    # Resumen final
    print("="*70)
    print(f"  RESUMEN:")
    print(f"  ✓ Cargados:  {loaded}")
    print(f"  ⊘ Omitidos:  {skipped}")
    print(f"  ✗ Fallidos:  {failed}")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()