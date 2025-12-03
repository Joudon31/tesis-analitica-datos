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
def should_skip_file(filename: str) -> tuple:
    """
    Determina si un archivo debe ser omitido
    Retorna: (skip: bool, reason: str)
    """
    # REGLA 1: Omitir archivos _clean.json (corruptos)
    if filename.endswith("_clean.json"):
        return True, "JSON corrupto (_clean.json)"
    
    # REGLA 2: Omitir archivos _cleanjson.json (campos con puntos)
    if filename.endswith("_cleanjson.json"):
        return True, "JSON con campos inválidos (_cleanjson.json)"
    
    # REGLA 3: Omitir archivos _cleanbin.json (duplicados)
    if filename.endswith("_cleanbin.json"):
        return True, "Duplicado (_cleanbin.json)"
    
    # REGLA 4: Omitir CSV MIES/MREMH con sufijo _cleancsv
    if ("mies_" in filename.lower() or "mremh_" in filename.lower()) and "_cleancsv.csv" in filename:
        return True, "CSV con delimitadores rotos (_cleancsv)"
    
    # REGLA 5: Omitir archivos duplicados CSV
    if "_cleancsv.csv" in filename:
        base = filename.replace("_cleancsv.csv", "")
        clean_version = f"{base}_clean.csv"
        if clean_version in os.listdir(PROCESSED_DIR):
            return True, "Duplicado de _clean.csv"
    
    return False, ""


# ==========================================
# VALIDACIÓN DE CSV
# ==========================================
def validate_csv(path: str) -> tuple:
    """Valida estructura del CSV"""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            first_line = f.readline().strip()
        
        # Detectar si tiene solo semicolons sin estructura
        semicolon_count = first_line.count(";")
        comma_count = first_line.count(",")
        
        if semicolon_count > 10 and comma_count == 0:
            return False, "CSV malformado (solo semicolons)"
        
        # Intentar parsear
        separator = ";" if semicolon_count > comma_count else ","
        df = pd.read_csv(path, sep=separator, nrows=1, encoding="utf-8-sig", engine="python")
        
        if len(df.columns) <= 1:
            return False, "Solo 1 columna detectada"
        
        return True, ""
    
    except Exception as e:
        return False, f"Error: {str(e)[:50]}"


# ==========================================
# LIMPIEZA DE NOMBRES DE CAMPOS
# ==========================================
def clean_bq_column(name: str) -> str:
    """Limpia nombres para BigQuery"""
    name = str(name)
    name = name.replace("\ufeff", "")
    name = name.strip()
    name = name.lower()
    
    # CRÍTICO: Reemplazar puntos y caracteres especiales
    name = name.replace(".", "_")
    name = name.replace(";", "_")
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = name.replace("/", "_")
    name = name.replace("(", "")
    name = name.replace(")", "")
    name = name.replace('"', "")
    name = name.replace("'", "")
    
    # Eliminar caracteres no permitidos
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    
    # Si empieza con número, agregar prefijo
    if name and name[0].isdigit():
        name = f"col_{name}"
    
    return name or "unnamed"


# ==========================================
# APLANAMIENTO RECURSIVO DE JSON
# ==========================================
def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Aplana un diccionario anidado recursivamente
    Ejemplo: {"a": {"b": 1}} → {"a_b": 1}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            # Recursión para objetos anidados
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Listas se convierten a JSON string
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    
    return dict(items)


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

    # Limpiar nombres de columnas
    df.columns = [clean_bq_column(c) for c in df.columns]
    
    cleaned_path = path.replace(".csv", "_bqload.csv")
    df.to_csv(cleaned_path, index=False, encoding="utf-8", sep=",")
    
    print(f"    ✓ {len(df.columns)} columnas: {', '.join(df.columns[:3])}...")
    return cleaned_path


def fix_ndjson_file(path: str) -> str:
    """
    Aplana NDJSON línea por línea
    CRÍTICO: Convierte "geometry.coordinates" → "geometry_coordinates"
    """
    cleaned_path = path.replace(".ndjson", "_bqload.ndjson")
    
    count = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as fr, \
         open(cleaned_path, "w", encoding="utf-8") as fw:
        
        for line in fr:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
                
                # PASO 1: Aplanar estructura anidada
                flattened = flatten_dict(obj)
                
                # PASO 2: Limpiar nombres de campos
                cleaned = {clean_bq_column(k): v for k, v in flattened.items()}
                
                fw.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
                count += 1
                
            except json.JSONDecodeError:
                continue
    
    print(f"    ✓ {count} registros procesados")
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
        print(f"[✓] Dataset creado\n")


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


def download_processed_from_bucket_if_empty():
    if os.path.exists(PROCESSED_DIR) and os.listdir(PROCESSED_DIR):
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(config["gcp"]["bucket_processed"])

    print(f"[INFO] Descargando desde bucket...\n")

    for blob in bucket.list_blobs(prefix="processed/"):
        if blob.name.endswith("/"):
            continue
        dest = os.path.join(PROCESSED_DIR, os.path.basename(blob.name))
        blob.download_to_filename(dest)


# ==========================================
# MAIN
# ==========================================
def main():
    print("\n" + "="*70)
    print("  CARGA A BIGQUERY - PRODUCCIÓN")
    print("="*70 + "\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    download_processed_from_bucket_if_empty()
    
    all_files = sorted([f for f in os.listdir(PROCESSED_DIR) if not f.startswith(".")])
    
    print(f"📂 {len(all_files)} archivos detectados\n")

    # Estadísticas
    loaded = 0
    skipped = 0
    failed = 0

    for filename in all_files:
        path = os.path.join(PROCESSED_DIR, filename)

        # ========== FILTRADO INTELIGENTE ==========
        should_skip, skip_reason = should_skip_file(filename)
        if should_skip:
            skipped += 1
            print(f"⊘ {filename[:50]}...")
            print(f"   Razón: {skip_reason}\n")
            continue

        # Normalizar nombre de tabla
        base = filename
        base = re.sub(r"_[0-9]{8,}", "", base)
        base = re.sub(r"_(clean|expanded|releases|cleancsv|cleanbin|cleanload|bqload).*", "", base)
        base = re.sub(r"\..+$", "", base)
        table_name = clean_bq_column(base)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print(f"{'='*70}")
        print(f"📄 {filename}")
        print(f"   → {table_name}")
        print(f"{'='*70}")

        try:
            # ========== CSV ==========
            if filename.endswith(".csv"):
                valid, reason = validate_csv(path)
                if not valid:
                    print(f"   ✗ CSV inválido: {reason}\n")
                    skipped += 1
                    continue
                
                fixed_path = fix_csv_file(path)
                load_csv_to_bq(client, table_id, fixed_path)
                print(f"   ✓ CARGADO\n")
                loaded += 1

            # ========== NDJSON ==========
            elif filename.endswith(".ndjson"):
                fixed_path = fix_ndjson_file(path)
                load_ndjson_to_bq(client, table_id, fixed_path)
                print(f"   ✓ CARGADO\n")
                loaded += 1

            else:
                print(f"   ⊘ Formato no soportado\n")
                skipped += 1

        except Exception as e:
            failed += 1
            error_msg = str(e)[:150]
            print(f"   ✗ ERROR: {error_msg}...\n")

    # ========== RESUMEN ==========
    print("="*70)
    print(f"  RESUMEN FINAL")
    print("="*70)
    print(f"  ✓ Cargados exitosos:  {loaded}")
    print(f"  ⊘ Omitidos (filtros): {skipped}")
    print(f"  ✗ Fallidos:           {failed}")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()