#!/usr/bin/env python3
"""
LOAD TO BIGQUERY V2 - PRODUCCIÓN
- Filtra archivos corruptos automáticamente
- Aplana JSON anidados (geometry.coordinates → geometry_coordinates)
- Valida CSV antes de cargar
"""

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
# REGLAS DE FILTRADO AUTOMÁTICO
# ==========================================
def should_skip_file(filename: str) -> tuple:
    """
    Determina si un archivo debe ser omitido
    Retorna: (skip: bool, reason: str)
    """
    # REGLA 1: Omitir _clean.json (corruptos)
    if filename.endswith("_clean.json"):
        return True, "JSON corrupto (_clean.json)"
    
    # REGLA 2: Omitir _cleanjson.json (campos inválidos)
    if filename.endswith("_cleanjson.json"):
        return True, "JSON con campos inválidos (_cleanjson.json)"
    
    # REGLA 3: Omitir _cleanbin.json (duplicados)
    if filename.endswith("_cleanbin.json"):
        return True, "Duplicado (_cleanbin.json)"
    
    # REGLA 4: Omitir MIES/MREMH _cleancsv (delimitadores rotos)
    if ("mies_" in filename.lower() or "mremh_" in filename.lower()):
        if "_cleancsv.csv" in filename:
            return True, "CSV MIES/MREMH con delimitadores rotos"
    
    # REGLA 5: Omitir duplicados CSV generales
    if "_cleancsv.csv" in filename:
        base = filename.replace("_cleancsv.csv", "")
        clean_version = f"{base}_clean.csv"
        if os.path.exists(os.path.join(PROCESSED_DIR, clean_version)):
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
        
        semicolon_count = first_line.count(";")
        comma_count = first_line.count(",")
        
        # Si tiene muchos semicolons pero ninguna coma = malformado
        if semicolon_count > 10 and comma_count == 0:
            return False, "CSV malformado (solo semicolons sin comas)"
        
        # Intentar parsear
        separator = ";" if semicolon_count > comma_count else ","
        df = pd.read_csv(
            path, 
            sep=separator, 
            nrows=1, 
            encoding="utf-8-sig", 
            engine="python",
            on_bad_lines='skip'
        )
        
        if len(df.columns) <= 1:
            return False, "Solo 1 columna detectada (delimitador incorrecto)"
        
        return True, ""
    
    except Exception as e:
        return False, f"Error al parsear: {str(e)[:60]}"


# ==========================================
# LIMPIEZA DE NOMBRES DE CAMPOS
# ==========================================
def clean_bq_column(name: str) -> str:
    """Limpia nombres para BigQuery (sin puntos, espacios, etc.)"""
    name = str(name)
    name = name.replace("\ufeff", "")  # BOM
    name = name.strip()
    name = name.lower()
    
    # Reemplazar caracteres especiales
    replacements = {
        ".": "_", ";": "_", " ": "_", "-": "_", "/": "_",
        "(": "", ")": "", '"': "", "'": "", ",": "_"
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    # Eliminar caracteres no alfanuméricos
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
    Aplana diccionario anidado recursivamente
    Ejemplo: {"geometry": {"coordinates": [1,2]}} 
          → {"geometry_coordinates": "[1,2]"}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            # Recursión para objetos anidados
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Listas → JSON string
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    
    return dict(items)


# ==========================================
# PROCESAMIENTO DE ARCHIVOS
# ==========================================
def process_csv(path: str) -> str:
    """Limpia y normaliza CSV"""
    print(f"    [→] Procesando CSV...")
    
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
    
    print(f"    [✓] {len(df)} filas, {len(df.columns)} columnas")
    print(f"        Columnas: {', '.join(df.columns[:5])}...")
    
    return cleaned_path


def process_ndjson(path: str) -> str:
    """
    Aplana NDJSON línea por línea
    Convierte: "geometry.coordinates" → "geometry_coordinates"
    """
    print(f"    [→] Procesando NDJSON...")
    
    cleaned_path = path.replace(".ndjson", "_bqload.ndjson")
    
    count = 0
    skipped = 0
    
    with open(path, "r", encoding="utf-8", errors="ignore") as fr, \
         open(cleaned_path, "w", encoding="utf-8") as fw:
        
        for line_num, line in enumerate(fr, 1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
                
                # PASO 1: Aplanar
                flattened = flatten_dict(obj)
                
                # PASO 2: Limpiar nombres
                cleaned = {clean_bq_column(k): v for k, v in flattened.items()}
                
                fw.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
                count += 1
                
            except json.JSONDecodeError as e:
                skipped += 1
                if skipped <= 3:  # Solo mostrar primeros 3 errores
                    print(f"        [!] Línea {line_num} ignorada: JSON inválido")
    
    print(f"    [✓] {count} registros procesados")
    if skipped > 0:
        print(f"        [!] {skipped} líneas ignoradas (JSON inválido)")
    
    return cleaned_path


# ==========================================
# BIGQUERY
# ==========================================
def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"[✓] Dataset existente: {DATASET_ID}\n")
    except Exception:
        print(f"[+] Creando dataset: {DATASET_ID}")
        client.create_dataset(dataset_ref)
        print(f"[✓] Dataset creado\n")


def load_csv_to_bq(client, table_id, file_path):
    """Carga CSV a BigQuery"""
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # CAMBIO: Reemplaza datos en vez de append
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="UTF-8",
        allow_quoted_newlines=True,
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)

    job.result()
    print(f"    [✓] Cargado a BigQuery")


def load_ndjson_to_bq(client, table_id, file_path):
    """Carga NDJSON a BigQuery"""
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # CAMBIO: Reemplaza datos en vez de append
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        encoding="UTF-8",
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, destination=table_id, job_config=job_config)

    job.result()
    print(f"    [✓] Cargado a BigQuery")


def download_files_if_needed():
    """Descarga archivos del bucket si no existen localmente"""
    if os.path.exists(PROCESSED_DIR) and os.listdir(PROCESSED_DIR):
        print(f"[✓] Archivos locales encontrados\n")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(config["gcp"]["bucket_processed"])

    print(f"[→] Descargando desde gs://{bucket.name}/processed/...\n")

    for blob in bucket.list_blobs(prefix="processed/"):
        if blob.name.endswith("/"):
            continue
        dest = os.path.join(PROCESSED_DIR, os.path.basename(blob.name))
        blob.download_to_filename(dest)
        print(f"    [✓] {os.path.basename(blob.name)}")


# ==========================================
# MAIN
# ==========================================
def main():
    print("\n" + "="*75)
    print("  CARGA A BIGQUERY V2 - CON FILTRADO INTELIGENTE")
    print("="*75 + "\n")

    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])
    ensure_dataset(client)

    download_files_if_needed()
    
    all_files = sorted([
        f for f in os.listdir(PROCESSED_DIR) 
        if not f.startswith(".") and not f.endswith("_bqload.csv") and not f.endswith("_bqload.ndjson")
    ])
    
    print(f"\n[i] {len(all_files)} archivos detectados\n")

    # Contadores
    loaded = 0
    skipped = 0
    failed = 0

    for idx, filename in enumerate(all_files, 1):
        path = os.path.join(PROCESSED_DIR, filename)

        print(f"\n{'='*75}")
        print(f"[{idx}/{len(all_files)}] {filename}")
        print("="*75)

        # ========== FILTRADO AUTOMÁTICO ==========
        should_skip, skip_reason = should_skip_file(filename)
        if should_skip:
            skipped += 1
            print(f"[⊘] OMITIDO: {skip_reason}\n")
            continue

        # Nombre de tabla
        base = filename
        base = re.sub(r"_\d{8,}", "", base)  # Eliminar timestamps
        base = re.sub(r"_(clean|expanded|releases|cleancsv|cleanbin).*", "", base)
        base = re.sub(r"\.\w+$", "", base)  # Eliminar extensión
        table_name = clean_bq_column(base)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        print(f"[→] Tabla destino: {table_name}")

        try:
            # ========== CSV ==========
            if filename.endswith(".csv"):
                valid, reason = validate_csv(path)
                if not valid:
                    print(f"[✗] CSV inválido: {reason}\n")
                    skipped += 1
                    continue
                
                processed_path = process_csv(path)
                load_csv_to_bq(client, table_id, processed_path)
                loaded += 1

            # ========== NDJSON ==========
            elif filename.endswith(".ndjson"):
                processed_path = process_ndjson(path)
                load_ndjson_to_bq(client, table_id, processed_path)
                loaded += 1

            else:
                print(f"[⊘] Formato no soportado\n")
                skipped += 1

        except Exception as e:
            failed += 1
            error_msg = str(e)[:200]
            print(f"[✗] ERROR: {error_msg}\n")

    # ========== RESUMEN FINAL ==========
    print("\n" + "="*75)
    print("  RESUMEN FINAL")
    print("="*75)
    print(f"  ✓ Cargados:  {loaded:>3}")
    print(f"  ⊘ Omitidos:  {skipped:>3}")
    print(f"  ✗ Fallidos:  {failed:>3}")
    print("="*75 + "\n")

    if failed == 0 and loaded > 0:
        print("🎉 ¡CARGA COMPLETADA SIN ERRORES!")
    elif failed > 0:
        print("⚠️  Algunos archivos fallaron. Revisa los errores arriba.")


if __name__ == "__main__":
    main()