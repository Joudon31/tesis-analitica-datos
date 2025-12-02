import os
import json
import hashlib
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from google.cloud import storage
from src.config_loader import load_config
import ast

config = load_config()
MODE = config.get("mode", "local")

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


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


# -----------------------
# Utilities for parsing
# -----------------------
def try_parse_json_like(text):
    """Intenta convertir a objeto JSON desde string que contenga JSON o lista.
    Usa json.loads, y si falla intenta ast.literal_eval."""
    if text is None:
        return None
    if isinstance(text, (dict, list)):
        return text
    s = str(text).strip()
    if s == "":
        return None
    # If already looks like JSON (starts with [ or {)
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            pass
    # Try ast.literal_eval as fallback (handles single quotes)
    try:
        return ast.literal_eval(s)
    except Exception:
        pass
    return None


def ensure_list_of_same_length(l1, l2):
    if l1 is None or l2 is None:
        return False
    return isinstance(l1, list) and isinstance(l2, list) and len(l1) == len(l2)


# -----------------------
# Readers
# -----------------------
def read_csv_robusto(path):
    """Lectura tolerante de CSV: varios intentos de decodificación y fallbacks."""
    # intentos por encoding y motores
    attempts = [
        {"encoding": "utf-8", "engine": "c"},
        {"encoding": "latin-1", "engine": "c"},
        {"encoding": "latin-1", "engine": "python", "on_bad_lines": "skip"},
    ]
    for a in attempts:
        try:
            return pd.read_csv(path, **{k: v for k, v in a.items()})
        except Exception:
            continue

    # último recurso: leer líneas y construir dataframe (separador coma)
    rows = []
    with open(path, "r", errors="ignore") as f:
        for line in f:
            rows.append(line.strip().split(","))
    df = pd.DataFrame(rows)
    if df.shape[0] > 0:
        df.columns = [f"col_{i}" for i in range(df.shape[1])]
    return df


def load_json_file(path):
    """Carga JSON desde archivo; devuelve objeto (list/dict) o None."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read().strip()
        if text == "":
            return None
        # A menudo file is NDJSON or array
        # Try normal json loads
        try:
            return json.loads(text)
        except Exception:
            # Try reading as NDJSON lines
            arr = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    arr.append(json.loads(line))
                except Exception:
                    # fallback: attempt ast literal
                    try:
                        arr.append(ast.literal_eval(line))
                    except Exception:
                        # skip invalid line
                        continue
            if arr:
                return arr
    except Exception:
        return None
    return None


# -----------------------
# Expanders (dataset-specific)
# -----------------------
def expand_clima(obj, fuente_archivo, fecha_proceso_utc, id_base):
    """Expande la estructura hourly: produce lista de dicts"""
    # obj is the parsed JSON (dict)
    hourly = obj.get("hourly", {}) if isinstance(obj, dict) else {}
    time_field = hourly.get("time") or obj.get("hourly.time")
    temp_field = hourly.get("temperature_2m") or hourly.get("hourly.temperature_2m") or hourly.get("hourly.temperature_2m")
    # If time_field or temp_field are strings that look like lists, parse them
    time_list = try_parse_json_like(time_field) if not isinstance(time_field, list) else time_field
    temp_list = try_parse_json_like(temp_field) if not isinstance(temp_field, list) else temp_field

    # If both lists and same length -> expand
    if ensure_list_of_same_length(time_list, temp_list):
        for t, temp in zip(time_list, temp_list):
            yield {
                "fuente_archivo": fuente_archivo,
                "fecha_proceso_utc": fecha_proceso_utc,
                "id_registro": hashlib.md5(f"{id_base}|{t}|{temp}".encode()).hexdigest(),
                "time": t,
                "temperature_2m": temp,
                # keep some top-level metadata
                "latitude": obj.get("latitude"),
                "longitude": obj.get("longitude"),
                "elevation": obj.get("elevation")
            }
        return

    # fallback: yield top-level flattened object
    flattened = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (str, int, float, bool)) :
                flattened[k] = v
            else:
                try:
                    flattened[k] = json.dumps(v, ensure_ascii=False)
                except Exception:
                    flattened[k] = str(v)
    flattened.update({
        "fuente_archivo": fuente_archivo,
        "fecha_proceso_utc": fecha_proceso_utc,
        "id_registro": hashlib.md5((id_base + "_fallback").encode()).hexdigest()
    })
    yield flattened


def expand_usgs_feature(feature, fuente_archivo, fecha_proceso_utc):
    """Convierte Feature en dict plano; arregla geometry.coordinates si viene como str."""
    out = {}
    # properties often flat but keys may contain dots -> convert to safe keys
    if isinstance(feature, dict):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        out.update({f"properties.{k}": v for k, v in props.items()})
        # geometry coordinates may be a string
        coords = geom.get("coordinates", None)
        if isinstance(coords, str):
            parsed = try_parse_json_like(coords)
            coords = parsed if parsed is not None else coords
        out["geometry.coordinates"] = coords
        out["type"] = feature.get("type")
        out["id"] = feature.get("id")
    else:
        # If feature is flatten like CSV row, just stringify
        out["feature"] = json.dumps(feature, ensure_ascii=False)
    out["fuente_archivo"] = fuente_archivo
    out["fecha_proceso_utc"] = fecha_proceso_utc
    out["id_registro"] = hashlib.md5(json.dumps(out, sort_keys=True).encode()).hexdigest()
    return out


def expand_sismos_list(obj, fuente_archivo, fecha_proceso_utc):
    """Recibe un objeto que puede ser dict with 'features' list or a list of features."""
    if isinstance(obj, dict) and "features" in obj and isinstance(obj["features"], list):
        features = obj["features"]
    elif isinstance(obj, list):
        features = obj
    else:
        # fallback: try parse 'data' keys
        return []

    for feat in features:
        yield expand_usgs_feature(feat, fuente_archivo, fecha_proceso_utc)


def expand_sercop_releases(obj, fuente_archivo, fecha_proceso_utc):
    """Convierte releases (puede venir como string) a registros por release,
    y si expansions awards->items está configurado, también devuelve items."""
    # obj probably has 'releases' key, or is itself a dict with 'releases'
    releases_raw = obj.get("releases") if isinstance(obj, dict) else None
    # if releases_raw looks like string with list -> parse
    releases = None
    if isinstance(releases_raw, str):
        releases = try_parse_json_like(releases_raw)
    elif isinstance(releases_raw, list):
        releases = releases_raw
    elif isinstance(obj, list):
        releases = obj
    else:
        # maybe the whole obj is already a dict representing a release
        releases = [obj]

    if not releases:
        return []

    for rel in releases:
        # build release-level record
        release_id = rel.get("id") or rel.get("ocid") or hashlib.md5(json.dumps(rel, sort_keys=True).encode()).hexdigest()
        base = {
            "release_id": release_id,
            "fuente_archivo": fuente_archivo,
            "fecha_proceso_utc": fecha_proceso_utc,
            "id_registro": hashlib.md5(f"{release_id}".encode()).hexdigest(),
            # keep buyer short if exists
            "buyer": json.dumps(rel.get("buyer", {}), ensure_ascii=False) if rel.get("buyer") else None,
            "date": rel.get("date")
        }
        # yield release-level
        yield ("release", base, rel)

        # expand awards -> items if present
        awards = rel.get("awards") or []
        for aw in awards:
            award_id = aw.get("id") or hashlib.md5(json.dumps(aw, sort_keys=True).encode()).hexdigest()
            items = aw.get("items") or []
            for it in items:
                item_record = {
                    "release_id": release_id,
                    "award_id": award_id,
                    "item_id": it.get("id") or hashlib.md5(json.dumps(it, sort_keys=True).encode()).hexdigest(),
                    "description": it.get("description"),
                    "quantity": it.get("quantity"),
                    "unit_value": (it.get("unit", {}) or {}).get("value") if isinstance(it.get("unit"), dict) else None,
                    "fuente_archivo": fuente_archivo,
                    "fecha_proceso_utc": fecha_proceso_utc,
                    "id_registro": hashlib.md5(json.dumps({"release": release_id, "item": it}, sort_keys=True).encode()).hexdigest()
                }
                yield ("item", item_record, it)


# -----------------------
# Generic load_file
# -----------------------
def load_file_dynamic(path):
    ext = path.split(".")[-1].lower()
    if ext == "csv":
        df = read_csv_robusto(path)
        return ("csv", df)
    if ext in ["xlsx", "xls"]:
        df = pd.read_excel(path)
        return ("csv", df)
    # JSON-like
    parsed = load_json_file(path)
    if parsed is not None:
        return ("json", parsed)
    # bin or unknown - attempt to read as json then csv fallback
    try:
        parsed = load_json_file(path)
        if parsed:
            return ("json", parsed)
        df = read_csv_robusto(path)
        return ("csv", df)
    except Exception:
        return (None, None)


# -----------------------
# Main pipeline
# -----------------------
def write_ndjson_lines(out_path, iterable):
    with open(out_path, "w", encoding="utf-8") as fout:
        count = 0
        for obj in iterable:
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    return count


def main():
    print("\n========== INICIANDO TRANSFORMACIÓN ==========\n")
    print(f"Modo: {MODE}")
    # ensure directories
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # If mode cloud, download files from bucket_raw
    if MODE == "cloud":
        client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
        bucket = client.bucket(config["gcp"]["bucket_raw"])
        for blob in bucket.list_blobs():
            dest = os.path.join(RAW_DIR, blob.name)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            blob.download_to_filename(dest)
            print(f"[GCP] Descargado → {dest}")

    files = sorted(os.listdir(RAW_DIR))
    print("[INFO] Archivos en raw:", files)

    for filename in files:
        raw_path = os.path.join(RAW_DIR, filename)
        print(f"\n[INFO] Procesando: {filename}")

        ftype, content = load_file_dynamic(raw_path)
        if ftype is None or content is None:
            print(f"[SKIP] No se pudo leer: {filename}")
            continue

        fecha_proceso_utc = datetime.utcnow().isoformat()
        base_name = filename.rsplit(".", 1)[0]

        # Decide ruta y tratamiento según nombre de archivo (opción A: tabla por archivo)
        if filename.startswith("api_clima"):
            # content is dict or list
            if isinstance(content, list):
                # if NDJSON array of objects, take first or iterate
                iterable = []
                for obj in content:
                    for rec in expand_clima(obj, filename, fecha_proceso_utc, base_name):
                        iterable.append(rec)
            elif isinstance(content, dict):
                iterable = list(expand_clima(content, filename, fecha_proceso_utc, base_name))
            else:
                iterable = []
            out_filename = f"{base_name}_expanded.ndjson"
            out_path = os.path.join(PROCESSED_DIR, out_filename)
            n = write_ndjson_lines(out_path, iterable)
            print(f"[OK] Clima expandido -> {out_path} ({n} filas)")
            if MODE == "cloud":
                upload_to_bucket(out_path, f"processed/{out_filename}")

        elif filename.startswith("api_sismos_usgs") or filename.startswith("api_sismos_ec"):
            # sismos: content is probably dict with features list
            iterable = []
            for rec in expand_sismos_list(content, filename, fecha_proceso_utc):
                iterable.append(rec)
            out_filename = f"{base_name}_expanded.ndjson"
            out_path = os.path.join(PROCESSED_DIR, out_filename)
            n = write_ndjson_lines(out_path, iterable)
            print(f"[OK] Sismos expandido -> {out_path} ({n} filas)")
            if MODE == "cloud":
                upload_to_bucket(out_path, f"processed/{out_filename}")

        elif filename.startswith("releases"):
            # SERCOP releases: expand releases and items
            iterable_releases = []
            iterable_items = []
            for t in expand_sercop_releases(content, filename, fecha_proceso_utc):
                typ, payload, rawrel = t
                if typ == "release":
                    iterable_releases.append(payload)
                elif typ == "item":
                    iterable_items.append(payload)
            if iterable_releases:
                out_rel = os.path.join(PROCESSED_DIR, f"{base_name}_releases_expanded.ndjson")
                write_ndjson_lines(out_rel, iterable_releases)
                print(f"[OK] Releases -> {out_rel} ({len(iterable_releases)} filas)")
                if MODE == "cloud":
                    upload_to_bucket(out_rel, f"processed/{os.path.basename(out_rel)}")
            if iterable_items:
                out_items = os.path.join(PROCESSED_DIR, f"{base_name}_items_expanded.ndjson")
                write_ndjson_lines(out_items, iterable_items)
                print(f"[OK] Items -> {out_items} ({len(iterable_items)} filas)")
                if MODE == "cloud":
                    upload_to_bucket(out_items, f"processed/{os.path.basename(out_items)}")

        elif ftype == "csv":
            # Save standardized CSV clean (if pandas DataFrame)
            df = content
            if isinstance(df, pd.DataFrame):
                # normalize columns
                df.columns = (
                    df.columns.str.strip()
                              .str.lower()
                              .str.replace(" ", "_")
                              .str.replace("-", "_")
                              .str.replace("/", "_")
                )
                df["fuente_archivo"] = filename
                df["fecha_proceso_utc"] = fecha_proceso_utc
                df["id_registro"] = df.apply(lambda r: generate_unique_id(r), axis=1)
                out_filename = f"{base_name}_cleancsv.csv"
                out_path = os.path.join(PROCESSED_DIR, out_filename)
                df.to_csv(out_path, index=False, encoding="utf-8")
                print(f"[OK] CSV procesado -> {out_path}")
                if MODE == "cloud":
                    upload_to_bucket(out_path, f"processed/{out_filename}")
            else:
                print(f"[WARN] CSV leído pero no DataFrame: {filename}")

        elif ftype == "json":
            # If it's JSON but not one of the special cases above, create NDJSON line per record
            iterable = []
            if isinstance(content, list):
                for obj in content:
                    # flatten simple objects minimally
                    rec = {}
                    for k, v in (obj.items() if isinstance(obj, dict) else []):
                        if isinstance(v, (str, int, float, bool)):
                            rec[k] = v
                        else:
                            rec[k] = json.dumps(v, ensure_ascii=False)
                    rec["fuente_archivo"] = filename
                    rec["fecha_proceso_utc"] = fecha_proceso_utc
                    rec["id_registro"] = hashlib.md5(json.dumps(rec, sort_keys=True).encode()).hexdigest()
                    iterable.append(rec)
            elif isinstance(content, dict):
                rec = {}
                for k, v in content.items():
                    if isinstance(v, (str, int, float, bool)):
                        rec[k] = v
                    else:
                        rec[k] = json.dumps(v, ensure_ascii=False)
                rec["fuente_archivo"] = filename
                rec["fecha_proceso_utc"] = fecha_proceso_utc
                rec["id_registro"] = hashlib.md5(json.dumps(rec, sort_keys=True).encode()).hexdigest()
                iterable.append(rec)
            if iterable:
                out_filename = f"{base_name}_clean_expanded.ndjson"
                out_path = os.path.join(PROCESSED_DIR, out_filename)
                write_ndjson_lines(out_path, iterable)
                print(f"[OK] JSON general -> {out_path} ({len(iterable)} filas)")
                if MODE == "cloud":
                    upload_to_bucket(out_path, f"processed/{out_filename}")
            else:
                print(f"[WARN] No se generaron registros para {filename}")

        else:
            print(f"[SKIP] No se reconoce el patrón de nombre ni el tipo: {filename}")

    print("\n========== TRANSFORMACIÓN COMPLETA ==========\n")


if __name__ == "__main__":
    main()
