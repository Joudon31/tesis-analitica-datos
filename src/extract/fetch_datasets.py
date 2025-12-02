import requests
import os
import datetime
from urllib.parse import urlparse
from google.cloud import storage
from src.config_loader import load_config

# Cargar configuración
config = load_config()
MODE = config["mode"]

DATA_DIR = "data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

# ================================
# Datasets estáticos (en GCP)
# ================================
STATIC_FILES = {
    "mies_bonos_2025": "mies_bonos_pensiones_2025_abril_csv.csv",
    "presupuesto_misiones_2025": "mremh_presupuestoejecutadomisionesecuadorexterior_2025junio.csv",
    "catalogo_compras_2025": "releases_2025_catalogo_electronico_compra_directa.json"
}

# ================================
# APIs automáticas
# ================================
API_SOURCES = {
    "api_clima": "https://api.open-meteo.com/v1/forecast?latitude=-2&longitude=-80&hourly=temperature_2m",
    "api_sismos_ec": "https://api.gael.cloud/general/public/sismos",
    "api_sismos_usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&orderby=time&limit=50"
}


# ================================
# FUNCIONES HELPERS
# ================================
def download_api(name, url, now):
    """Descarga datos desde APIs y los guarda localmente."""
    print(f"[INFO] Llamando API: {name}...")

    r = requests.get(url, timeout=60)
    r.raise_for_status()

    filename = f"{name}_{now}.json"
    path = os.path.join(DATA_DIR, filename)

    with open(path, "wb") as f:
        f.write(r.content)

    print(f"[OK] API guardada: {path}")
    return path


def copy_from_bucket(blob_name, now):
    """Copia archivos estáticos desde el bucket GCP a data/raw."""
    bucket_name = config["gcp"]["bucket_raw"]

    print(f"[INFO] Copiando desde GCP: {blob_name}")

    client = storage.Client.from_service_account_json(config["gcp"]["credentials"])
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    ext = blob_name.split(".")[-1]
    filename = f"{blob_name.replace('.', '_')}_{now}.{ext}"
    path = os.path.join(DATA_DIR, filename)

    blob.download_to_filename(path)

    print(f"[OK] Copiado local: {path}")
    return path


# ================================
# MAIN
# ================================
def main():
    now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    downloaded_files = []

    # ---- ESTÁTICOS DESDE GCP ----
    print("\n========== ARCHIVOS ESTÁTICOS GCP ==========")
    for name, blob_name in STATIC_FILES.items():
        try:
            path = copy_from_bucket(blob_name, now)
            downloaded_files.append(path)
        except Exception as e:
            print(f"[ERROR] No se pudo copiar {blob_name}: {e}")

    # ---- APIs AUTOMÁTICAS ----
    print("\n========== APIS AUTOMÁTICAS ==========")
    for name, url in API_SOURCES.items():
        try:
            path = download_api(name, url, now)
            downloaded_files.append(path)
        except Exception as e:
            print(f"[ERROR] API falló ({name}): {e}")

    # ---- RESUMEN ----
    print("\n========== RESUMEN DESCARGAS ==========")
    for f in downloaded_files:
        print(f" - {f}")

    print("\n[FIN] Extracción completada.")


if __name__ == "__main__":
    main()
