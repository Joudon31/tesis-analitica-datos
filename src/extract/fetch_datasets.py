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

# EDITAR: tus datasets reales luego
datasets = {
        # ----------------------------
    # 1. Datasets estáticos desde GCP
    # ----------------------------
    "mies_bonos_2025": "gs://tu_bucket_raw/mies_bonos_pensiones_2025_abril_csv.csv",
    "presupuesto_misiones_2025": "gs://tu_bucket_raw/mremh_presupuestoejecutadomisionesecuadorexterior_2025junio.csv",
    "catalogo_compras_2025": "gs://tu_bucket_raw/releases_2025_catalogo_electronico_compra_directa.json",

    # ----------------------------
    # 2. APIs automáticas
    # ----------------------------
    "api_clima": "https://api.open-meteo.com/v1/forecast?latitude=-2&longitude=-80&hourly=temperature_2m",
    "api_sismos_ec": "https://api.gael.cloud/general/public/sismos",  # API de sismos en Ecuador
    "api_sismos_usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&orderby=time&limit=50"  # Últimos 50 sismos globales
}

""" "ejemplo_csv": "https://raw.githubusercontent.com/ageron/handson-ml/master/datasets/housing/housing.csv",
    "ejemplo_json": "https://jsonplaceholder.typicode.com/posts" """

""" Ejemplo con API JSON:
    datasets = {
    "clima_hoy": "https://api.open-meteo.com/v1/forecast?latitude=-2&longitude=-80&hourly=temperature_2m"
}

¿Y si quiero datasets del portal de Datos Abiertos del Ecuador?
datasets = {
    "gastos_publicos": "https://www.datosabiertos.gob.ec/.../gastos_2024.csv",
    "presupuesto_educacion": "https://www.datosabiertos.gob.ec/.../edu_presupuesto.csv"
}
"""

def upload_to_gcp(local_path, bucket_name):
    client = storage.Client.from_service_account_json(
        config["gcp"]["credentials"]
    )
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)
    print(f"[GCP] Subido a gs://{bucket_name}/{blob.name}")

def guess_extension_from_url(url, response=None):
    path = urlparse(url).path.lower()
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".json") or (response and 'application/json' in response.headers.get('Content-Type', '')):
        return "json"
    return "bin"

def main():
    now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    for name, url in datasets.items():
        try:
            print(f"[INFO] Descargando {name}...")
            r = requests.get(url, timeout=60)
            r.raise_for_status()

            ext = guess_extension_from_url(url, r)
            filename = f"{name}_{now}.{ext}"
            local_path = os.path.join(DATA_DIR, filename)

            with open(local_path, "wb") as f:
                f.write(r.content)

            print(f"[OK] Guardado local: {local_path}")

            # Subir a GCP si modo == cloud
            if MODE == "cloud":
                upload_to_gcp(local_path, config["gcp"]["bucket_raw"])

        except Exception as e:
            print(f"[ERROR] {e}")
            
            # Resumen de archivos descargados
    print("\n[RESUMEN] Archivos descargados en data/raw/:")
    for f in os.listdir(DATA_DIR):
        print(f" - {f}")

if __name__ == "__main__":
    main()
