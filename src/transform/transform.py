import duckdb
import glob
import os

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def process():
    con = duckdb.connect()

    files = glob.glob(os.path.join(RAW_DIR, "*"))
    
    for f in files:
        fname = os.path.basename(f)

        if fname.endswith(".csv"):
            out = os.path.join(OUT_DIR, fname.replace(".csv", ".parquet"))
            con.execute(f"COPY (SELECT * FROM read_csv_auto('{f}')) TO '{out}' (FORMAT PARQUET)")
            print(f"[OK] CSV -> Parquet: {out}")

        elif fname.endswith(".json"):
            out = os.path.join(OUT_DIR, fname.replace(".json", ".parquet"))
            con.execute(f"COPY (SELECT * FROM read_json_auto('{f}')) TO '{out}' (FORMAT PARQUET)")
            print(f"[OK] JSON -> Parquet: {out}")

        else:
            print(f"[SKIP] Tipo no soportado: {fname}")

if __name__ == "__main__":
    process()
