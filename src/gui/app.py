import streamlit as st
import pandas as pd
import glob

st.title("Dashboard de Datos — Tesis Analítica")

files = glob.glob("data/processed/*.parquet")

if not files:
    st.warning("No hay archivos procesados aún. Ejecuta el ETL primero.")
else:
    choice = st.selectbox("Selecciona un dataset procesado:", files)

    if choice:
        df = pd.read_parquet(choice)
        st.write("Vista previa:")
        st.dataframe(df.head())

        if "median_house_value" in df.columns:
            st.line_chart(df["median_house_value"])
