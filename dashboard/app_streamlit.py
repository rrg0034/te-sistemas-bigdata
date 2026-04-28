from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import os

BASE = Path(__file__).resolve().parents[1]
OUTPUT = BASE / "data" / "output"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "superfresh_bigdata")

st.set_page_config(page_title="SuperFresh Dashboard", layout="wide")

@st.cache_data(ttl=60)
def cargar_datos(nombre_coleccion, archivo):
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
        cliente.admin.command("ping")
        datos = list(cliente[MONGO_DB][nombre_coleccion].find({}, {"_id": 0}))
        if datos:
            return pd.DataFrame(datos)
    except ServerSelectionTimeoutError:
        pass
    return pd.read_csv(OUTPUT / archivo)

predicciones = cargar_datos("predicciones", "predicciones.csv")
metricas = cargar_datos("metricas_modelo", "metricas_modelo.csv")
stock = cargar_datos("recomendaciones_stock", "recomendaciones_stock.csv")

st.title("SuperFresh - Cuadro de mando de demanda e inventario")
st.caption("Predicciones generadas con pipeline Big Data: fuentes historicas + clima + promociones + eventos.")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Unidades previstas", f"{predicciones['unidades_previstas'].sum():,.0f}")
col2.metric("Productos", predicciones["producto_id"].nunique())
col3.metric("Tiendas", predicciones["tienda_id"].nunique())
col4.metric("Reposicion total", f"{stock['unidades_a_reponer'].sum():,.0f}")

st.sidebar.header("Filtros")
tiendas = sorted(predicciones["tienda_id"].unique())
productos = sorted(predicciones["producto_id"].unique())
seleccion_tienda = st.sidebar.multiselect("Tienda", tiendas, default=tiendas)
seleccion_producto = st.sidebar.multiselect("Producto", productos, default=productos[:5])

filtro = predicciones[predicciones["tienda_id"].isin(seleccion_tienda) & predicciones["producto_id"].isin(seleccion_producto)].copy()

st.subheader("Evolucion prevista de la demanda")
fig_linea = px.line(filtro, x="fecha", y="unidades_previstas", color="producto_id", line_group="tienda_id", markers=True)
st.plotly_chart(fig_linea, use_container_width=True)

col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Demanda por categoria")
    categoria = predicciones.groupby("categoria", as_index=False)["unidades_previstas"].sum().sort_values("unidades_previstas", ascending=False)
    st.plotly_chart(px.bar(categoria, x="categoria", y="unidades_previstas"), use_container_width=True)

with col_b:
    st.subheader("Comparativa de modelos")
    st.plotly_chart(px.bar(metricas, x="modelo", y=["MAE", "RMSE"], barmode="group"), use_container_width=True)
    st.dataframe(metricas, use_container_width=True)

st.subheader("Recomendaciones de reposicion")
prioridad = st.selectbox("Prioridad", ["Todas", "ALTA", "MEDIA", "BAJA"])
stock_filtrado = stock if prioridad == "Todas" else stock[stock["prioridad"] == prioridad]
st.dataframe(stock_filtrado.sort_values("unidades_a_reponer", ascending=False), use_container_width=True)
