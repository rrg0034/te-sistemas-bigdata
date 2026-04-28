from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .modelos import Prediccion, MetricaModelo, RecomendacionStock
from .servicios import consultar, resumen_demanda
from .conexion_mongo import mongo_disponible

app = FastAPI(
    title="SuperFresh Big Data API",
    description="API para consultar predicciones de demanda y recomendaciones de stock generadas con Spark y almacenadas en MongoDB.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "mongodb": "online" if mongo_disponible() else "fallback_csv",
        "mensaje": "API SuperFresh funcionando"
    }

@app.get("/metricas", response_model=list[MetricaModelo])
def obtener_metricas():
    return consultar("metricas_modelo", limite=20)

@app.get("/predicciones", response_model=list[Prediccion])
def obtener_predicciones(
    tienda_id: str | None = None,
    producto_id: str | None = None,
    categoria: str | None = None,
    limite: int = Query(100, ge=1, le=1000)
):
    filtro = {}
    if tienda_id:
        filtro["tienda_id"] = tienda_id
    if producto_id:
        filtro["producto_id"] = producto_id
    if categoria:
        filtro["categoria"] = categoria
    return consultar("predicciones", filtro, limite)

@app.get("/predicciones/{producto_id}", response_model=list[Prediccion])
def obtener_predicciones_producto(producto_id: str, limite: int = Query(100, ge=1, le=1000)):
    resultados = consultar("predicciones", {"producto_id": producto_id}, limite)
    if not resultados:
        raise HTTPException(status_code=404, detail="No hay predicciones para ese producto")
    return resultados

@app.get("/recomendaciones-stock", response_model=list[RecomendacionStock])
def obtener_recomendaciones_stock(
    prioridad: str | None = Query(None, pattern="^(ALTA|MEDIA|BAJA)$"),
    limite: int = Query(100, ge=1, le=500)
):
    filtro = {"prioridad": prioridad} if prioridad else {}
    return consultar("recomendaciones_stock", filtro, limite)

@app.get("/resumen-demanda")
def obtener_resumen_demanda():
    return resumen_demanda()
