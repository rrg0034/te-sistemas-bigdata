from pathlib import Path
import csv
from typing import Any

from .conexion_mongo import obtener_bd, mongo_disponible

BASE = Path(__file__).resolve().parents[1]
OUTPUT = BASE / "data" / "output"

COLECCION_ARCHIVO = {
    "predicciones": OUTPUT / "predicciones.csv",
    "metricas_modelo": OUTPUT / "metricas_modelo.csv",
    "recomendaciones_stock": OUTPUT / "recomendaciones_stock.csv",
}

def convertir(valor: str) -> Any:
    if valor is None:
        return valor
    texto = str(valor).strip()
    if texto == "":
        return None
    try:
        if "." in texto:
            return float(texto)
        return int(texto)
    except ValueError:
        return texto

def leer_csv(nombre: str):
    ruta = COLECCION_ARCHIVO[nombre]
    if not ruta.exists():
        return []
    with open(ruta, encoding="utf-8") as f:
        return [{k: convertir(v) for k, v in fila.items()} for fila in csv.DictReader(f)]

def consultar(nombre: str, filtro: dict | None = None, limite: int = 100):
    filtro = filtro or {}
    if mongo_disponible():
        bd = obtener_bd()
        return list(bd[nombre].find(filtro, {"_id": 0}).limit(limite))

    filas = leer_csv(nombre)
    for campo, valor in filtro.items():
        filas = [fila for fila in filas if str(fila.get(campo)) == str(valor)]
    return filas[:limite]

def resumen_demanda():
    filas = consultar("predicciones", limite=10000)
    resumen = {}
    for fila in filas:
        categoria = fila.get("categoria", "Sin categoria")
        resumen[categoria] = resumen.get(categoria, 0) + float(fila.get("unidades_previstas", 0))
    return [{"categoria": k, "unidades_previstas": round(v, 2)} for k, v in sorted(resumen.items(), key=lambda x: x[1], reverse=True)]
