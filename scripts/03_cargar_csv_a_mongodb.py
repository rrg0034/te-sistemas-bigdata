from pathlib import Path
import csv
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
BASE = Path(__file__).resolve().parents[1]
OUTPUT = BASE / "data" / "output"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

colecciones = {
    "metricas_modelo": OUTPUT / "metricas_modelo.csv",
    "predicciones": OUTPUT / "predicciones.csv",
    "recomendaciones_stock": OUTPUT / "recomendaciones_stock.csv",
}

def convertir(valor):
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

cliente = MongoClient(MONGO_URI)
db = cliente[MONGO_DB]

for nombre, ruta in colecciones.items():
    with open(ruta, encoding="utf-8") as f:
        filas = [{k: convertir(v) for k, v in fila.items()} for fila in csv.DictReader(f)]
    db[nombre].delete_many({})
    if filas:
        db[nombre].insert_many(filas)
    print(f"Coleccion {nombre}: {len(filas)} documentos cargados")
