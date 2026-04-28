from pathlib import Path
import csv
import random
import math
from datetime import date, timedelta

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
PROCESSED = BASE / "data" / "processed"
OUTPUT = BASE / "data" / "output"
for carpeta in (RAW, PROCESSED, OUTPUT):
    carpeta.mkdir(parents=True, exist_ok=True)

random.seed(42)

productos = [
    {"producto_id":"P001","producto":"Agua mineral 1.5L","categoria":"Bebidas","base":54,"precio":0.75},
    {"producto_id":"P002","producto":"Refresco cola 2L","categoria":"Bebidas","base":38,"precio":1.65},
    {"producto_id":"P003","producto":"Leche entera 1L","categoria":"Lacteos","base":45,"precio":1.05},
    {"producto_id":"P004","producto":"Yogur natural pack","categoria":"Lacteos","base":29,"precio":2.20},
    {"producto_id":"P005","producto":"Pan de molde","categoria":"Panaderia","base":36,"precio":1.35},
    {"producto_id":"P006","producto":"Croissants bolsa","categoria":"Panaderia","base":22,"precio":2.10},
    {"producto_id":"P007","producto":"Manzana golden kg","categoria":"Fruta","base":31,"precio":2.05},
    {"producto_id":"P008","producto":"Tomate ensalada kg","categoria":"Verdura","base":28,"precio":2.30},
    {"producto_id":"P009","producto":"Pechuga pollo kg","categoria":"Carne","base":24,"precio":6.80},
    {"producto_id":"P010","producto":"Pizza congelada","categoria":"Congelados","base":18,"precio":3.10},
    {"producto_id":"P011","producto":"Paraguas plegable","categoria":"Bazar","base":5,"precio":6.50},
    {"producto_id":"P012","producto":"Turron chocolate","categoria":"Dulces","base":7,"precio":2.95},
]

tiendas = [
    {"tienda_id":"T01","tienda":"SuperFresh Centro","ciudad":"Malaga","factor":1.18},
    {"tienda_id":"T02","tienda":"SuperFresh Norte","ciudad":"Sevilla","factor":1.08},
    {"tienda_id":"T03","tienda":"SuperFresh Costa","ciudad":"Almeria","factor":0.96},
    {"tienda_id":"T04","tienda":"SuperFresh Sierra","ciudad":"Granada","factor":0.91},
    {"tienda_id":"T05","tienda":"SuperFresh Sur","ciudad":"Cordoba","factor":1.02},
]

def rango_fechas(inicio, fin):
    actual = inicio
    while actual <= fin:
        yield actual
        actual += timedelta(days=1)

def factor_estacional(categoria, mes):
    if categoria == "Bebidas":
        return 1.35 if mes in (6, 7, 8, 9) else 0.92 if mes in (12, 1, 2) else 1.0
    if categoria == "Dulces":
        return 2.70 if mes == 12 else 1.45 if mes in (11, 1) else 0.65
    if categoria == "Bazar":
        return 1.65 if mes in (10, 11, 12, 1, 2, 3) else 0.80
    if categoria == "Fruta":
        return 1.20 if mes in (5, 6, 7, 8) else 0.95
    if categoria == "Congelados":
        return 1.12 if mes in (6, 7, 8) else 1.0
    return 1.0

def generar_clima(ciudad, dia):
    base_temp = {"Malaga": 18, "Sevilla": 20, "Almeria": 19, "Granada": 15, "Cordoba": 19}[ciudad]
    amplitud = {"Malaga": 8, "Sevilla": 13, "Almeria": 9, "Granada": 12, "Cordoba": 13}[ciudad]
    temp = base_temp + amplitud * math.sin((dia.month - 3) / 12 * 2 * math.pi) + random.gauss(0, 2.2)
    prob_lluvia = 0.18 if dia.month in (10, 11, 12, 1, 2, 3) else 0.06
    if ciudad in ("Granada", "Malaga"):
        prob_lluvia += 0.03
    lluvia = max(0, round(random.random() < prob_lluvia) * random.uniform(1.5, 18.0))
    return round(temp, 1), round(lluvia, 1)

def generar_evento(ciudad, dia):
    if dia.month == 12 and dia.day in range(20, 32):
        return "Campana Navidad", 0.30
    if dia.month == 8 and dia.day in range(10, 18) and ciudad == "Malaga":
        return "Feria local", 0.35
    if dia.weekday() in (4, 5) and random.random() < 0.035:
        return "Evento fin de semana", random.uniform(0.10, 0.22)
    return "Sin evento", 0.0

def main():
    promociones = {}
    with open(RAW / "promociones_superfresh.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "producto_id", "promocion_activa", "descuento", "tipo_promocion"])
        for dia in rango_fechas(date(2024, 1, 1), date(2025, 12, 31)):
            for producto in productos:
                prob = 0.09
                if producto["categoria"] in ("Bebidas", "Dulces") and dia.month in (7, 8, 12):
                    prob = 0.16
                activa = 1 if random.random() < prob else 0
                descuento = round(random.choice([0.10, 0.15, 0.20, 0.25]), 2) if activa else 0.0
                tipo = random.choice(["2x1", "descuento_directo", "segunda_unidad"]) if activa else "sin_promocion"
                promociones[(dia.isoformat(), producto["producto_id"])] = (activa, descuento, tipo)
                writer.writerow([dia.isoformat(), producto["producto_id"], activa, descuento, tipo])

    clima = {}
    with open(RAW / "clima_superfresh.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "tienda_id", "ciudad", "temperatura_media", "lluvia_mm"])
        for dia in rango_fechas(date(2024, 1, 1), date(2025, 12, 31)):
            for tienda in tiendas:
                temp, lluvia = generar_clima(tienda["ciudad"], dia)
                clima[(dia.isoformat(), tienda["tienda_id"])] = (temp, lluvia)
                writer.writerow([dia.isoformat(), tienda["tienda_id"], tienda["ciudad"], temp, lluvia])

    eventos = {}
    with open(RAW / "eventos_superfresh.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "ciudad", "nombre_evento", "impacto_estimado"])
        ciudades = sorted({t["ciudad"] for t in tiendas})
        for dia in rango_fechas(date(2024, 1, 1), date(2025, 12, 31)):
            for ciudad in ciudades:
                nombre, impacto = generar_evento(ciudad, dia)
                eventos[(dia.isoformat(), ciudad)] = (nombre, round(impacto, 3))
                writer.writerow([dia.isoformat(), ciudad, nombre, round(impacto, 3)])

    with open(RAW / "ventas_superfresh.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "tienda_id", "tienda", "ciudad", "producto_id", "producto", "categoria", "unidades_vendidas", "precio_unitario", "ingresos", "stock_actual"])
        indice = 0
        for dia in rango_fechas(date(2024, 1, 1), date(2025, 12, 31)):
            for tienda in tiendas:
                temp, lluvia = clima[(dia.isoformat(), tienda["tienda_id"])]
                _, impacto = eventos[(dia.isoformat(), tienda["ciudad"])]
                for producto in productos:
                    activa, descuento, _ = promociones[(dia.isoformat(), producto["producto_id"])]
                    factor_dia = 1.10 if dia.weekday() in (4, 5) else 0.93 if dia.weekday() == 6 else 1.0
                    factor_promo = 1 + descuento * 1.7
                    factor_clima = 1.0
                    if producto["categoria"] == "Bebidas":
                        factor_clima += max(temp - 24, 0) * 0.025
                    if producto["categoria"] == "Bazar":
                        factor_clima += min(lluvia, 20) * 0.035
                    if producto["categoria"] in ("Panaderia", "Dulces") and temp < 12:
                        factor_clima += 0.08
                    tendencia = 1 + indice * 0.0000025
                    ruido = random.gauss(1, 0.13)
                    unidades = max(0, round(producto["base"] * tienda["factor"] * factor_dia * factor_estacional(producto["categoria"], dia.month) * factor_promo * factor_clima * (1 + impacto) * tendencia * ruido))
                    stock = max(0, round(unidades * random.uniform(0.8, 2.8)))
                    ingresos = round(unidades * producto["precio"] * (1 - descuento), 2)
                    writer.writerow([dia.isoformat(), tienda["tienda_id"], tienda["tienda"], tienda["ciudad"], producto["producto_id"], producto["producto"], producto["categoria"], unidades, producto["precio"], ingresos, stock])
                    indice += 1

    print("Datos generados correctamente en data/raw")

if __name__ == "__main__":
    main()
