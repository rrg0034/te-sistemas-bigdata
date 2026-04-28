from pathlib import Path
import os
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, dayofweek, month, year, lag, avg, when, lit, to_date, round as spark_round
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

load_dotenv()

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
OUTPUT = BASE / "data" / "output"
OUTPUT.mkdir(parents=True, exist_ok=True)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

spark = (
    SparkSession.builder
    .appName("SuperFresh Big Data - Prediccion ventas")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def leer_csv(nombre):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(RAW / nombre))

ventas = leer_csv("ventas_superfresh.csv")
clima = leer_csv("clima_superfresh.csv")
promos = leer_csv("promociones_superfresh.csv")
eventos = leer_csv("eventos_superfresh.csv")

ventas = ventas.withColumn("fecha", to_date(col("fecha")))
clima = clima.withColumn("fecha", to_date(col("fecha")))
promos = promos.withColumn("fecha", to_date(col("fecha")))
eventos = eventos.withColumn("fecha", to_date(col("fecha")))

# Union de fuentes: ventas + clima + promociones + eventos.
datos = (
    ventas
    .join(clima.select("fecha", "tienda_id", "temperatura_media", "lluvia_mm"), ["fecha", "tienda_id"], "left")
    .join(promos.select("fecha", "producto_id", "promocion_activa", "descuento"), ["fecha", "producto_id"], "left")
    .join(eventos.select("fecha", "ciudad", "nombre_evento", "impacto_estimado"), ["fecha", "ciudad"], "left")
)

# Ingenieria de variables temporales, promocionales y externas.
ventana = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")
datos = (
    datos
    .withColumn("anio", year(col("fecha")))
    .withColumn("mes", month(col("fecha")))
    .withColumn("dia_semana", dayofweek(col("fecha")))
    .withColumn("es_fin_semana", when(col("dia_semana").isin([1, 7]), 1).otherwise(0))
    .withColumn("ventas_lag_7", lag("unidades_vendidas", 7).over(ventana))
    .withColumn("media_ventas_7d", avg("unidades_vendidas").over(ventana.rowsBetween(-7, -1)))
    .fillna({
        "promocion_activa": 0,
        "descuento": 0.0,
        "impacto_estimado": 0.0,
        "lluvia_mm": 0.0,
        "ventas_lag_7": 0.0,
        "media_ventas_7d": 0.0
    })
)

indexadores = [
    StringIndexer(inputCol="producto_id", outputCol="producto_idx", handleInvalid="keep"),
    StringIndexer(inputCol="tienda_id", outputCol="tienda_idx", handleInvalid="keep"),
    StringIndexer(inputCol="categoria", outputCol="categoria_idx", handleInvalid="keep"),
]

features = [
    "producto_idx", "tienda_idx", "categoria_idx", "anio", "mes", "dia_semana",
    "es_fin_semana", "temperatura_media", "lluvia_mm", "promocion_activa",
    "descuento", "impacto_estimado", "ventas_lag_7", "media_ventas_7d", "stock_actual"
]

assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="keep")

train = datos.filter(col("fecha") < lit("2025-10-01"))
test = datos.filter(col("fecha") >= lit("2025-10-01"))

modelos = {
    "RandomForestRegressor": RandomForestRegressor(labelCol="unidades_vendidas", featuresCol="features", numTrees=90, maxDepth=9, seed=42),
    "GBTRegressor": GBTRegressor(labelCol="unidades_vendidas", featuresCol="features", maxIter=45, maxDepth=6, seed=42),
    "LinearRegression": LinearRegression(labelCol="unidades_vendidas", featuresCol="features")
}

evaluadores = {
    "MAE": RegressionEvaluator(labelCol="unidades_vendidas", predictionCol="prediction", metricName="mae"),
    "RMSE": RegressionEvaluator(labelCol="unidades_vendidas", predictionCol="prediction", metricName="rmse"),
    "R2": RegressionEvaluator(labelCol="unidades_vendidas", predictionCol="prediction", metricName="r2")
}

resultados = []
mejor_nombre = None
mejor_rmse = None
mejor_modelo = None
mejores_predicciones = None

for nombre, estimador in modelos.items():
    pipeline = Pipeline(stages=indexadores + [assembler, estimador])
    modelo = pipeline.fit(train)
    predicciones = modelo.transform(test)
    mae = evaluadores["MAE"].evaluate(predicciones)
    rmse = evaluadores["RMSE"].evaluate(predicciones)
    r2 = evaluadores["R2"].evaluate(predicciones)
    resultados.append({"modelo": nombre, "MAE": round(mae, 3), "RMSE": round(rmse, 3), "R2": round(r2, 4), "fecha_entrenamiento": datetime.now().isoformat(timespec="seconds")})
    if mejor_rmse is None or rmse < mejor_rmse:
        mejor_nombre = nombre
        mejor_rmse = rmse
        mejor_modelo = modelo
        mejores_predicciones = predicciones

# Exportacion de metricas y predicciones recientes.
metricas_df = spark.createDataFrame(resultados)
metricas_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT / "metricas_modelo_spark"))

predicciones_export = (
    mejores_predicciones
    .select(
        col("fecha").cast("string").alias("fecha"),
        "tienda_id", "producto_id", "categoria",
        spark_round(col("prediction"), 0).alias("unidades_previstas"),
        spark_round(col("prediction") * 0.82, 0).alias("intervalo_inferior"),
        spark_round(col("prediction") * 1.18, 0).alias("intervalo_superior"),
    )
    .withColumn("modelo", lit(mejor_nombre))
)

predicciones_export.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT / "predicciones_spark"))

# Recomendacion de reposicion: demanda prevista de los primeros 7 dias evaluados vs stock actual.
from pyspark.sql.functions import date_add, min as spark_min
fecha_minima = predicciones_export.select(to_date(col("fecha")).alias("fecha_date")).agg(spark_min("fecha_date").alias("fecha_min")).collect()[0]["fecha_min"]
predicciones_7d = predicciones_export.filter(to_date(col("fecha")) <= date_add(lit(fecha_minima), 6))

recomendaciones = (
    predicciones_7d
    .groupBy("tienda_id", "producto_id", "categoria")
    .agg({"unidades_previstas": "sum"})
    .withColumnRenamed("sum(unidades_previstas)", "demanda_7d_prevista")
    .withColumn("stock_objetivo", spark_round(col("demanda_7d_prevista") * 1.20, 0))
)

stock_actual = (
    datos
    .groupBy("tienda_id", "producto_id")
    .agg({"stock_actual": "max"})
    .withColumnRenamed("max(stock_actual)", "stock_actual")
)

recomendaciones = (
    recomendaciones
    .join(stock_actual, ["tienda_id", "producto_id"], "left")
    .withColumn("unidades_a_reponer", when(col("stock_objetivo") > col("stock_actual"), col("stock_objetivo") - col("stock_actual")).otherwise(0))
    .withColumn("prioridad", when(col("unidades_a_reponer") > col("demanda_7d_prevista") * 0.55, "ALTA").when(col("unidades_a_reponer") > 0, "MEDIA").otherwise("BAJA"))
)

recomendaciones.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT / "recomendaciones_stock_spark"))

# Carga en MongoDB.
cliente = MongoClient(MONGO_URI)
db = cliente[MONGO_DB]

def reemplazar_coleccion(nombre, filas):
    db[nombre].delete_many({})
    if filas:
        db[nombre].insert_many(filas)

reemplazar_coleccion("metricas_modelo", [fila.asDict() for fila in metricas_df.collect()])
reemplazar_coleccion("predicciones", [fila.asDict() for fila in predicciones_export.limit(5000).collect()])
reemplazar_coleccion("recomendaciones_stock", [fila.asDict() for fila in recomendaciones.collect()])

print("Pipeline finalizado")
print(f"Modelo seleccionado: {mejor_nombre}")
print(f"Resultados guardados en MongoDB: {MONGO_DB}")

spark.stop()
