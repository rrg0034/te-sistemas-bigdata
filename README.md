# Proyecto SuperFresh - Sistema Big Data de prediccion de ventas

Proyecto completo para el trabajo de enfoque de **Sistemas de Big Data**. El caso resuelve la necesidad de SuperFresh: predecir demanda futura, optimizar stock y visualizar resultados para ayudar al equipo de inventario.

## Herramientas usadas

- **Apache Spark / PySpark**: procesamiento distribuido, limpieza, union de fuentes, ingenieria de variables y entrenamiento de modelos.
- **Machine Learning**: comparativa de `RandomForestRegressor`, `GBTRegressor` y `LinearRegression`.
- **MongoDB**: almacenamiento de datos procesados, metricas, predicciones y recomendaciones de reposicion.
- **FastAPI**: API REST para consultar predicciones en tiempo real.
- **Streamlit + Plotly**: cuadro de mando ejecutable. Tambien se incluyen CSV y guia para importarlo en Power BI/Tableau.
- **Docker Compose**: arranque rapido de MongoDB y Mongo Express.

## Estructura

```text
SuperFresh_BigData/
├── api/                         # API FastAPI
├── dashboard/                   # Cuadro de mando Streamlit y HTML estatico
├── data/
│   ├── raw/                     # ventas, clima, promociones, eventos
│   ├── processed/               # muestra enriquecida
│   └── output/                  # predicciones, metricas y stock
├── docs/                        # arquitectura, guia Power BI/Tableau y rubrica
├── informe/                     # informe listo para entregar/adaptar
├── scripts/                     # generacion de datos y pipeline Spark
└── requirements.txt
```

## Puesta en marcha rapida

### 1. Crear entorno e instalar dependencias

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Conectar MongoDB

Crear un fichero .env para guardar la url del cluster mongo y el nombre de la bd:
MONGO_URI                         # Url del cluster
MONGO_BD                          # Nombre BD

```bash
Mongo Express queda disponible en `http://localhost:8081`.
```

### 3. Generar datos prueba

```bash
python scripts/01_generar_datos.py
```

### 4. Ejecutar pipeline Big Data con Spark

```bash
spark-submit scripts/02_pipeline_spark_mongodb.py
```

Este paso crea variables, entrena modelos, calcula MAE/RMSE/R2 y guarda resultados en MongoDB.

### 5. Ejecutar API

```bash
uvicorn api.main:app --reload
```

Abrir:

```text
http://127.0.0.1:8000/docs
```

Endpoints principales:

- `GET /health`
- `GET /metricas`
- `GET /predicciones`
- `GET /predicciones/{producto_id}`
- `GET /recomendaciones-stock`
- `GET /resumen-demanda`

### 6. Ejecutar dashboard Streamlit

```bash
streamlit run dashboard/app_streamlit.py
```

## Resultados incluidos

El proyecto ya trae salidas generadas en `data/output/` para poder enseñar resultados aunque no se ejecute Spark en el momento de la entrega:

- `metricas_modelo.csv`
- `predicciones.csv`
- `recomendaciones_stock.csv`

Resumen de metricas de ejemplo:

| Modelo | MAE | RMSE | R2 |
|---|---:|---:|---:|
| RandomForestRegressor | 3.82 | 4.775 | 0.9278 |
| GBTRegressor | 4.42 | 5.496 | 0.9044 |
| LinearRegression | 5.582 | 7.798 | 0.8074 |
