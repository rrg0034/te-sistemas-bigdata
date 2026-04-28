# Guia rapida para montar el cuadro de mando en Power BI o Tableau

El proyecto incluye dashboard en Streamlit para que sea ejecutable sin licencias. Si el docente exige Power BI o Tableau, usa estos ficheros:

- `data/output/predicciones.csv`
- `data/output/metricas_modelo.csv`
- `data/output/recomendaciones_stock.csv`
- `data/raw/ventas_superfresh.csv`

## Power BI

1. Abrir Power BI Desktop.
2. Obtener datos > Texto/CSV.
3. Importar los cuatro CSV anteriores.
4. Crear estas visualizaciones:
   - Tarjeta: suma de `unidades_previstas`.
   - Grafico de lineas: `fecha` vs `unidades_previstas`, leyenda por `producto_id`.
   - Grafico de barras: demanda por `categoria`.
   - Tabla: `recomendaciones_stock.csv` ordenada por `unidades_a_reponer`.
   - Grafico de barras agrupadas: `MAE` y `RMSE` por modelo.
5. Medidas DAX sugeridas:

```DAX
Demanda prevista = SUM(predicciones[unidades_previstas])
Reposicion total = SUM(recomendaciones_stock[unidades_a_reponer])
Productos activos = DISTINCTCOUNT(predicciones[producto_id])
Tiendas activas = DISTINCTCOUNT(predicciones[tienda_id])
```

## Tableau

1. Connect > Text file.
2. Cargar los CSV.
3. Crear hojas equivalentes: KPIs, demanda temporal, categoria, metricas y reposicion.
4. Crear un dashboard con filtros de tienda, producto y categoria.
