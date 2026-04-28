from pydantic import BaseModel

class Prediccion(BaseModel):
    fecha: str
    tienda_id: str
    producto_id: str
    categoria: str
    unidades_previstas: float
    intervalo_inferior: float | None = None
    intervalo_superior: float | None = None
    modelo: str | None = None

class MetricaModelo(BaseModel):
    modelo: str
    MAE: float
    RMSE: float
    R2: float
    estado: str | None = None

class RecomendacionStock(BaseModel):
    tienda_id: str
    producto_id: str
    categoria: str
    demanda_7d_prevista: float
    stock_actual: float
    stock_objetivo: float
    unidades_a_reponer: float
    prioridad: str
