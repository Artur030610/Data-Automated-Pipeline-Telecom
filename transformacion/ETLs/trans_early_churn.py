import polars as pl
import duckdb
import os 
from utils import ingesta_incremental_polars
from config import PATHS

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)  # Sube el segundo nivel

ruta_raw = PATHS.get("raw_churn_risk")
ruta_bronze = PATHS.get("bronze")

ingesta_incremental_polars(ruta_raw, ruta_bronze, "Fecha Contrato")