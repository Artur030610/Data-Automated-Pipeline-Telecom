import polars as pl
import duckdb

from utils import ingesta_incremental_polars
from config import PATHS

ruta_raw = PATHS.get("raw_churn_risk")
ruta_bronze = PATHS.get("bronze")

ingesta_incremental_polars(ruta_raw, ruta_bronze, "Fecha Contrato")