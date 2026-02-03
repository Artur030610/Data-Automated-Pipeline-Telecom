import pandas as pd
from config import PATHS
import numpy as np
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console


@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]15. ETL: VUELVE A CASA (CBH)[/]")
    
    # 1. Definición de Columnas Esperadas
    cols_esperadas = [
        "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha Llamada", "Hora Llamada", "Tipo Respuesta",
          "Detalle Respuesta", "Responsable", "Suscripción", 
          "Grupo Afinidad", "Franquicia", "Ciudad"]

    # 2. Lectura
    df = leer_carpeta(
        PATHS["raw_comeback"],  filtro_exclusion="Consolidado",
        columnas_esperadas=cols_esperadas)
    
    filas_raw = len(df)

    df = df.drop_duplicates(subset=[
        "N° Abonado", "Documento", "Fecha", "Hora", "Responsable"])
    # 1. Convertimos strings vacíos o con espacios a NaN real
    df["Fecha"] = df["Fecha"].replace(r'^\s*$', np.nan, regex=True)

    df = df[df['Fecha'].notna()]
    
    guardar_parquet(
        df, "CBH_Gold.parquet", PATHS["gold"], filas_iniciales=filas_raw)