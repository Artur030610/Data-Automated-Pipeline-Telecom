import pandas as pd
import numpy as np
import sys
import os
import glob
import datetime

# --- CONFIGURACIÓN DE RUTAS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import (
    guardar_parquet, 
    reportar_tiempo, 
    console, 
    limpiar_nulos_powerbi, 
    obtener_rango_fechas
)

# ==========================================
# CONSTANTES DE NEGOCIO (IDF)
# ==========================================
NOC_USERS = [
    "GFARFAN", "JVELASQUEZ", "JOLUGO", "KUSEA", "SLOPEZ",
    "EDESPINOZA", "SANDYJIM", "JOCASTILLO", "JESUSGARCIA",
    "DFUENTES", "JOCANTO", "IXMONTILLA", "OMTRUJILLO",
    "LLZERPA", "LUIJIMENEZ"
]

EXCLUIR_SOLUCIONES = [
    "CAMBIO DE CLAVE", "CLIENTE SOLICITO REEMBOLSO", 
    "LLAMADAS DE AGENDAMIENTO", "ORDEN REPETIDA"
]

# Columnas que queremos en el detalle (Silver)
COLS_IDF_SILVER = [
    "Quincena Evaluada", "FechaInicio", "FechaFin",
    "N° Contrato", "N° Orden", 
    "Fecha Creacion", "Fecha Finalizacion", 
    "Franquicia", "Grupo Trabajo", "Usuario Final",
    "Solucion Aplicada", "Detalle Orden", 
    "Clasificacion", "Es_Falla" # Columna calculada
]

# ==========================================
# FUNCIONES LOCALES
# ==========================================
def limpiar_fechas_mixtas(series):
    if pd.api.types.is_datetime64_any_dtype(series): return series
    series_nums = pd.to_numeric(series, errors='coerce')
    mask_es_serial = (series_nums > 35000) & (series_nums < 60000)
    fechas_excel = pd.to_datetime(series_nums[mask_es_serial], unit='D', origin='1899-12-30')
    resto = series[~mask_es_serial].astype(str).str.strip()
    fechas_texto = pd.to_datetime(resto, dayfirst=True, errors='coerce')
    return fechas_excel.combine_first(fechas_texto)

# =============================================================================
# PIPELINE PRINCIPAL (IDF)
# =============================================================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]PIPELINE: ÍNDICE DE FALLA (SILVER & GOLD)[/]")

    ruta_origen = PATHS.get("raw_idf")
    ruta_silver = PATHS.get("silver")
    ruta_gold   = PATHS.get("gold")

    if not ruta_origen or not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: Ruta 'raw_idf' no existe[/]")
        return

    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    console.print(f"📂 Procesando {len(archivos)} archivos de fallas...")

    dataframes_procesados = []

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        if nombre_archivo.startswith("~$") or "Consolidado" in nombre_archivo: continue
        
        # 1. Obtener Fechas (Usando utils)
        fecha_inicio, fecha_fin, quincena_nombre = obtener_rango_fechas(nombre_archivo)
        if not fecha_inicio: continue

        try:
            df = pd.read_excel(archivo, engine="calamine")
            if df.empty: continue

            # 2. Limpieza de Fechas
            cols_fecha = ["Fecha Creacion", "Fecha Finalizacion"]
            for col in cols_fecha:
                if col in df.columns: df[col] = limpiar_fechas_mixtas(df[col])

            # 3. FILTROS DE TIEMPO (Crítico para IDF)
            if "Fecha Finalizacion" not in df.columns or "Fecha Creacion" not in df.columns:
                console.print(f"[red]⚠️ {nombre_archivo} sin columnas de fecha.[/]")
                continue

            # Regla A: El ticket se cerró en esta quincena
            mask_cierre = (df["Fecha Finalizacion"] >= fecha_inicio) & (df["Fecha Finalizacion"] <= fecha_fin)
            # Regla B: El ticket se creó DENTRO de esta quincena (No traemos backlog viejo)
            mask_creacion = df["Fecha Creacion"] >= fecha_inicio
            
            df = df[mask_cierre & mask_creacion].copy()
            if df.empty: continue

            # 4. Normalización
            df = df.assign(
                Solucion_Norm = lambda x: x["Solucion Aplicada"].fillna("").astype(str).str.upper() if "Solucion Aplicada" in x else "",
                Grupo_Norm    = lambda x: x["Grupo Trabajo"].fillna("").astype(str).str.upper() if "Grupo Trabajo" in x else "",
                Detalle_Norm  = lambda x: x["Detalle Orden"].fillna("").astype(str).str.upper() if "Detalle Orden" in x else "",
                Usuario_Norm  = lambda x: x["Usuario Final"].fillna("").astype(str).str.upper() if "Usuario Final" in x else "",
                Franquicia    = lambda x: x["Franquicia"].fillna("NO DEFINIDA").astype(str).str.strip().str.upper() if "Franquicia" in x else "NO DEFINIDA",
                FechaInicio   = fecha_inicio,
                FechaFin      = fecha_fin,
                Quincena_Evaluada = quincena_nombre,
                Es_Falla      = 1 # Marcador para sumar luego
            ).rename(columns={"Quincena_Evaluada": "Quincena Evaluada"})

            # 5. Clasificación (NOC vs MESA vs OPERACIONES)
            condiciones = [
                df['Usuario_Norm'].isin(NOC_USERS),
                df['Grupo_Norm'].isin(NOC_USERS),
                df['Usuario_Norm'].str.contains('NOC', na=False),
                df['Grupo_Norm'].str.contains('OPERACIONES', na=False)
            ]
            opciones = ['NOC', 'NOC', 'NOC', 'OPERACIONES']
            df['Clasificacion'] = np.select(condiciones, opciones, default='MESA DE CONTROL')

            # 6. Filtros de Exclusión (Basura operativa)
            mask_excluir = (
                df["Solucion_Norm"].isin(EXCLUIR_SOLUCIONES) |
                df["Grupo_Norm"].str.contains("GT API FIBEX", na=False) |
                (df["Detalle_Norm"] == "PRUEBA DE INTERNET")
            )
            df_limpio = df[~mask_excluir].copy()

            # Selección columnas Silver
            cols_existentes = [c for c in COLS_IDF_SILVER if c in df_limpio.columns]
            dataframes_procesados.append(df_limpio[cols_existentes])

            console.print(f"   ✅ {nombre_archivo} -> {quincena_nombre}: {len(df_limpio)} fallas")

        except Exception as e:
            console.print(f"   ❌ Error en {nombre_archivo}: {e}")

    if not dataframes_procesados:
        console.print("[yellow]⚠️ No se encontraron tickets de falla válidos.[/]")
        return

    # ---------------------------------------------------------------------
    # CAPA SILVER (DETALLE DE FALLAS)
    # ---------------------------------------------------------------------
    console.rule("[cyan]GENERANDO IDF SILVER (DETALLE)[/]")
    
    df_silver = pd.concat(dataframes_procesados, ignore_index=True)
    df_silver = df_silver.drop_duplicates()
    df_silver = limpiar_nulos_powerbi(df_silver)
    
    guardar_parquet(
        df=df_silver, 
        nombre_archivo="IDF_Fallas_Silver_Detalle.parquet",
        filas_iniciales=len(df_silver),
        ruta_destino=ruta_silver
    )

    # ---------------------------------------------------------------------
    # CAPA GOLD (RESUMEN POR FRANQUICIA)
    # ---------------------------------------------------------------------
    console.rule("[gold3]GENERANDO IDF GOLD (RESUMEN)[/]")
    
    # Agrupamos fallas por Quincena y Franquicia
    df_gold = df_silver.groupby(
        ["Quincena Evaluada", "Franquicia"], 
        as_index=False
    ).agg(
        Total_Fallas=("Es_Falla", "sum"),     # Suma de tickets
        Fecha_Corte=("FechaFin", "max")
    )
    
    df_gold = df_gold.sort_values(by=["Quincena Evaluada", "Franquicia"])

    guardar_parquet(
        df=df_gold, 
        nombre_archivo="IDF_Fallas_Gold_Resumen.parquet", 
        filas_iniciales=len(df_gold),
        ruta_destino=ruta_gold
    )

if __name__ == "__main__":
    ejecutar()