import pandas as pd
import numpy as np
import sys
import os
import glob
import re
import datetime
import calendar
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed

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
    archivos_raw # <-- Importación confirmada
)

# --- CONFIGURACIÓN GLOBAL ---
MAPEO_COLUMNAS = {
    "Nombre Franquicia": "Franquicia",
    "Franquicia": "Franquicia",
    "ID": "ID",
    "Id": "ID",
    "id": "ID",
    "ID_CLIENTE": "ID",
    "Estatus contrato": "Estatus contrato",
    "Estatus": "Estatus contrato"
}

COLS_ABONADOS_SILVER = [
    "Quincena Evaluada", "FechaInicio", "FechaFin",
    "ID", "Estatus contrato",
    "Franquicia" 
]

PATRON_FECHA = re.compile(r"hasta el.*?(\d{1,2}\W\d{1,2}\W\d{4})", re.IGNORECASE)

# ==========================================
# LÓGICA DE NEGOCIO (FECHAS SNAPSHOT)
# ==========================================
def obtener_fecha_corte_snapshot(nombre_archivo):
    try:
        nombre_limpio = os.path.basename(nombre_archivo).lower()
        match = PATRON_FECHA.search(nombre_limpio)
        
        if not match: 
            return None, None, None
        
        fecha_str = re.sub(r"\W", "-", match.group(1))
        fecha_archivo = pd.to_datetime(fecha_str, format="%d-%m-%Y")
        
        dia, mes, anio = fecha_archivo.day, fecha_archivo.month, fecha_archivo.year
        
        if dia <= 5:
            fecha_target = fecha_archivo.replace(day=1) - datetime.timedelta(days=1)
            quincena_str = "Q2"
        elif dia <= 20:
            fecha_target = fecha_archivo.replace(day=15)
            quincena_str = "Q1"
        else:
            ultimo_dia = calendar.monthrange(anio, mes)[1]
            fecha_target = fecha_archivo.replace(day=ultimo_dia)
            quincena_str = "Q2"

        meses = ["", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
                 "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
        
        nombre_etiqueta = f"{meses[fecha_target.month]} {fecha_target.year} {quincena_str}"
        
        return fecha_target, fecha_target, nombre_etiqueta

    except Exception as e:
        console.print(f"[red]Error calc fecha: {e}[/]")
        return None, None, None

# ==========================================
# WORKER: PROCESAR UN SOLO ARCHIVO
# ==========================================
def procesar_archivo_worker(ruta_completa):
    nombre_archivo = os.path.basename(ruta_completa)
    fecha_inicio, fecha_fin, quincena_nombre = obtener_fecha_corte_snapshot(nombre_archivo)
    
    if not fecha_inicio:
        return False, f"[yellow]⚠️ Sin fecha válida (Regex): {nombre_archivo}[/]", None

    try:
        df = pd.read_excel(ruta_completa, engine="calamine")
        if df.empty: 
            return False, f"[dim]⚠️ Archivo vacío: {nombre_archivo}[/]", None

        df.columns = df.columns.astype(str).str.strip()
        df = df.rename(columns=MAPEO_COLUMNAS)

        if "Franquicia" not in df.columns:
            return False, f"[red]❌ Falta columna Franquicia: {nombre_archivo}[/]", None

        df_small = pd.DataFrame()
        df_small["ID"] = df["ID"] if "ID" in df.columns else None
        df_small["Estatus contrato"] = df["Estatus contrato"] if "Estatus contrato" in df.columns else None
        df_small["Franquicia"] = df["Franquicia"].fillna("NO DEFINIDA").astype(str).str.strip().str.upper()
        
        df_small["Quincena Evaluada"] = quincena_nombre
        df_small["FechaInicio"] = fecha_inicio
        df_small["FechaFin"] = fecha_fin

        col_detalle = next((c for c in ["Detalle Orden", "Detalle"] if c in df.columns), None)
        if col_detalle:
            mask_pruebas = df[col_detalle] == "PRUEBA DE INTERNET"
            df_small = df_small[~mask_pruebas]

        del df
        gc.collect()

        cols_finales = [c for c in COLS_ABONADOS_SILVER if c in df_small.columns]
        return True, f"   ✅ {nombre_archivo} -> {quincena_nombre}", df_small[cols_finales].copy()

    except Exception as e:
        return False, f"[red]❌ Error crítico en {nombre_archivo}: {e}[/]", None

# ==========================================
# ORQUESTADOR PARALELO
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold cyan]PIPELINE: ABONADOS (SNAPSHOTS)[/]")

    # 1. Resolución de Rutas
    ruta_origen = PATHS.get("raw_abonados_idf")
    if not ruta_origen or not os.path.exists(ruta_origen):
        # Fallback hardcodeado que tienes en tu script
        ruta_origen = r"C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\raw_data\5-Indice de falla\2-Abonados"
    
    if not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: No se encuentra la ruta: {ruta_origen}[/]")
        return

    # -------------------------------------------------------------------------
    # PASO NUEVO: GENERAR CAPA BRONZE (Respaldo Crudo Consolidado)
    # -------------------------------------------------------------------------
    ruta_bronze_salida = os.path.join(PATHS.get("bronze", "data/bronze"), "Stock_Abonados_Raw_Bronze.parquet")
    try:
        # Polars consolidará todos los snapshots en un solo Parquet en segundos
        archivos_raw(ruta_origen, ruta_bronze_salida)
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se pudo generar: {e}[/]")
    # -------------------------------------------------------------------------

    ruta_silver = PATHS.get("silver")
    ruta_gold   = PATHS.get("gold")

    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    archivos = [f for f in archivos if not os.path.basename(f).startswith("~$")]
    
    console.print(f"📂 Ruta: {ruta_origen}")
    console.print(f"🚀 Iniciando procesamiento paralelo de {len(archivos)} archivos...")

    dataframes_list = []
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(procesar_archivo_worker, archivo): archivo for archivo in archivos}
        
        for future in as_completed(futures):
            exito, mensaje, df_result = future.result()
            console.print(mensaje)
            
            if exito and df_result is not None:
                dataframes_list.append(df_result)
                gc.collect()

    if not dataframes_list:
        console.print("[bold red]⛔ No se generaron datos.[/]")
        return

    # --- CONSOLIDACIÓN SILVER/GOLD ---
    console.print(f"\n🔄 Consolidando {len(dataframes_list)} DataFrames...")
    
    df_silver = pd.concat(dataframes_list, ignore_index=True)
    df_silver = df_silver.drop_duplicates()
    df_silver = limpiar_nulos_powerbi(df_silver)

    guardar_parquet(df_silver, "Stock_Abonados_Silver_Detalle.parquet", filas_iniciales=len(df_silver), ruta_destino=ruta_silver)

    df_gold = df_silver.groupby(["Quincena Evaluada", "Franquicia"], as_index=False).agg(
        Total_Abonados=("ID", "nunique"),
        Fecha_Corte=("FechaFin", "max")
    )
    
    guardar_parquet(df_gold, "Stock_Abonados_Gold_Resumen.parquet", filas_iniciales=len(df_gold), ruta_destino=ruta_gold)
    
    console.print(f"[bold green]✨ Proceso Finalizado. (Bronze, Silver y Gold generados)[/]")

if __name__ == "__main__":
    ejecutar()