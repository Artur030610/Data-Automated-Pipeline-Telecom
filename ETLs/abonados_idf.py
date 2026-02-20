import pandas as pd
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
    limpiar_nulos_powerbi
)

# --- CONFIGURACIÓN GLOBAL ---
MAPEO_COLUMNAS = {
    "Nombre Franquicia": "Franquicia",
    "Franquicia": "Franquicia",
    
    # --- CAMBIO: AHORA MAPEAMOS EL ID ---
    "ID": "ID",
    "Id": "ID",
    "id": "ID",
    "ID_CLIENTE": "ID",
    
    "Estatus contrato": "Estatus contrato",
    "Estatus": "Estatus contrato"
}

COLS_ABONADOS_SILVER = [
    "Quincena Evaluada", "FechaInicio", "FechaFin",
    "ID", "Estatus contrato",  # <--- CAMBIO AQUÍ
    "Franquicia" 
]

# --- REGEX FLEXIBLE ---
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
        
        # Normalizar separadores
        fecha_str = re.sub(r"\W", "-", match.group(1))
        
        fecha_archivo = pd.to_datetime(fecha_str, format="%d-%m-%Y")
        
        dia = fecha_archivo.day
        mes = fecha_archivo.month
        anio = fecha_archivo.year
        
        # --- LÓGICA DE CIERRE DE QUINCENA ---
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
        # LECTURA
        df = pd.read_excel(ruta_completa, engine="calamine")
        
        if df.empty: 
            return False, f"[dim]⚠️ Archivo vacío: {nombre_archivo}[/]", None

        # 1. Limpieza de columnas
        df.columns = df.columns.astype(str).str.strip()
        df = df.rename(columns=MAPEO_COLUMNAS)

        if "Franquicia" not in df.columns:
            return False, f"[red]❌ Falta columna Franquicia: {nombre_archivo}[/]", None

        # 2. Transformación y Selección
        df_small = pd.DataFrame()
        
        # --- CAMBIO IMPORTANTE: Usamos ID en lugar de Contrato ---
        if "ID" in df.columns:
            df_small["ID"] = df["ID"]
        else:
            # Fallback por si acaso algún archivo viejo usa otro nombre
            # Pero llenará nulos si no existe ID, lo cual es correcto si queremos ID
            df_small["ID"] = None 
            
        df_small["Estatus contrato"] = df["Estatus contrato"] if "Estatus contrato" in df.columns else None
        
        # Limpieza Franquicia
        df_small["Franquicia"] = df["Franquicia"].fillna("NO DEFINIDA").astype(str).str.strip().str.upper()
        
        # Columnas calculadas
        df_small["Quincena Evaluada"] = quincena_nombre
        df_small["FechaInicio"] = fecha_inicio
        df_small["FechaFin"] = fecha_fin

        # Filtro pruebas
        col_detalle = next((c for c in ["Detalle Orden", "Detalle"] if c in df.columns), None)
        if col_detalle:
            mask_pruebas = df[col_detalle] == "PRUEBA DE INTERNET"
            df_small = df_small[~mask_pruebas]

        # Liberamos memoria
        del df
        gc.collect()

        # Filtrar solo columnas existentes del Silver
        cols_finales = [c for c in COLS_ABONADOS_SILVER if c in df_small.columns]
        df_final = df_small[cols_finales].copy()

        return True, f"   ✅ {nombre_archivo} -> {quincena_nombre}", df_final

    except Exception as e:
        return False, f"[red]❌ Error crítico en {nombre_archivo}: {e}[/]", None

# ==========================================
# ORQUESTADOR PARALELO
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold cyan]PIPELINE: ABONADOS (SNAPSHOTS)[/]")

    # Intenta buscar en la ruta específica de abonados
    ruta_origen = PATHS.get("raw_abonados_idf")
    if not ruta_origen:
        base_raw = PATHS.get("raw_idf")
        if base_raw:
             ruta_origen = os.path.join(os.path.dirname(base_raw), "2-Abonados")
    
    # HARDCODE TEMPORAL
    if not ruta_origen or not os.path.exists(ruta_origen):
        ruta_origen = r"C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\raw_data\5-Indice de falla\2-Abonados"
    
    if not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: No se encuentra la ruta: {ruta_origen}[/]")
        return

    ruta_silver = PATHS.get("silver")
    ruta_gold   = PATHS.get("gold")

    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    archivos = [f for f in archivos if not os.path.basename(f).startswith("~$")]
    
    console.print(f"📂 Ruta: {ruta_origen}")
    console.print(f"🚀 Iniciando procesamiento de {len(archivos)} archivos...")

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

    # --- CONSOLIDACIÓN ---
    console.print(f"\n🔄 Consolidando {len(dataframes_list)} DataFrames...")
    
    # SILVER
    df_silver = pd.concat(dataframes_list, ignore_index=True)
    df_silver = df_silver.drop_duplicates()
    df_silver = limpiar_nulos_powerbi(df_silver)

    guardar_parquet(df_silver, "Stock_Abonados_Silver_Detalle.parquet", filas_iniciales=len(df_silver), ruta_destino=ruta_silver)

    # GOLD (Resumen por Franquicia y Quincena usando ID)
    df_gold = df_silver.groupby(["Quincena Evaluada", "Franquicia"], as_index=False).agg(
        Total_Abonados=("ID", "nunique"), # <--- AHORA CUENTA IDs ÚNICOS
        Fecha_Corte=("FechaFin", "max")
    )
    
    guardar_parquet(df_gold, "Stock_Abonados_Gold_Resumen.parquet", filas_iniciales=len(df_gold), ruta_destino=ruta_gold)
    
    console.print(f"[bold green]✨ Proceso Finalizado. (Usando columna ID)[/]")

if __name__ == "__main__":
    ejecutar()