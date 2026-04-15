import pandas as pd
import numpy as np
import sys
import os
import glob
import re
import datetime
import calendar
import polars as pl
import duckdb
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

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
        fecha_archivo = pd.to_datetime(fecha_str, format="%d-%m-%Y", errors='coerce')
        
        if pd.isnull(fecha_archivo):
            return None, None, None
            
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
        return None, None, None

# ==========================================
# ORQUESTADOR (INCREMENTAL CRONOLÓGICO)
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold cyan]📊 PIPELINE: ABONADOS (INCREMENTAL CRONOLÓGICO ULTRA-RÁPIDO)[/]")

    ruta_origen = PATHS.get("raw_abonados_idf")
    if not ruta_origen or not os.path.exists(ruta_origen):
        ruta_origen = r"C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\raw_data\5-Indice de falla\2-Abonados"
    
    if not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: No se encuentra la ruta: {ruta_origen}[/]")
        return

    # --- FIX CRONOLÓGICO: Ordenamos por la FECHA REAL del archivo, no por el nombre ---
    archivos_raw = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    archivos = sorted(
        [f for f in archivos_raw if not os.path.basename(f).startswith("~$")],
        key=lambda x: obtener_fecha_corte_snapshot(x)[0] if obtener_fecha_corte_snapshot(x)[0] else pd.Timestamp('1900-01-01')
    )
    
    if not archivos:
        console.print("[bold red]⛔ No se encontraron archivos RAW para procesar.[/]")
        return

    # 1. LEER MEMORIA HISTÓRICA
    ruta_silver = PATHS.get("silver", "data/silver")
    ruta_silver_completa = os.path.join(ruta_silver, "Stock_Abonados_Silver_Detalle.parquet")
    
    quincenas_existentes = set()
    if os.path.exists(ruta_silver_completa):
        try:
            df_hist_meta = pd.read_parquet(ruta_silver_completa, columns=["Quincena Evaluada"])
            quincenas_existentes = set(df_hist_meta["Quincena Evaluada"].unique())
            del df_hist_meta
            
            console.print(f"[green]✅ Memoria Silver cargada. {len(quincenas_existentes)} quincenas registradas.[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Error leyendo memoria: {e}. Se hará lectura completa.[/]")

    # 2. PROCESAMIENTO INTELIGENTE
    console.print(f"\n[cyan]🚀 Fase 1: Escaneando {len(archivos)} Snapshots en orden cronológico...[/]")
    dataframes_list = []
    quincenas_procesadas_hoy = []
    
    # El último de la lista ordenada cronológicamente es el mes más reciente
    ultimo_archivo_path = archivos[-1] 

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]({task.completed}/{task.total})[/]"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("[dim]{task.description}"),
        console=console
    ) as progress:
        
        task = progress.add_task("Procesando...", total=len(archivos))
        
        for archivo in archivos:
            nombre_archivo = os.path.basename(archivo)
            es_ultimo_archivo = (archivo == ultimo_archivo_path)
            
            fecha_inicio, fecha_fin, quincena_nombre = obtener_fecha_corte_snapshot(archivo)
            
            if not fecha_inicio:
                progress.advance(task)
                continue

            # OMISIÓN INTELIGENTE
            if quincena_nombre in quincenas_existentes and not es_ultimo_archivo:
                progress.console.print(f"[dim]  ⏭️ Saltando -> {quincena_nombre} (Ya en Silver)[/]")
                progress.advance(task)
                continue

            # LECTURA
            try:
                if es_ultimo_archivo:
                    progress.console.print(f"[magenta]  🔄 Refrescando quincena actual -> {quincena_nombre}[/]")
                
                df = pl.read_excel(archivo, engine="calamine", infer_schema_length=0).to_pandas(use_pyarrow_extension_array=True)
                if df.empty: 
                    progress.advance(task)
                    continue

                df.columns = df.columns.astype(str).str.strip()
                df = df.rename(columns=MAPEO_COLUMNAS)

                # Construcción del DataFrame pequeño
                df_small = pd.DataFrame()
                df_small["ID"] = df["ID"] if "ID" in df.columns else None
                df_small["Estatus contrato"] = df["Estatus contrato"] if "Estatus contrato" in df.columns else None
                df_small["Franquicia"] = df["Franquicia"].fillna("NO DEFINIDA").astype(str).str.strip().str.upper()
                df_small["Quincena Evaluada"] = quincena_nombre
                df_small["FechaInicio"] = fecha_inicio
                df_small["FechaFin"] = fecha_fin

                # Filtro Pruebas
                col_det = next((c for c in ["Detalle Orden", "Detalle"] if c in df.columns), None)
                if col_det:
                    df_small = df_small[df[col_det] != "PRUEBA DE INTERNET"]

                dataframes_list.append(df_small[COLS_ABONADOS_SILVER].copy())
                quincenas_procesadas_hoy.append(quincena_nombre)

                progress.console.print(f"[green]  ✅ Procesado -> {quincena_nombre} ({len(df_small):,} abonados)[/]")

            except Exception as e:
                progress.console.print(f"[red]❌ Error en {nombre_archivo}: {e}[/]")
            finally:
                if 'df' in locals(): del df
                
            
            progress.advance(task)

    # 3. UPSERT EN SILVER
    if not dataframes_list:
        console.print("[bold green]✅ El sistema ya está al día.[/]")
        return

    console.print(f"\n[cyan]🔄 Fase 2: Uniendo con historial Silver (Upsert)...[/]")
    df_nuevo_lote = pd.concat(dataframes_list, ignore_index=True).drop_duplicates()
    dataframes_list.clear()
    
    # Limpiamos los nulos del lote nuevo antes de inyectarlo
    df_nuevo_lote = limpiar_nulos_powerbi(df_nuevo_lote)
    
    if os.path.exists(ruta_silver_completa):
        console.print("[cyan]🦆 Ejecutando Upsert Out-Of-Core con DuckDB (Cero RAM)...[/]")
        con = duckdb.connect(database=':memory:')
        con.execute("PRAGMA memory_limit='2GB'")
        con.register('df_nuevo_lote', df_nuevo_lote)
        
        quincenas_str = ", ".join([f"'{q}'" for q in quincenas_procesadas_hoy])
        ruta_temp = ruta_silver_completa + ".tmp"
        
        # Hacemos el cruce escribiendo directamente al disco duro
        query = f"""
        COPY (
            SELECT * FROM read_parquet('{ruta_silver_completa.replace(chr(92), '/')}')
            WHERE "Quincena Evaluada" NOT IN ({quincenas_str})
            UNION ALL BY NAME
            SELECT * FROM df_nuevo_lote
        ) TO '{ruta_temp.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """
        con.execute(query)
        
        # 4. GUARDADO GOLD OUT-OF-CORE
        ruta_gold = PATHS.get("gold", "data/gold")
        ruta_gold_completa = os.path.join(ruta_gold, "Stock_Abonados_Gold_Resumen.parquet")
        query_gold = f"""
        COPY (
            SELECT "Quincena Evaluada", Franquicia, 
                   COUNT(DISTINCT ID) as Total_Abonados, 
                   MAX(FechaFin) as Fecha_Corte
            FROM read_parquet('{ruta_temp.replace(chr(92), '/')}')
            GROUP BY "Quincena Evaluada", Franquicia
        ) TO '{ruta_gold_completa.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """
        con.execute(query_gold)
        con.close()
        
        # Reemplazamos el silver viejo por el nuevo consolidado
        if os.path.exists(ruta_silver_completa):
            os.remove(ruta_silver_completa)
        os.rename(ruta_temp, ruta_silver_completa)
        
        del df_nuevo_lote
        
    else:
        df_silver_final = df_nuevo_lote
        guardar_parquet(df_silver_final, "Stock_Abonados_Silver_Detalle.parquet", filas_iniciales=len(df_silver_final), ruta_destino=ruta_silver)
        df_gold = df_silver_final.groupby(["Quincena Evaluada", "Franquicia"], as_index=False).agg(Total_Abonados=("ID", "nunique"), Fecha_Corte=("FechaFin", "max"))
        guardar_parquet(df_gold, "Stock_Abonados_Gold_Resumen.parquet", filas_iniciales=len(df_gold), ruta_destino=PATHS.get("gold", "data/gold"))
    
    console.print(f"[bold green]✨ Proceso Finalizado. (Sincronizado Cronológicamente)[/]")

if __name__ == "__main__":
    ejecutar()