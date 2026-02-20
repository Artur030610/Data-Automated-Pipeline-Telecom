import pandas as pd
import numpy as np
import sys
import os
import glob
import re
import datetime

# --- CONFIGURACIÓN DE RUTAS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, console, limpiar_nulos_powerbi

ruta_silver = PATHS.get("silver")
ruta_gold = PATHS.get("gold")

# ==========================================
# 1. CONSTANTES DE NEGOCIO
# ==========================================
HORAS_SLA_META = 24.0

NOC_USERS = [
    "GFARFAN", "JVELASQUEZ", "JOLUGO", "KUSEA", "SLOPEZ",
    "EDESPINOZA", "SANDYJIM", "JOCASTILLO", "JESUSGARCIA",
    "DFUENTES", "JOCANTO", "IXMONTILLA", "OMTRUJILLO",
    "LLZERPA", "LUIJIMENEZ"
]

EXCLUIR_SOLUCIONES = [
    "CAMBIO DE CLAVE", "CLIENTE SOLICITO REEMBOLSO", 
    "LLAMADAS DE AGENDAMIENTO", "ORDEN REPETIDA", "CONSULTA DE SALDO", "ORDEN MAL GENERADA",
]

# ==========================================
# 2. DEFINICIÓN DE COLUMNAS
# ==========================================

# --- SILVER (Detalle Fila por Fila) ---
ORDEN_FINAL_SILVER = [
    "Quincena Evaluada", "FechaInicio", "FechaFin", 
    "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion",
    "Fecha_Creacion_Date", 
    "Es_Falla", "Cumplio_SLA", "Duracion_Horas",
    "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min",
    "Franquicia", "Grupo Trabajo", "Grupo Afinidad", "Clasificacion",
    "N° Contrato", "N° Orden", "Estatus contrato", "Estatus_orden",
    "Usuario Final", "Usuario Emisión", "Usuario Impresión",
    "Solucion Aplicada", "Detalle Orden", "SLA Detalle Texto"
]

# --- GOLD SLA (Resumen Tiempos) ---
ORDEN_FINAL_GOLD_SLA = [
    "Quincena Evaluada", "Franquicia", "Clasificacion", "Fecha_Creacion_Date",
    "Total_Ordenes",      # Ahora será conteo único
    "SLA Resolucion Min", 
    "SLA Despacho Min",   
    "SLA Impresion Min"   
]

# --- GOLD IDF (Resumen Fallas) ---
ORDEN_FINAL_GOLD_IDF = [
    "Quincena Evaluada", "Franquicia", "Fecha_Creacion_Date",
    "Total_Fallas", 
    "Fecha_Corte"
]

# --- INPUT RAW ---
COLS_INPUT_RAW = [
    "FechaInicio", "FechaFin", "FechaInicioQuincena", "Quincena Evaluada",
    "N° Contrato", "Estatus contrato", "N° Orden", "Estatus_orden",
    "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion", 
    "Grupo Afinidad", "Detalle Orden", "Franquicia",
    "Grupo Trabajo", "Usuario Emisión", "Usuario Impresión", 
    "Usuario Final", "Solucion Aplicada"
]

# ==========================================
# 3. FUNCIONES UTILITARIAS
# ==========================================
def obtener_rango_fechas(nombre_archivo):
    try:
        nombre_limpio = os.path.basename(nombre_archivo).lower()
        patron = r"(\d{1,2}-\d{1,2}-\d{4})\s+al\s+(\d{1,2}-\d{1,2}-\d{4})"
        match = re.search(patron, nombre_limpio)
        if not match: return None, None, None

        str_inicio, str_fin = match.groups()
        f_inicio = datetime.datetime.strptime(str_inicio, "%d-%m-%Y")
        f_fin = datetime.datetime.strptime(str_fin, "%d-%m-%Y")

        quincena_str = "Q1" if f_fin.day <= 15 else "Q2"
        meses = ["", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
                 "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
        nombre_etiqueta = f"{meses[f_fin.month]} {f_fin.year} {quincena_str}"
        return f_inicio, f_fin, nombre_etiqueta
    except Exception as e:
        console.print(f"[red]❌ Error interpretando fechas en {nombre_archivo}: {e}[/]")
        return None, None, None

def limpiar_fechas_mixtas(series):
    if pd.api.types.is_datetime64_any_dtype(series): return series
    series_nums = pd.to_numeric(series, errors='coerce')
    mask_es_serial = (series_nums > 35000) & (series_nums < 60000)
    fechas_excel = pd.to_datetime(series_nums[mask_es_serial], unit='D', origin='1899-12-30')
    resto = series[~mask_es_serial].astype(str).str.strip()
    fechas_texto = pd.to_datetime(resto, dayfirst=True, errors='coerce')
    return fechas_excel.combine_first(fechas_texto)

# ==========================================
# 4. PIPELINE PRINCIPAL
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]PIPELINE MASTER: TICKETS (SLA + IDF + DIARIO)[/]")

    ruta_origen = PATHS.get("raw_idf") 
    if not ruta_origen or not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: La ruta 'raw_idf' no existe[/]")
        return

    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    console.print(f"📂 Se encontraron {len(archivos)} archivos. Iniciando procesamiento...")

    dataframes_procesados = []

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        if nombre_archivo.startswith("~$") or "Consolidado" in nombre_archivo: continue
        
        fecha_inicio, fecha_fin, quincena_nombre = obtener_rango_fechas(nombre_archivo)
        if not fecha_inicio: continue

        try:
            df = pd.read_excel(archivo, engine="calamine")
            if df.empty: continue

            # --- LIMPIEZA FECHAS ---
            cols_fecha = ["Fecha Creacion", "Fecha Emisión", "Fecha Final", 
                          "Fecha Impresion", "Fecha Cierre", "Fecha Finalizacion"]
            for col in cols_fecha:
                if col in df.columns: df[col] = limpiar_fechas_mixtas(df[col])

            if "Fecha Creacion" not in df.columns: continue

            mask_creacion = df["Fecha Creacion"] >= fecha_inicio
            if "Fecha Finalizacion" in df.columns:
                 df_filtrado = df[mask_creacion].copy() 
            else:
                 df_filtrado = df[mask_creacion].copy()

            if len(df_filtrado) == 0: continue

            # --- ENRIQUECIMIENTO ---
            df_filtrado = df_filtrado.assign(
                FechaInicio = fecha_inicio,
                FechaFin = fecha_fin,
                FechaInicioQuincena = fecha_inicio,
                Quincena_Evaluada = quincena_nombre
            ).rename(columns={"Quincena_Evaluada": "Quincena Evaluada"})

            # --- NORMALIZACIÓN ---
            df_filtrado = df_filtrado.assign(
                Solucion_Norm = lambda x: x["Solucion Aplicada"].fillna("").astype(str).str.upper(),
                Grupo_Norm    = lambda x: x["Grupo Trabajo"].fillna("").astype(str).str.upper(),
                Detalle_Norm  = lambda x: x["Detalle Orden"].fillna("").astype(str).str.upper(),
                Estatus_Norm  = lambda x: x["Estatus_orden"].fillna("").astype(str).str.upper()
            )
            
            # --- FILTROS DE EXCLUSIÓN (Mejorados) ---
            mask_excluir = (
                df_filtrado["Solucion_Norm"].isin(EXCLUIR_SOLUCIONES) |
                df_filtrado["Grupo_Norm"].str.contains("GT API FIBEX", na=False) |
                (df_filtrado["Detalle_Norm"] == "PRUEBA DE INTERNET") |
                df_filtrado["Estatus_Norm"].str.contains("CREACIÓN", na=False) |
                # NUEVO: Excluir explícitamente anuladas para no inflar conteo
                df_filtrado["Estatus_Norm"].isin(["ANULADA", "CANCELADA", "ELIMINADA"])
            )
            
            df_final = df_filtrado[~mask_excluir].copy()
            
            # Conservamos columnas necesarias
            cols_to_keep = [c for c in COLS_INPUT_RAW if c in df_final.columns]
            dataframes_procesados.append(df_final[cols_to_keep])
            
            console.print(f"   ✅ {nombre_archivo} -> {quincena_nombre}: {len(df_final)} filas")

        except Exception as e:
            console.print(f"   ❌ Error procesando {nombre_archivo}: {e}")

    # ==========================================
    # 5. CONSOLIDACIÓN Y CÁLCULOS
    # ==========================================
    if dataframes_procesados:
        console.print(f"\n🔄 Consolidando {len(dataframes_procesados)} DataFrames...")
        df_total = pd.concat(dataframes_procesados, ignore_index=True)

        # --- CLASIFICACIÓN ---
        df_total['Grupo_Norm'] = df_total['Grupo Trabajo'].fillna('').astype(str).str.upper()
        df_total['Usuario_Norm'] = df_total['Usuario Final'].fillna('').astype(str).str.upper()

        condiciones = [
            df_total['Usuario_Norm'].isin(NOC_USERS),
            df_total['Grupo_Norm'].isin(NOC_USERS),
            df_total['Usuario_Norm'].str.contains('NOC', na=False),
            df_total['Grupo_Norm'].str.contains('OPERACIONES', na=False)
        ]
        df_total['Clasificacion'] = np.select(condiciones, ['NOC', 'NOC', 'NOC', 'OPERACIONES'], default='MESA DE CONTROL')

        # --- CÁLCULOS KPI ---
        for c in ['Fecha Finalizacion', 'Fecha Creacion', 'Fecha Impresion']:
            df_total[c] = pd.to_datetime(df_total[c], errors='coerce')

        delta_res = df_total['Fecha Finalizacion'] - df_total['Fecha Creacion']
        delta_des = df_total['Fecha Finalizacion'] - df_total['Fecha Impresion']
        delta_imp = df_total['Fecha Impresion'] - df_total['Fecha Creacion']

        df_total['SLA Resolucion Min'] = (delta_res.dt.total_seconds() / 60).round(2)
        df_total['SLA Despacho Min']   = (delta_des.dt.total_seconds() / 60).round(2)
        df_total['SLA Impresion Min']  = (delta_imp.dt.total_seconds() / 60).round(2)
        df_total['SLA Detalle Texto']  = delta_res.astype(str).replace('NaT', None)

        # --- FILTRO LIMPIEZA (Negativos y Ceros) ---
        df_total = df_total[df_total['SLA Resolucion Min'] > 0]

        # --- GRANULARIDAD ---
        df_total['Fecha_Creacion_Date'] = df_total['Fecha Creacion'].dt.date

        # Métricas
        df_total['Duracion_Horas'] = (delta_res.dt.total_seconds() / 3600)
        df_total['Es_Falla'] = 1 
        df_total['Cumplio_SLA'] = np.where(
            (df_total['Duracion_Horas'].notna()) & (df_total['Duracion_Horas'] <= HORAS_SLA_META), 
            1, 0
        )

        df_total = df_total.drop(columns=['Grupo_Norm', 'Usuario_Norm'])
        df_total = df_total.drop_duplicates()
        df_total = limpiar_nulos_powerbi(df_total)

        # ==========================================
        # 6. SALIDAS (MEDALLION)
        # ==========================================
        
        # --- A. SILVER MASTER ---
        cols_existentes_silver = [c for c in ORDEN_FINAL_SILVER if c in df_total.columns]
        df_silver_master = df_total.reindex(columns=cols_existentes_silver)
        guardar_parquet(df_silver_master, "Tickets_Silver_Master.parquet", filas_iniciales=len(df_silver_master), ruta_destino=ruta_silver)
        
        # --- B. GOLD SLA (Agrupado por DÍA) ---
        df_sla_base = df_silver_master.dropna(subset=['SLA Resolucion Min'])
        
        # ---------------------------------------------------------------
        # AQUÍ ESTÁ EL CAMBIO CLAVE PARA CORREGIR "TOTAL MUY ALTO" 👇
        # Usamos "nunique" en lugar de "count" para Total_Ordenes
        # ---------------------------------------------------------------
        df_gold_sla = df_sla_base.groupby(
            ["Quincena Evaluada", "Franquicia", "Clasificacion", "Fecha_Creacion_Date"], 
            as_index=False
        ).agg(
            Total_Ordenes=("N° Orden", "nunique"), # <--- ESTO CORRIGE LA DUPLICIDAD
            **{
                "SLA Resolucion Min": ("SLA Resolucion Min", "sum"),
                "SLA Despacho Min": ("SLA Despacho Min", "sum"),
                "SLA Impresion Min": ("SLA Impresion Min", "sum")
            }
        )
        df_gold_sla = df_gold_sla.reindex(columns=ORDEN_FINAL_GOLD_SLA)
        guardar_parquet(df_gold_sla, "SLA_Gold.parquet", filas_iniciales=len(df_gold_sla), ruta_destino=ruta_gold)

        # --- C. GOLD IDF (Agrupado por DÍA) ---
        # Para fallas también aplicamos unicidad si lo deseas, o mantenemos sumas si quieres ver eventos
        # Normalmente IDF es "Tickets Únicos con Falla"
        df_gold_idf = df_silver_master.groupby(
            ["Quincena Evaluada", "Franquicia", "Fecha_Creacion_Date"],
            as_index=False
        ).agg(
            Total_Fallas=("N° Orden", "nunique"), # <--- TAMBIÉN AQUÍ
            Fecha_Corte=("Fecha Finalizacion", "max")
        )
        df_gold_idf = df_gold_idf.reindex(columns=ORDEN_FINAL_GOLD_IDF)
        guardar_parquet(df_gold_idf, "IDF_Gold.parquet", filas_iniciales=len(df_gold_idf), ruta_destino=ruta_gold)

        console.print(f"[bold green]✨ Proceso Finalizado (Corregido Conteo Único).[/]")
        console.print(f"   1. 🥈 Tickets_Silver_Master.parquet")
        console.print(f"   2. 🥇 SLA_Gold.parquet (Suma Minutos / Conteo Único)")
        console.print(f"   3. 🥇 IDF_Gold.parquet (Conteo Único)")
        
        # VERIFICACIÓN RÁPIDA
        total_filas_silver = len(df_silver_master)
        total_unicos = df_silver_master["N° Orden"].nunique()
        console.print(f"\n[dim]🔍 Auditoría rápida:[/]")
        console.print(f"   - Filas Totales (Eventos): {total_filas_silver:,}")
        console.print(f"   - Tickets Únicos Reales:   {total_unicos:,}")
        console.print(f"   - Diferencia (Duplicados): {total_filas_silver - total_unicos:,}")

    else:
        console.print("[yellow]⚠️ No se generaron datos.[/]")

if __name__ == "__main__":
    ejecutar()