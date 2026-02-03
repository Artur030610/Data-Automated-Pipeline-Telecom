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
# CONSTANTES DE NEGOCIO
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

COLS_EXTRACCION = [
    "FechaInicio", "FechaFin", "FechaInicioQuincena", "Quincena Evaluada",
    "N° Contrato", "Estatus contrato", "N° Orden", "Estatus_orden",
    "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion", 
    "Grupo Afinidad", "Detalle Orden", "Franquicia",
    "Grupo Trabajo", "Usuario Emisión", "Usuario Impresión", 
    "Usuario Final", "Solucion Aplicada"
]

ORDEN_FINAL_SILVER = [
    "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion",
    "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min", # Numéricos (KPIs)
    "SLA Detalle Texto", # Visual (Tablas)
    "Clasificacion", "N° Orden", "N° Contrato", 
    "Grupo Trabajo", "Usuario Final", "Solucion Aplicada",
    "Estatus_orden", "Detalle Orden", "Grupo Afinidad", 
    "Franquicia", "Usuario Emisión", "Usuario Impresión", "Estatus contrato",
    "Quincena Evaluada", "FechaInicio", "FechaFin"
]
ORDEN_FINAL_GOLD = [
    #"Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion",
    "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min", # Numéricos (KPIs)
    "SLA Detalle Texto", # Visual (Tablas)
    "Clasificacion", #"N° Orden", "N° Contrato", 
    #"Grupo Trabajo", "Usuario Final", "Solucion Aplicada",
    "Estatus_orden", "Detalle Orden", "Grupo Afinidad", 
    "Franquicia", #"Usuario Emisión", "Usuario Impresión", "Estatus contrato",
    "Quincena Evaluada", #"FechaInicio", "FechaFin"
]

# ==========================================
# 1. FUNCIONES UTILITARIAS
# ==========================================
def obtener_rango_fechas(nombre_archivo):
    """
    Extrae fechas del nombre del archivo y genera la etiqueta de quincena.
    Ej: 'Data... 1-12-2025 al 15-1-2026.xlsx' -> fechas y 'ENE 2026 Q1'
    """
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
    """
    Maneja columnas con mezcla de fechas, números seriales Excel y texto sucio.
    """
    # 1. Si ya es fecha, no tocar
    if pd.api.types.is_datetime64_any_dtype(series):
        return series

    # 2. Intentar convertir seriales numéricos de Excel
    series_nums = pd.to_numeric(series, errors='coerce')
    # Seriales válidos aprox entre 1995 (35000) y 2064 (60000)
    mask_es_serial = (series_nums > 35000) & (series_nums < 60000)
    
    fechas_excel = pd.to_datetime(series_nums[mask_es_serial], unit='D', origin='1899-12-30')
    
    # 3. El resto tratar como texto
    resto = series[~mask_es_serial].astype(str).str.strip()
    fechas_texto = pd.to_datetime(resto, dayfirst=True, errors='coerce')
    
    # 4. Combinar
    return fechas_excel.combine_first(fechas_texto)

# ==========================================
# 2. PIPELINE PRINCIPAL
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]PIPELINE: ACUERDOS DE SERVICIOS (SLA) - GOLD EDITION[/]")

    # 1. Validar Ruta
    ruta_origen = PATHS.get("raw_sla")
    if not ruta_origen or not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: La ruta 'raw_sla' no existe o no está en config.py[/]")
        return

    # 2. Listar Archivos
    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    console.print(f"📂 Se encontraron {len(archivos)} archivos. Iniciando procesamiento...")

    dataframes_procesados = []

    # 3. Procesamiento Iterativo
    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)

        if nombre_archivo.startswith("~$") or "Consolidado" in nombre_archivo:
            continue
        
        fecha_inicio, fecha_fin, quincena_nombre = obtener_rango_fechas(nombre_archivo)
        if not fecha_inicio:
            console.print(f"[dim yellow]⚠️ Saltando {nombre_archivo} (Sin fechas válidas)[/]")
            continue

        try:
            # --- C. LECTURA (Detectando tipos nativos) ---
            df = pd.read_excel(archivo, engine="calamine")
            
            if df.empty: continue

            # --- D. CORRECCIÓN DE FECHAS ---
            cols_fecha = ["Fecha Creacion", "Fecha Emisión", "Fecha Final", 
                          "Fecha Impresion", "Fecha Cierre", "Fecha Finalizacion"]
            
            for col in cols_fecha:
                if col in df.columns:
                    df[col] = limpiar_fechas_mixtas(df[col])

            # --- E. FILTROS ESTRICTOS (Regla de Negocio) ---
            if "Fecha Finalizacion" not in df.columns or "Fecha Creacion" not in df.columns:
                console.print(f"[red]❌ {nombre_archivo} falta columnas de fecha críticas[/]")
                continue

            # 1. Finalizado dentro del rango del archivo
            mask_cierre = (df["Fecha Finalizacion"] >= fecha_inicio) & (df["Fecha Finalizacion"] <= fecha_fin)
            
            # 2. Creado a partir de la fecha de inicio del archivo (Elimina backlogs viejos)
            mask_creacion = df["Fecha Creacion"] >= fecha_inicio

            df_filtrado = df[mask_cierre & mask_creacion].copy()
            
            # Auditoría opcional
            eliminados = len(df[mask_cierre]) - len(df_filtrado)
            if eliminados > 0:
                console.print(f"   [dim yellow]✂️ Se eliminaron {eliminados} tickets creados antes del {fecha_inicio.date()}[/]")

            if len(df_filtrado) == 0:
                console.print(f"[yellow]⚠️ {nombre_archivo}: 0 filas tras filtros.[/]")
                continue

            # --- F. ENRIQUECIMIENTO ---
            df_filtrado = df_filtrado.assign(
                FechaInicio = fecha_inicio,
                FechaFin = fecha_fin,
                FechaInicioQuincena = fecha_inicio,
                Quincena_Evaluada = quincena_nombre
            ).rename(columns={"Quincena_Evaluada": "Quincena Evaluada"})

            # --- G. LIMPIEZA DE CAMPOS ---
            df_filtrado = df_filtrado.assign(
                Solucion_Norm = lambda x: x["Solucion Aplicada"].fillna("").astype(str).str.upper(),
                Grupo_Norm    = lambda x: x["Grupo Trabajo"].fillna("").astype(str).str.upper(),
                Detalle_Norm  = lambda x: x["Detalle Orden"].fillna("").astype(str).str.upper(),
                Estatus_Norm  = lambda x: x["Estatus_orden"].fillna("").astype(str).str.upper()
            )
            
            mask_excluir = (
                df_filtrado["Solucion_Norm"].isin(EXCLUIR_SOLUCIONES) |
                df_filtrado["Grupo_Norm"].str.contains("GT API FIBEX", na=False) |
                (df_filtrado["Detalle_Norm"] == "PRUEBA DE INTERNET") |
                df_filtrado["Estatus_Norm"].str.contains("CREACIÓN", na=False)
            )
            
            df_final = df_filtrado[~mask_excluir].reindex(columns=COLS_EXTRACCION)
            dataframes_procesados.append(df_final)
            console.print(f"   ✅ {nombre_archivo} -> [cyan]{quincena_nombre}[/]: {len(df_final)} filas OK")

        except Exception as e:
            console.print(f"   ❌ Error procesando {nombre_archivo}: {e}")

    # ==========================================
    # 4. CONSOLIDACIÓN
    # ==========================================
    if dataframes_procesados:
        console.print(f"\n🔄 Consolidando {len(dataframes_procesados)} DataFrames...")
        
        df_total = pd.concat(dataframes_procesados, ignore_index=True)

        # --- A. CLASIFICACIÓN ---
        df_total['Grupo_Norm'] = df_total['Grupo Trabajo'].fillna('').astype(str).str.upper()
        df_total['Usuario_Norm'] = df_total['Usuario Final'].fillna('').astype(str).str.upper()

        condiciones = [
            df_total['Usuario_Norm'].isin(NOC_USERS),
            df_total['Grupo_Norm'].isin(NOC_USERS),
            df_total['Usuario_Norm'].str.contains('NOC', na=False),
            df_total['Grupo_Norm'].str.contains('OPERACIONES', na=False)
        ]
        opciones = ['NOC', 'NOC', 'NOC', 'OPERACIONES']
        df_total['Clasificacion'] = np.select(condiciones, opciones, default='MESA DE CONTROL')

        # --- B. LIMPIEZA DE DATA BASURA ---
        # Borrar tickets que tienen fin pero no inicio (imposible calcular SLA)
        nulos_inicio = df_total['Fecha Creacion'].isna().sum()
        if nulos_inicio > 0:
            console.print(f"[yellow]⚠️ Eliminando {nulos_inicio} tickets con Fecha Fin pero SIN Fecha Inicio.[/]")
            df_total = df_total.dropna(subset=['Fecha Creacion'])

        # --- C. CÁLCULO DE SLAs ---
        # 1. Timedeltas base
        delta_res = df_total['Fecha Finalizacion'] - df_total['Fecha Creacion']
        delta_des = df_total['Fecha Finalizacion'] - df_total['Fecha Impresion']
        delta_imp = df_total['Fecha Impresion'] - df_total['Fecha Creacion']

        # 2. MINUTOS (Numérico para Power BI / KPIs)
        # Dividimos segundos entre 60 y redondeamos
        df_total['SLA Resolucion Min'] = (delta_res.dt.total_seconds() / 60).round(2)
        df_total['SLA Despacho Min']   = (delta_des.dt.total_seconds() / 60).round(2)
        df_total['SLA Impresion Min']  = (delta_imp.dt.total_seconds() / 60).round(2)

        # 3. TEXTO (Para visualización "bonita" en tablas: "1 days 05:00:00")
        df_total['SLA Detalle Texto'] = delta_res.astype(str).replace('NaT', None)

       # --- D. GUARDADO FINAL ---
        # 1. Limpieza Común
        df_total = df_total.drop(columns=['Grupo_Norm', 'Usuario_Norm'])
        df_total = df_total.drop_duplicates()
        df_total = limpiar_nulos_powerbi(df_total)

        # ---------------------------------------------------------
        # 2. PREPARACIÓN DE DATAFRAMES (SEPARACIÓN EXPLÍCITA)
        # ---------------------------------------------------------
        
        # SILVER: Todas las columnas (Detalle)
        df_silver = df_total.reindex(columns=ORDEN_FINAL_SILVER)
        
        # GOLD: Solo columnas KPI (Resumen)
        # Nota: Usamos df_total original para reindexar, es más seguro
        df_gold = df_total.reindex(columns=ORDEN_FINAL_GOLD)

        # ---------------------------------------------------------
        # 3. GUARDADO 
        # ---------------------------------------------------------
        
        # Guardar SILVER
        guardar_parquet(
            df_silver,                      # DataFrame: SILVER
            "SLA_Silver.parquet",           # Nombre: Silver
            filas_iniciales=len(df_silver),
            ruta_destino=ruta_silver        # Carpeta: Silver
        )
        
        # Guardar GOLD
        guardar_parquet(
            df_gold,                        # DataFrame: GOLD
            "SLA_Gold.parquet",             # Nombre: Gold
            filas_iniciales=len(df_gold),
            ruta_destino=ruta_gold          # Carpeta: Gold
        )

        console.print(f"[bold green] Proceso Finalizado Exitosamente.[/]")
        
    else:
        console.print("[yellow]⚠️ No se generaron datos. Verifica nombres de archivos o filtros.[/]")

if __name__ == "__main__":
    ejecutar()