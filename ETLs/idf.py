import pandas as pd
import numpy as np
import sys
import os
import glob

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, console, obtener_rango_fechas, limpiar_nulos_powerbi

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]PIPELINE: ÍNDICE DE FALLA (IdF) - ENGINE: CALAMINE[/]")

    # 1. Definir Ruta de Origen
    ruta_origen = PATHS.get("raw_idf")
    
    if not ruta_origen or not os.path.exists(ruta_origen):
        console.print(f"[red]❌ Error: La ruta 'raw_idf' no existe o no está en config.py[/]")
        return

    # --- CORRECCIÓN: DEFINIR LA VARIABLE 'ARCHIVOS' ---
    # Usamos glob para listar los archivos uno por uno
    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx"))
    console.print(f"📂 Se encontraron {len(archivos)} archivos. Iniciando lectura optimizada...")

    # 2. Schema Final
    cols_finales = [
        "FechaInicio", "FechaFin", "FechaInicioQuincena", "Quincena Evaluada", "Registros válidos",
        "N° Contrato", "Estatus contrato", "N° Orden", "Estatus_orden",
        "Fecha Emisión", "Fecha Impresión", "Fecha Final", "Grupo Afinidad",
        "Tipo Orden", "Detalle Orden", "Fecha Cierre", "Franquicia",
        "Grupo Trabajo", "Usuario Emisión", "Usuario Impresión", "Usuario Final",
        "Fecha Finalizacion", "Solucion Aplicada"
    ]

    dataframes_procesados = []

    # 3. Procesamiento Iterativo
    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)

        # --- A. Filtros de Archivo ---
        if nombre_archivo.startswith("~$"): continue 
        if "Consolidado" in nombre_archivo: continue
        if "$" in nombre_archivo: continue

        # --- B. Metadatos (Fechas desde el nombre) ---
        fecha_inicio, fecha_fin, quincena_nombre = obtener_rango_fechas(nombre_archivo)
        
        if not fecha_inicio:
            continue

        try:
            # --- C. LECTURA TURBO CON CALAMINE 🚀 ---
            df = pd.read_excel(
                archivo, 
                engine="calamine", 
                dtype=str 
            )
            
            if df.empty: continue

            # --- D. Normalización de Fechas ---
            cols_fecha = ["Fecha Emisión", "Fecha Final", "Fecha Impresión", "Fecha Cierre", "Fecha Finalizacion"]
            for col in cols_fecha:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # --- E. Cálculo Inicio Quincena ---
            dia_fin = fecha_fin.day
            inicio_q_dia = 1 if dia_fin == 15 else 15
            fecha_inicio_quincena = pd.Timestamp(fecha_fin.year, fecha_fin.month, inicio_q_dia)

            # --- F. Filtro Cruzado de Fechas ---
            required_cols = ["Fecha Emisión", "Fecha Final", "Fecha Cierre", "Fecha Finalizacion"]
            if not all(col in df.columns for col in required_cols):
                console.print(f"[yellow]⚠️ {nombre_archivo}: Faltan columnas de fecha críticas. Saltando.[/]")
                continue

            mask_fechas = (
                (df["Fecha Emisión"] >= fecha_inicio) & (df["Fecha Emisión"] <= fecha_fin) &
                (df["Fecha Final"] >= fecha_inicio_quincena) & (df["Fecha Final"] <= fecha_fin) &
                (df["Fecha Cierre"] >= fecha_inicio_quincena) & (df["Fecha Cierre"] <= fecha_fin) &
                (df["Fecha Finalizacion"] >= fecha_inicio_quincena) & (df["Fecha Finalizacion"] <= fecha_fin)
            )
            
            df_filtrado = df[mask_fechas].copy()
            registros_validos = len(df_filtrado)

            if registros_validos == 0: continue

            # --- G. Enriquecimiento ---
            df_filtrado["FechaInicio"] = fecha_inicio
            df_filtrado["FechaFin"] = fecha_fin
            df_filtrado["FechaInicioQuincena"] = fecha_inicio_quincena
            df_filtrado["Quincena Evaluada"] = quincena_nombre
            df_filtrado["Registros válidos"] = registros_validos

            # --- H. Filtros de Negocio ---
            col_solucion = df_filtrado["Solucion Aplicada"].fillna("").str.upper()
            col_grupo = df_filtrado["Grupo Trabajo"].fillna("").str.upper()
            col_detalle = df_filtrado["Detalle Orden"].fillna("").str.upper()
            col_estatus = df_filtrado["Estatus_orden"].fillna("").str.upper()

            excluir_soluciones = ["CAMBIO DE CLAVE", "CLIENTE SOLICITO REEMBOLSO", "LLAMADAS DE AGENDAMIENTO", "ORDEN REPETIDA"]
            
            mask_negocio = (
                ~col_solucion.isin(excluir_soluciones) &
                ~col_grupo.str.contains("GT API FIBEX", na=False) &
                (col_detalle != "PRUEBA DE INTERNET") &
                ~col_estatus.str.contains("CREACIÓN", na=False)
            )
            
            df_filtrado = df_filtrado[mask_negocio]

            # --- I. Selección Final ---
            df_final = df_filtrado.reindex(columns=cols_finales)
            dataframes_procesados.append(df_final)

            console.print(f"   ✅ {nombre_archivo}: {len(df_final)} filas procesadas")

        except ImportError:
             console.print("[bold red]❌ ERROR CRÍTICO: Falta instalar calamine.[/]")
             console.print("   Ejecuta: pip install python-calamine")
             return
        except Exception as e:
            console.print(f"   ❌ Error en {nombre_archivo}: {e}")

    # 4. Consolidación
    if dataframes_procesados:
        console.print("🔄 Consolidando IdF...")
        df_total = pd.concat(dataframes_procesados, ignore_index=True)
        df_total = df_total.drop_duplicates()
        df_total = limpiar_nulos_powerbi(df_total)
        
         # 5. Guardado
        guardar_parquet(df_total, "IdF_Gold.parquet", filas_iniciales=len(df_total))
    else:
        console.print("[yellow]⚠️ No se generaron datos de IdF.[/]")

if __name__ == "__main__":
    ejecutar()