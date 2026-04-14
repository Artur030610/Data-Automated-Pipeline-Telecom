import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# IMPORTAMOS LAS VARIABLES EXACTAS DE TU CONFIG
from config import (
    PATHS, 
    FOLDERS_RECLAMOS_GENERAL, 
    SUB_RECLAMOS_APP, 
    SUB_RECLAMOS_BANCO,
    FALLAS_BANCO_TARGET
)
from utils import guardar_parquet, console, reportar_tiempo, ingesta_incremental_polars, standard_hours, limpiar_nulos_powerbi

# -----------------------------------------------------------------------------
# 1. ETL: RECLAMOS GENERALES (Call Center, OOCC, RRSS)
# -----------------------------------------------------------------------------
@reportar_tiempo  
def procesar_reclamos_general():
    console.rule("[bold cyan]1. ETL: RECLAMOS GENERALES (BRONZE INCREMENTAL / GOLD FULL)[/]")
    
    NOMBRE_GOLD = "Reclamos_General_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Reclamos_General_Raw_Bronze.parquet")

    # =========================================================
    # --- PASO 1: ACTUALIZACIÓN INCREMENTAL BRONZE (POLARS) ---
    # =========================================================
    for carpeta_nombre in FOLDERS_RECLAMOS_GENERAL:
        ruta_completa = os.path.join(PATHS["raw_reclamos"], carpeta_nombre)
        if os.path.exists(ruta_completa):
            try:
                ingesta_incremental_polars(
                    ruta_raw=ruta_completa,
                    ruta_bronze_historico=RUTA_BRONZE,
                    columna_fecha="Fecha Llamada"
                )
            except Exception as e:
                console.print(f"[yellow]⚠️ La capa Bronze no se actualizó para {carpeta_nombre}. Error: {e}[/]")

    # =========================================================
    # --- PASO 2: LECTURA FULL DESDE BRONZE ---
    # =========================================================
    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold green]✅ Reclamos Generales: Sistema actualizado (sin datos).[/]")
        return
        
    df_nuevo_total = pd.read_parquet(RUTA_BRONZE)

    if df_nuevo_total.empty:
        console.print("[bold green]✅ Reclamos Generales: Sistema actualizado.[/]")
        return

    # =========================================================
    # --- PASO 3: TRANSFORMACIÓN PANDAS ---
    # =========================================================
    console.print(f"[cyan]🛠️ Transformando {len(df_nuevo_total)} reclamos generales totales...[/]")
    
    # EL FIX CRÍTICO: Limpiar nombres de columnas (espacios en blanco ocultos del Excel)
    df_nuevo_total.columns = df_nuevo_total.columns.str.strip()

    df_nuevo_total = df_nuevo_total.rename(columns={"Tipo Llamada": "Origen"})
    
    cols_std = ["N° Abonado", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
                "Origen", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
                "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad", "Fecha_Modificacion_Archivo"]
    
    # 🚨 Alerta de Columnas Faltantes
    cols_check = [c for c in cols_std if c != "Fecha_Modificacion_Archivo"]
    faltantes = [c for c in cols_check if c not in df_nuevo_total.columns]
    if faltantes:
        console.print(f"[bold yellow]⚠️ ALERTA: Faltan columnas en origen:[/] [white]{', '.join(faltantes)}[/]")

    for c in cols_std:
        if c not in df_nuevo_total.columns:
            df_nuevo_total[c] = np.nan
            
    df_nuevo_total = df_nuevo_total.reindex(columns=cols_std)
    
    df_nuevo_total["Fecha Llamada"] = pd.to_datetime(df_nuevo_total["Fecha Llamada"], dayfirst=True, errors="coerce").dt.normalize()
    
    # =========================================================
    # --- PASO 4: ORDENAMIENTO CDC, DEDUPLICACIÓN Y BLINDAJE ---
    # =========================================================
    df_final = df_nuevo_total.copy()
    
    if 'Fecha_Modificacion_Archivo' in df_final.columns:
        df_final = df_final.sort_values(by='Fecha_Modificacion_Archivo', ascending=True)
        
    df_final = df_final.drop_duplicates(subset=["N° Abonado", "Fecha Llamada", "Hora Llamada", "Origen"], keep='last')
    
    if 'Fecha_Modificacion_Archivo' in df_final.columns:
        df_final = df_final.drop(columns=['Fecha_Modificacion_Archivo'])
        
    # Aplicamos el traductor de horas y el blindaje
    df_final = standard_hours(df_final, 'Hora Llamada')
    df_final = limpiar_nulos_powerbi(df_final)
    
    
    guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=len(df_nuevo_total), ruta_destino=PATHS.get("gold", ""))

# -----------------------------------------------------------------------------
# 2. ETL: FALLAS APP (USANDO CONFIG)
# -----------------------------------------------------------------------------
@reportar_tiempo  
def procesar_fallas_app():
    console.rule("[bold cyan]2. ETL: FALLAS APP (BRONZE INCREMENTAL / GOLD FULL)[/]")
    
    RUTA_APP = os.path.join(PATHS["raw_reclamos"], SUB_RECLAMOS_APP)
    NOMBRE_GOLD = "Reclamos_App_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Reclamos_App_Raw_Bronze.parquet")

    if not os.path.exists(RUTA_APP):
        console.print(f"[bold red]❌ Error: No existe la ruta configurada: {RUTA_APP}[/]")
        return

    try:
        ingesta_incremental_polars(
            ruta_raw=RUTA_APP,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha Llamada"
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó. Error: {e}[/]")

    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold green]✅ Fallas App: Sistema actualizado (sin datos).[/]")
        return
        
    df_nuevo = pd.read_parquet(RUTA_BRONZE)

    if df_nuevo.empty:
        console.print("[bold green]✅ Fallas App: Sistema actualizado.[/]")
        return

    console.print(f"[cyan]📱 Procesando {len(df_nuevo)} registros totales de APP...[/]")
    
    df_nuevo.columns = df_nuevo.columns.str.strip()

    df_nuevo["Detalle Respuesta"] = df_nuevo["Detalle Respuesta"].astype(str).str.upper()
    df_nuevo["OrdenCategoria"] = df_nuevo.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    df_nuevo["Fecha Llamada"] = pd.to_datetime(df_nuevo["Fecha Llamada"], dayfirst=True, errors="coerce").dt.normalize()
    
    cols = ["N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
            "Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Responsable", 
            "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad", "OrdenCategoria", "Fecha_Modificacion_Archivo"]
    
    for c in cols:
        if c not in df_nuevo.columns:
            df_nuevo[c] = np.nan
            
    df_nuevo = df_nuevo.reindex(columns=cols)
    
    df_final = df_nuevo.copy()
    
    if not df_final.empty:
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.sort_values(by='Fecha_Modificacion_Archivo', ascending=True)
            
        df_final = df_final.drop_duplicates(subset=["N° Abonado", "Fecha Llamada", "Hora Llamada"], keep='last')
        
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.drop(columns=['Fecha_Modificacion_Archivo'])
            
        df_final = standard_hours(df_final, 'Hora Llamada')
        df_final = limpiar_nulos_powerbi(df_final)
        
        guardar_parquet(df_final, NOMBRE_GOLD, ruta_destino=PATHS.get("gold", ""))

# -----------------------------------------------------------------------------
# 3. ETL: FALLAS BANCOS (USANDO CONFIG)
# -----------------------------------------------------------------------------
@reportar_tiempo 
def procesar_fallas_banco():
    console.rule("[bold cyan]3. ETL: FALLAS BANCOS (BRONZE INCREMENTAL / GOLD FULL)[/]")
    
    RUTA_BANCO = os.path.join(PATHS["raw_reclamos"], SUB_RECLAMOS_BANCO)
    NOMBRE_GOLD = "Reclamos_Banco_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Reclamos_Banco_Raw_Bronze.parquet")

    if not os.path.exists(RUTA_BANCO):
        console.print(f"[bold red]❌ Error: No existe la ruta configurada: {RUTA_BANCO}[/]")
        return

    try:
        ingesta_incremental_polars(
            ruta_raw=RUTA_BANCO,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha Llamada"
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó. Error: {e}[/]")

    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold green]✅ Fallas Banco: Sistema actualizado (sin datos).[/]")
        return
        
    df_nuevo = pd.read_parquet(RUTA_BRONZE)

    if df_nuevo.empty:
        console.print("[bold green]✅ Fallas Banco: Sistema actualizado.[/]")
        return

    console.print(f"[cyan]🏦 Procesando {len(df_nuevo)} registros totales de BANCOS...[/]")
    
    df_nuevo.columns = df_nuevo.columns.str.strip()

    target = FALLAS_BANCO_TARGET
    if "Detalle Respuesta" not in df_nuevo.columns:
        df_nuevo["Detalle Respuesta"] = np.nan
        
    df_nuevo["Detalle Respuesta"] = df_nuevo["Detalle Respuesta"].astype(str).str.upper().str.strip()
    
    df_nuevo = df_nuevo[df_nuevo["Detalle Respuesta"].isin(target)].copy()
    
    if not df_nuevo.empty:
        df_nuevo["Detalle Respuesta"] = (df_nuevo["Detalle Respuesta"]
                                        .str.replace("FALLA CON ", "", regex=False)
                                        .str.replace("FALLA ", "", regex=False)
                                        .str.strip())
        
        df_nuevo["TotalCuenta"] = df_nuevo.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
        df_nuevo["Fecha Llamada"] = pd.to_datetime(df_nuevo["Fecha Llamada"], dayfirst=True, errors="coerce").dt.normalize()
        
        cols = ["N° Abonado", "Cliente", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
                "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
                "Observación", "Grupo Afinidad", "Franquicia", "Ciudad", "TotalCuenta", "Fecha_Modificacion_Archivo"]

        for c in cols:
            if c not in df_nuevo.columns:
                df_nuevo[c] = np.nan

        df_nuevo = df_nuevo.reindex(columns=cols)

        df_final = df_nuevo.copy()
        
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.sort_values(by='Fecha_Modificacion_Archivo', ascending=True)
            
        df_final = df_final.drop_duplicates(subset=["N° Abonado", "Fecha Llamada", "Hora Llamada", "Detalle Respuesta"], keep='last')
        
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.drop(columns=['Fecha_Modificacion_Archivo'])
            
        df_final = standard_hours(df_final, 'Hora Llamada')
        df_final = limpiar_nulos_powerbi(df_final)
        
        guardar_parquet(df_final, NOMBRE_GOLD, ruta_destino=PATHS.get("gold", ""))
    else:
        console.print("[yellow]⚠️ Archivos leídos pero sin fallas bancarias relevantes.[/]")

def ejecutar():
    procesar_reclamos_general()
    procesar_fallas_app()
    procesar_fallas_banco()

if __name__ == "__main__":
    ejecutar()