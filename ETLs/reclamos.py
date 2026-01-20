import pandas as pd
import os
# Asumimos que config y utils están en la carpeta raíz y se ejecuta desde main.py
from config import PATHS, FOLDERS_RECLAMOS_GENERAL, SUB_RECLAMOS_APP, SUB_RECLAMOS_BANCO
from utils import leer_carpeta, guardar_parquet, console, reportar_tiempo

# --- A. RECLAMOS GENERALES ---
@reportar_tiempo  
def procesar_reclamos_general():
    console.rule("[bold cyan]1. ETL: RECLAMOS GENERALES (CC + OOCC + RRSS)[/]")
    
    dfs_acumulados = []
    
    for carpeta_nombre in FOLDERS_RECLAMOS_GENERAL:
        ruta_completa = os.path.join(PATHS["raw_reclamos"], carpeta_nombre)
        # Aquí no hace falta columnas_esperadas porque concatenas simple
        df_temp = leer_carpeta(ruta_completa, filtro_exclusion="Consolidado")
        
        if not df_temp.empty:
            dfs_acumulados.append(df_temp)
    
    if not dfs_acumulados:
        console.print("[warning]⚠️ No data en carpetas generales.[/]")
        return

    df = pd.concat(dfs_acumulados, ignore_index=True)

    df = df.rename(columns={"Tipo Llamada": "Origen"})
    if "Barrio" in df.columns: 
        df["Barrio"] = df["Barrio"].astype(str).str.upper()
        
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    
    cols = ["N° Abonado", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
            "Origen", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
            "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad"]
    
    # --- CORRECCIÓN AQUÍ ---
    # 1. Filtramos primero las columnas de interés
    df_final = df.reindex(columns=cols)
    
    # 2. Eliminamos duplicados EXACTOS basándonos en esas columnas
    filas_antes = len(df_final)
    df_final = df_final.drop_duplicates()
    filas_despues = len(df_final)
    
    if filas_antes > filas_despues:
        console.print(f"[yellow]  -> Se eliminaron {filas_antes - filas_despues} duplicados en General.[/]")

    # Limpieza específica que ya tenías
    for col in ["N° Abonado", "Responsable", "Detalle Respuesta", "Origen"]:
        df_final[col] = df_final[col].fillna("").astype(str)
    
    guardar_parquet(df_final, "Reclamos_General_Gold.parquet")

# --- B. FALLAS APP ---
@reportar_tiempo  
def procesar_fallas_app():
    console.rule("[bold cyan]2. ETL: FALLAS APP[/]")
    
    ruta_app = os.path.join(PATHS["raw_reclamos"], SUB_RECLAMOS_APP)
    df = leer_carpeta(ruta_app)
    
    if df.empty: return

    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper()
    
    # Aplicamos drop_duplicates antes de calcular transformaciones para no contar doble
    df = df.drop_duplicates()

    df["OrdenCategoria"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], dayfirst=True, errors="coerce")
    
    cols = ["N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
            "Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Responsable", 
            "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad", "OrdenCategoria"]
    
    df_final = df.reindex(columns=cols)
    
    # Segunda limpieza de duplicados por si el reindex generó filas idénticas al quitar columnas únicas
    df_final = df_final.drop_duplicates()

    for c in ["N° Abonado", "Documento"]:
        df_final[c] = df_final[c].astype(str).replace('nan', None)
    
    guardar_parquet(df_final, "Reclamos_App_Gold.parquet")

# --- C. FALLAS BANCO ---
@reportar_tiempo 
def procesar_fallas_banco():
    console.rule("[bold cyan]3. ETL: FALLAS BANCOS[/]")
    
    ruta_banco = os.path.join(PATHS["raw_reclamos"], SUB_RECLAMOS_BANCO)
    df = leer_carpeta(ruta_banco)
    
    if df.empty: return
    filas_raw = len(df)
    
    # Eliminamos duplicados crudos al inicio
    df = df.drop_duplicates()

    target = ["FALLA BNC", "FALLA CON BDV", "FALLA CON R4", "FALLA MERCANTIL"]
    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper().str.strip()
    df = df[df["Detalle Respuesta"].isin(target)].copy()
    
    if df.empty:
        console.print("[warning]⚠️ Sin datos de Banco.[/]")
        return

    df["Detalle Respuesta"] = (df["Detalle Respuesta"]
                               .str.replace("FALLA CON ", "", regex=False)
                               .str.replace("FALLA ", "", regex=False)
                               .str.strip())
    
    df["TotalCuenta"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    
    cols = ["N° Abonado", "Cliente", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
            "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
            "Observación", "Grupo Afinidad", "Franquicia", "Ciudad", "TotalCuenta"]

    df_final = df.reindex(columns=cols)
    
    # Aseguramos unicidad final
    df_final = df_final.drop_duplicates()

    for c in ["Observación", "N° Abonado", "Cliente", "Responsable"]:
        df_final[c] = df_final[c].fillna("").astype(str)

    guardar_parquet(df_final, "Reclamos_Banco_Gold.parquet", filas_iniciales=filas_raw)

# ==========================================
# FUNCIÓN MAESTRA
# ==========================================
def ejecutar():
    """
    Esta función es la que llama main.py
    """
    console.print("[bold blue]Iniciando Suite de Reclamos...[/]")
    procesar_reclamos_general()
    procesar_fallas_app()
    procesar_fallas_banco()