import pandas as pd
import numpy as np
import os
import re
from config import PATHS
import utils

@utils.reportar_tiempo
def ejecutar():
    utils.console.rule("[bold magenta]ETL: VENTAS ESTATUS (BRONZE INCREMENTAL / SILVER-GOLD FULL)[/]")
    
    # 1. CONFIGURACIÓN PARA SILVER Y GOLD
    RUTA_RAW = PATHS["ventas_estatus"]
    NOMBRE_SILVER = "Ventas_Estatus_Silver.parquet"
    RUTA_SILVER_COMPLETA = os.path.join(PATHS.get("silver", "data/silver"), NOMBRE_SILVER)
    
    NOMBRE_GOLD = "Ventas_Estatus_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Ventas_Estatus_Bronze.parquet")

    # =========================================================
    # --- PASO 1: ACTUALIZACIÓN BRONZE CON POLARS ---
    # =========================================================
    try:
        # Usamos Polars para actualizar el histórico Bronze velozmente
        utils.ingesta_incremental_polars(
            ruta_raw=RUTA_RAW,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha Venta"
        )
    except Exception as e:
        utils.console.print(f"[yellow]⚠️ La capa Bronze no se actualizó. Error: {e}[/]")
            
    # =========================================================
    # --- PASO 2: LECTURA FULL DESDE BRONZE ---
    # =========================================================
    if not os.path.exists(RUTA_BRONZE):
        utils.console.print("[red]❌ No se encontró la capa Bronze. Ejecución abortada.[/]")
        return
        
    utils.console.print("[cyan]📥 Leyendo histórico completo desde capa Bronze...[/]")
    df_nuevo = pd.read_parquet(RUTA_BRONZE)

    if df_nuevo.empty:
        utils.console.print("[bold green]✅ No hay datos en Bronze para procesar.[/]")
        return

    # =========================================================
    # --- PASO 3: TRANSFORMACIÓN PANDAS (Lógica de Negocio) ---
    # =========================================================
    utils.console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} registros totales...[/]")
    
    df_nuevo.columns = df_nuevo.columns.str.strip()

    mapa_columnas = {
        "Fecha Venta": "Fecha", 
        "Franquicia": "Nombre Franquicia", 
        "Hora venta": "Hora",
        "Cédula": "Documento",    
        "Rif": "Documento",
        "Nro. Doc": "Documento"
    }
    df_nuevo = df_nuevo.rename(columns=mapa_columnas)
    
    # 🚨 NUEVO: ALERTA DE COLUMNAS FALTANTES
    cols_check = ["Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", "Estatus", "Documento"]
    columnas_faltantes = [c for c in cols_check if c not in df_nuevo.columns]
    if columnas_faltantes:
        utils.console.print(f"[bold yellow]⚠️ ALERTA: Faltan las siguientes columnas en el origen:[/] [white]{', '.join(columnas_faltantes)}[/]")

    for col in cols_check:
        if col not in df_nuevo.columns:
            df_nuevo[col] = np.nan 

    df_nuevo["Paquete/Servicio"] = df_nuevo["Paquete/Servicio"].astype(str).str.upper()
    df_nuevo["Vendedor"] = df_nuevo["Vendedor"].astype(str).str.upper().str.strip()
    
    df_nuevo = df_nuevo[~df_nuevo["Paquete/Servicio"].str.contains("FIBEX PLAY|FIBEXPLAY", na=False, regex=True)].copy()
    df_nuevo = df_nuevo[~df_nuevo["Vendedor"].str.contains("VENTAS CALLE|AGENTE", regex=True, na=False)].copy()
    df_nuevo = df_nuevo[df_nuevo["Vendedor"].str.contains("OFI", case=False, na=False)].copy()
    df_nuevo["Tipo de afluencia"] = "Ventas"
    
    patron_oficina = r'.*(?:OFICINA|OFIC|OFI)\s+(.*)$'
    df_nuevo["Oficina"] = df_nuevo["Vendedor"].str.extract(patron_oficina)[0].str.strip()

    df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce").dt.normalize()

    cols_finales = [
        "N° Abonado", "Documento", "Estatus", "Fecha", "Vendedor", 
        "Costo", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
        "Hora", "Tipo de afluencia", "Oficina", "Fecha_Modificacion_Archivo"
    ]
    
    for c in cols_finales:
        if c not in df_nuevo.columns:
            df_nuevo[c] = np.nan
            
    df_nuevo = df_nuevo.reindex(columns=cols_finales)

    # =========================================================
    # --- PASO 4: ORDENAMIENTO (CDC) Y DEDUPLICACIÓN ---
    # =========================================================
    df_final = df_nuevo.copy()

    if not df_final.empty:
        filas_antes = len(df_final)
        
        # 1. ORDENAMIENTO CRONOLÓGICO POR METADATA (Si existe)
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            utils.console.print("[cyan]⏱️ Ordenando por metadata para preservar el registro más reciente...[/]")
            df_final = df_final.sort_values(by='Fecha_Modificacion_Archivo', ascending=True)
        
        # 2. ESTANDARIZAR ANTES DE DEDUPLICAR (Evita falsos duplicados por formato ERP)
        df_final = utils.standard_hours(df_final, 'Hora')

        # 3. DEDUPLICACIÓN ESTRICTA
        subset_dedup = ["N° Abonado", "Documento", "Hora", "Fecha", "Vendedor"]
        df_final = df_final.drop_duplicates(subset=subset_dedup, keep='last')
        
        #  ELIMINAMOS LA COLUMNA FANTASMA (Hizo su trabajo y no sale)
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.drop(columns=['Fecha_Modificacion_Archivo'])
            utils.console.print("[dim]🧹 Metadata CDC eliminada. DataFrame limpio para Power BI.[/]")
        
        # =========================================================
        # --- PASO 5: BLINDAJE POWER BI Y GUARDADO ---
        # =========================================================
        
        if hasattr(utils, 'limpiar_nulos_powerbi'):
            df_final = utils.limpiar_nulos_powerbi(df_final)
            
            
        # 5. GUARDADO EN SILVER
        utils.console.print("\n[bold cyan]💾 Actualizando capas Silver y Gold...[/]")
        utils.guardar_parquet(
            df_final, 
            NOMBRE_SILVER, 
            filas_iniciales=filas_antes,
            ruta_destino=PATHS.get("silver", "data/silver")
        )

        # 6. GUARDADO EN GOLD
        utils.guardar_parquet(
            df_final, 
            NOMBRE_GOLD, 
            filas_iniciales=len(df_final),
            ruta_destino=PATHS.get("gold", "data/gold")
        )

if __name__ == "__main__":
    ejecutar()