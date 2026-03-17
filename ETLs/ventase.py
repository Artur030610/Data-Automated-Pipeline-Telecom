import pandas as pd
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
    # --- PASO 2: LECTURA FULL DESDE BRONZE (Fuente de Verdad) ---
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
    
    cols_check = ["Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", "Estatus", "Documento"]
    for col in cols_check:
        if col not in df_nuevo.columns:
            df_nuevo[col] = "" 

    df_nuevo["Paquete/Servicio"] = df_nuevo["Paquete/Servicio"].astype(str).str.upper()
    df_nuevo["Vendedor"] = df_nuevo["Vendedor"].astype(str).str.upper().str.strip()
    
    df_nuevo = df_nuevo[~df_nuevo["Paquete/Servicio"].str.contains("FIBEX PLAY|FIBEXPLAY", na=False, regex=True)].copy()
    df_nuevo = df_nuevo[~df_nuevo["Vendedor"].str.contains("VENTAS CALLE|AGENTE", regex=True, na=False)].copy()

    df_nuevo["Tipo de afluencia"] = "Ventas"
    
    patron_oficina = r'.*(?:OFICINA|OFIC|OFI)\s+(.*)$'
    df_nuevo["Oficina"] = df_nuevo["Vendedor"].str.extract(patron_oficina)[0].str.strip()

    df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")

    cols_finales = [
        "N° Abonado", "Documento", "Estatus", "Fecha", "Vendedor", 
        "Costo", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
        "Hora", "Tipo de afluencia", "Oficina"
    ]
    df_nuevo = df_nuevo.reindex(columns=cols_finales)

    # =========================================================
    # --- PASO 4: ASEGURAMIENTO DE CALIDAD Y DEDUPLICACIÓN ---
    # =========================================================
    # Asignamos directamente la data transformada a df_final
    df_final = df_nuevo.copy()

    if not df_final.empty:
        filas_antes = len(df_final)
        
        subset_dedup = ["N° Abonado", "Documento", "Hora", "Fecha", "Vendedor"]
        df_final = df_final.drop_duplicates(subset=subset_dedup, keep='last')
        df_final = utils.standard_hours(df_final, 'Hora')
        
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