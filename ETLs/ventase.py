import pandas as pd
import os
import re
from config import PATHS
import utils

@utils.reportar_tiempo
def ejecutar():
    utils.console.rule("[bold magenta]ETL: VENTAS ESTATUS (INCREMENTAL)[/]")
    
    # 1. CONFIGURACIÓN PARA SILVER Y GOLD
    RUTA_RAW = PATHS["ventas_estatus"]
    
    NOMBRE_SILVER = "Ventas_Estatus_Silver.parquet"
    RUTA_SILVER_COMPLETA = os.path.join(PATHS.get("silver", "data/silver"), NOMBRE_SILVER) # Ajusta la clave según tu config.py
    
    NOMBRE_GOLD = "Ventas_Estatus_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Ventas_Estatus_Bronze.parquet")

    try:
        utils.archivos_raw(RUTA_RAW, RUTA_BRONZE)
    except Exception as e:
        utils.console.print(f"[yellow]⚠️ La capa Bronze no se actualizó, pero el ETL continuará. Error: {e}[/]")
            
    # 2. INGESTA INTELIGENTE
    df_nuevo, df_historico = utils.ingesta_inteligente(
        ruta_raw=RUTA_RAW, 
        ruta_gold=RUTA_GOLD_COMPLETA, 
        col_fecha_corte="Fecha"
    )

    if df_nuevo.empty and not df_historico.empty:
        utils.console.print("[bold green]✅ Proceso terminado sin cambios.[/]")
        return

    # 3. TRANSFORMACIÓN (SOLO DATOS NUEVOS)
    if not df_nuevo.empty:
        utils.console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} filas nuevas...[/]")
        
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
            "Hora", "Tipo de afluencia", "Oficina", 
            "fecha_mod_archivo" # <-- SE INCLUYE PARA QUE NO SE BORRE EN EL REINDEX
        ]
        df_nuevo = df_nuevo.reindex(columns=cols_finales)

    # 4. FUSIÓN Y DEDUPLICACIÓN
    df_final = pd.DataFrame()

    # Preparamos el histórico (que viene del Gold y ya no tiene fecha_mod_archivo)
    if not df_historico.empty:
        # Le asignamos timestamp 0 para que los datos nuevos siempre le ganen en caso de conflicto
        df_historico["fecha_mod_archivo"] = 0

    if not df_historico.empty and not df_nuevo.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        filas_antes = len(df_final)
        
        # --- ORDENAMIENTO POR FECHA DE MODIFICACIÓN ---
        if "fecha_mod_archivo" in df_final.columns:
            df_final = df_final.sort_values(by="fecha_mod_archivo", ascending=True)
        
        subset_dedup = ["N° Abonado", "Documento", "Hora", "Fecha", "Vendedor"]
        
        # Ahora el 'last' siempre retiene el registro proveniente del archivo físicamente más nuevo
        df_final = df_final.drop_duplicates(subset=subset_dedup, keep='last')
        df_final = utils.standard_hours(df_final, 'Hora')
        
        # 5. GUARDADO EN SILVER (CON LA COLUMNA DE CONTROL)
        utils.console.print("\n[bold cyan]💾 Guardando capa Silver...[/]")
        utils.guardar_parquet(
            df_final, 
            NOMBRE_SILVER, 
            filas_iniciales=filas_antes,
            ruta_destino=PATHS.get("silver", "data/silver") # Revisa que esta ruta exista en tu config
        )

        # 6. LIMPIEZA Y GUARDADO EN GOLD (SIN LA COLUMNA DE CONTROL)
        if "fecha_mod_archivo" in df_final.columns:
            df_final = df_final.drop(columns=["fecha_mod_archivo"])
            
        utils.console.print("\n[bold yellow]🏆 Guardando capa Gold...[/]")
        utils.guardar_parquet(
            df_final, 
            NOMBRE_GOLD, 
            filas_iniciales=len(df_final), # Pasamos el actual para que no reporte eliminaciones dobles
            ruta_destino=PATHS.get("gold", "data/gold")
        )

if __name__ == "__main__":
    ejecutar()