import pandas as pd
import os
import re
from config import PATHS
import utils

@utils.reportar_tiempo
def ejecutar():
    utils.console.rule("[bold magenta]ETL: VENTAS ESTATUS (INCREMENTAL)[/]")
    
    # 1. CONFIGURACIÓN
    RUTA_RAW = PATHS["ventas_estatus"]
    NOMBRE_GOLD = "Ventas_Estatus_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)

    # 2. INGESTA INTELIGENTE
    # Usamos "Fecha" porque así se llama la columna en el GOLD (ya renombrada)
    df_nuevo, df_historico = utils.ingesta_inteligente(
        ruta_raw=RUTA_RAW, 
        ruta_gold=RUTA_GOLD_COMPLETA, 
        col_fecha_corte="Fecha"
    )

    # Si no hay nada que hacer, salimos
    if df_nuevo.empty and not df_historico.empty:
        utils.console.print("[bold green]✅ Proceso terminado sin cambios.[/]")
        return

    # 3. TRANSFORMACIÓN (SOLO DATOS NUEVOS)
    if not df_nuevo.empty:
        utils.console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} filas nuevas...[/]")
        
        # A. Limpieza de columnas del Excel Raw
        df_nuevo.columns = df_nuevo.columns.str.strip()

        # B. Renombrado Estándar (Vital hacerlo al principio)
        mapa_columnas = {
            "Fecha Venta": "Fecha", 
            "Franquicia": "Nombre Franquicia", 
            "Hora venta": "Hora",
            "Cédula": "Documento",    
            "Rif": "Documento",
            "Nro. Doc": "Documento"
        }
        df_nuevo = df_nuevo.rename(columns=mapa_columnas)
        
        # C. Asegurar columnas críticas (Relleno de seguridad)
        cols_check = ["Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", "Estatus", "Documento"]
        for col in cols_check:
            if col not in df_nuevo.columns:
                df_nuevo[col] = "" 

        # D. Transformaciones de Texto
        df_nuevo["Paquete/Servicio"] = df_nuevo["Paquete/Servicio"].astype(str).str.upper()
        df_nuevo["Vendedor"] = df_nuevo["Vendedor"].astype(str).str.upper().str.strip()
        
        # E. Filtros (Fibex Play y Calle)
        df_nuevo = df_nuevo[~df_nuevo["Paquete/Servicio"].str.contains("FIBEX PLAY|FIBEXPLAY", na=False, regex=True)].copy()
        df_nuevo = df_nuevo[~df_nuevo["Vendedor"].str.contains("VENTAS CALLE|AGENTE", regex=True, na=False)].copy()

        # F. Etiquetado y Extracción
        df_nuevo["Tipo de afluencia"] = "Ventas"
        
        # Regex para Oficina
        patron_oficina = r'.*(?:OFICINA|OFIC|OFI)\s+(.*)$'
        df_nuevo["Oficina"] = df_nuevo["Vendedor"].str.extract(patron_oficina)[0].str.strip()

        # G. Conversión de Fecha
        df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")

        # H. Selección de Columnas Finales (Antes de unir)
        cols_finales = [
            "N° Abonado", "Documento", "Estatus", "Fecha", "Vendedor", 
            "Costo", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
            "Hora", "Tipo de afluencia", "Oficina"
        ]
        # Usamos reindex para que si falta alguna columna (ej. Costo), se cree con NaN
        df_nuevo = df_nuevo.reindex(columns=cols_finales)

    # 4. FUSIÓN Y DEDUPLICACIÓN
    df_final = pd.DataFrame()

    # Unir
    if not df_historico.empty and not df_nuevo.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        filas_antes = len(df_final)
        
        # Deduplicación (Sobre todo el conjunto por seguridad)
        subset_dedup = ["N° Abonado", "Documento", "Hora", "Fecha", "Vendedor"]
        
        # Eliminamos filas idénticas, manteniendo la última (la más reciente cargada)
        df_final = df_final.drop_duplicates(subset=subset_dedup, keep='last')

        # 5. GUARDADO
        utils.guardar_parquet(
            df_final, 
            NOMBRE_GOLD, 
            filas_iniciales=filas_antes,
            ruta_destino=PATHS.get("gold", "data/gold")
        )

if __name__ == "__main__":
    ejecutar()