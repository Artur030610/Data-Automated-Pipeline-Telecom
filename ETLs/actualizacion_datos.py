import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, FOLDERS_ACT_DATOS
# Importamos la función incremental de Polars
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console, archivos_raw, ingesta_incremental_polars, standard_hours, limpiar_nulos_powerbi

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]5. ETL: ACTUALIZACIÓN DE DATOS (INCREMENTAL HÍBRIDO)[/]")
    
    # 1. DEFINICIÓN DE RUTAS Y ESTRUCTURAS
    BASE_PATH_RAW = PATHS["raw_act_datos"]
    NOMBRE_GOLD = "Actualizacion_Datos_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Actualizacion_Datos_Raw_Bronze.parquet")
    
    # --- PASO 1: ACTUALIZACIÓN BRONZE CON POLARS ---
    # Nota: Quitamos el try/except externo para ver si el error de Rich persiste aquí
    try:
        # Usamos la lógica de Polars que ya manejamos para que el Bronze sea instantáneo
        ingesta_incremental_polars(
            ruta_raw=BASE_PATH_RAW,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha" # O la columna que uses de pivote
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ Capa Bronze no actualizada: {e}[/]")

    # --- ESTRUCTURA DE COLUMNAS ---
    cols_comunes = [
        "N° Abonado", "Estatus", "Saldo", 
        "Responsable", "Suscripción", 
        "Grupo Afinidad", "Ciudad", "Zona", "Barrio", "Dirección"
    ]

    # ---------------------------------------------------------
    # 2. PROCESAMIENTO (SIN EL CONSOLE.STATUS ANIDADO)
    # ---------------------------------------------------------
    dfs_para_anexar = []
    
    # ELIMINAMOS el 'with console.status' de aquí porque archivos_raw/polars ya usó uno
    # y eso es lo que causa el error "Only one live display".
    console.print("[blue]🔍 Escaneando carpetas de Actualización de Datos...[/]")
    
    for carpeta in FOLDERS_ACT_DATOS:
        ruta_completa = os.path.join(BASE_PATH_RAW, carpeta)
        
        # --- ESTRATEGIA A: CALL CENTER y OOCC ---
        if "CALL CENTER" in carpeta or "OOCC" in carpeta:
            cols_source = cols_comunes + ["Tipo Respuesta", "Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Franquicia"]
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                df_temp = df_temp.rename(columns={
                    "Fecha Llamada": "Fecha",
                    "Hora Llamada": "Hora",
                    "Franquicia": "Nombre Franquicia"
                })
                df_temp["Origen"] = df_temp.get("Source.Name", "").astype(str).str.upper()
                dfs_para_anexar.append(df_temp)
                console.print(f"   🔹 {carpeta}: {len(df_temp)} registros.")

        # --- ESTRATEGIA B: OBSERVACIONES ---
        elif "OBSERVACIONES" in carpeta:
            cols_source = cols_comunes + ["Fecha", "Hora", "Observacion", "Asunto", "Franquicia"]
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                if "Detalle Respuesta" not in df_temp.columns:
                    df_temp["Detalle Respuesta"] = np.nan
                
                df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp.get("Observacion"))
                df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp.get("Asunto"))
                
                if "Tipo Respuesta" not in df_temp.columns:
                    df_temp["Tipo Respuesta"] = "GESTION INTERNA / OBS" 

                df_temp = df_temp.rename(columns={"Franquicia": "Nombre Franquicia"})
                df_temp = df_temp.drop(columns=[c for c in ["Observacion", "Asunto"] if c in df_temp.columns])
                
                df_temp["Origen"] = df_temp.get("Source.Name", "").astype(str).str.upper()
                dfs_para_anexar.append(df_temp)
                console.print(f"   🔹 {carpeta}: {len(df_temp)} registros.")

    # ---------------------------------------------------------
    # 3. CONSOLIDACIÓN TOTAL
    # ---------------------------------------------------------
    if not dfs_para_anexar:
        console.print("[bold red]❌ No se encontraron datos en las carpetas raw.[/]")
        return

    df_total = pd.concat(dfs_para_anexar, ignore_index=True)
    df_total["Fecha"] = pd.to_datetime(df_total["Fecha"], dayfirst=True, errors="coerce")

    # ---------------------------------------------------------
    # 4. TRANSFORMACIÓN FINAL (MANTENIENDO TU LÓGICA)
    # ---------------------------------------------------------
    df_total["Detalle Respuesta"] = df_total["Detalle Respuesta"].fillna("").astype(str).str.upper().str.strip()
    df_total["Detalle Respuesta"] = df_total["Detalle Respuesta"].str.split("ANT: ").str[0].str.strip()
    df_total["Tipo Respuesta"] = df_total["Tipo Respuesta"].fillna("SIN CLASIFICAR").astype(str).str.upper()

    reemplazos = {
        "EMAIL": "CORREO ELECTRÓNICO",
        "CÉDULA DE IDENTIDAD": "CEDULA",
        "CELULAR": "TELEFONO",
        "NÚMERO TELEFÓNICO": "TELEFONO"
    }
    for viejo, nuevo in reemplazos.items():
        df_total["Detalle Respuesta"] = df_total["Detalle Respuesta"].str.replace(viejo, nuevo, regex=False)

    cols_finales = [
        "Origen", "N° Abonado", "Estatus", "Fecha", "Hora", 
        "Detalle Respuesta", "Tipo Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad"
    ]
    
    for c in cols_finales:
        if c not in df_total.columns:
            df_total[c] = None

    df_total = df_total.reindex(columns=cols_finales)

    # ---------------------------------------------------------
    # 5. DEDUPLICACIÓN Y GUARDADO
    # ---------------------------------------------------------
    subset_duplicados = ["N° Abonado", "Fecha", "Hora", "Detalle Respuesta", "Responsable"]
    df_final = df_total.drop_duplicates(subset=subset_duplicados, keep='last')
    
    # --- BLINDAJE PARA POWER BI ---
    df_final = standard_hours(df_final, 'Hora')
    df_final = limpiar_nulos_powerbi(df_final)

    guardar_parquet(
        df_final, 
        NOMBRE_GOLD,
        filas_iniciales=len(df_total),
        ruta_destino=PATHS.get("gold", "")
    )
    console.print(f"[bold green]✅ Actualización Datos Gold finalizado. Filas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()